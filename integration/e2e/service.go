package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/thanos-io/thanos/pkg/runutil"

	"github.com/cortexproject/cortex/pkg/util"
)

var (
	dockerPortPattern = regexp.MustCompile(`^.*:(\d+)$`)
)

// ConcreteService represents microservice with optional ports which will be discoverable from docker
// with <name>:<port>. For connecting from test, use `Endpoint` method.
//
// ConcreteService can be reused (started and stopped many time), but it can represent only one running container
// at the time.
type ConcreteService struct {
	name         string
	image        string
	networkPorts []int
	env          map[string]string
	user         string
	command      *Command
	readiness    ReadinessProbe

	// Maps container ports to dynamically binded local ports.
	networkPortsContainerToLocal map[int]int

	// Generic retry backoff.
	retryBackoff *util.Backoff

	// docker NetworkName used to start this container.
	// If empty it means service is stopped.
	usedNetworkName string
}

func NewConcreteService(
	name string,
	image string,
	command *Command,
	readiness ReadinessProbe,
	networkPorts ...int,
) *ConcreteService {
	return &ConcreteService{
		name:                         name,
		image:                        image,
		networkPorts:                 networkPorts,
		command:                      command,
		networkPortsContainerToLocal: map[int]int{},
		readiness:                    readiness,
		retryBackoff: util.NewBackoff(context.Background(), util.BackoffConfig{
			MinBackoff: 300 * time.Millisecond,
			MaxBackoff: 600 * time.Millisecond,
			MaxRetries: 50, // Sometimes the CI is slow ¯\_(ツ)_/¯
		}),
	}
}

func (s *ConcreteService) isExpectedRunning() bool {
	return s.usedNetworkName != ""
}

func (s *ConcreteService) Name() string { return s.name }

// Less often used options.

func (s *ConcreteService) SetBackoff(cfg util.BackoffConfig) {
	s.retryBackoff = util.NewBackoff(context.Background(), cfg)
}

func (s *ConcreteService) SetEnvVars(env map[string]string) {
	s.env = env
}

func (s *ConcreteService) SetUser(user string) {
	s.user = user
}

func (s *ConcreteService) Start(networkName, sharedDir string) (err error) {
	// In case of any error, if the container was already created, we
	// have to cleanup removing it. We ignore the error of the "docker rm"
	// because we don't know if the container was created or not.
	defer func() {
		if err != nil {
			_, _ = RunCommandAndGetOutput("docker", "rm", "--force", s.name)
		}
	}()

	cmd := exec.Command("docker", s.buildDockerRunArgs(networkName, sharedDir)...)
	cmd.Stdout = &LinePrefixLogger{prefix: s.name + ": ", logger: logger}
	cmd.Stderr = &LinePrefixLogger{prefix: s.name + ": ", logger: logger}
	if err = cmd.Start(); err != nil {
		return err
	}
	s.usedNetworkName = networkName

	// Wait until the container has been started.
	if err = s.WaitStarted(); err != nil {
		return err
	}

	// Get the dynamic local ports mapped to the container.
	for _, containerPort := range s.networkPorts {
		var out []byte
		var localPort int

		out, err = RunCommandAndGetOutput("docker", "port", s.containerName(), strconv.Itoa(containerPort))
		if err != nil {
			// Catch init errors.
			if werr := s.WaitStarted(); werr != nil {
				return errors.Wrapf(werr, "failed to get mapping for port as container %s exited: %v", s.containerName(), err)
			}
			return errors.Wrapf(err, "unable to get mapping for port %d; service: %s", containerPort, s.name)
		}

		stdout := strings.TrimSpace(string(out))
		matches := dockerPortPattern.FindStringSubmatch(stdout)
		if len(matches) != 2 {
			return fmt.Errorf("unable to get mapping for port %d (output: %s); service: %s", containerPort, stdout, s.name)
		}

		localPort, err = strconv.Atoi(matches[1])
		if err != nil {
			return errors.Wrapf(err, "unable to get mapping for port %d; service: %s", containerPort, s.name)
		}
		s.networkPortsContainerToLocal[containerPort] = localPort
	}
	logger.Log("Ports for container:", s.containerName(), "Mapping:", s.networkPortsContainerToLocal)
	return nil
}

func (s *ConcreteService) Stop() error {
	if !s.isExpectedRunning() {
		return nil
	}

	logger.Log("Stopping", s.name)

	if out, err := RunCommandAndGetOutput("docker", "stop", "--time=30", s.containerName()); err != nil {
		logger.Log(string(out))
		return err
	}
	s.usedNetworkName = ""

	return nil
}

func (s *ConcreteService) Kill() error {
	if !s.isExpectedRunning() {
		return nil
	}

	logger.Log("Killing", s.name)

	if out, err := RunCommandAndGetOutput("docker", "stop", "--time=0", s.containerName()); err != nil {
		logger.Log(string(out))
		return err
	}
	s.usedNetworkName = ""

	return nil
}

// Endpoint returns external (from host perspective) service endpoint (host:port) for given internal port.
// External means that it will be accessible only from host, but not from docker containers.
//
// If your service is not running, this method returns incorrect `stopped` endpoint.
func (s *ConcreteService) Endpoint(port int) string {
	if !s.isExpectedRunning() {
		return "stopped"
	}

	// Map the container port to the local port.
	localPort, ok := s.networkPortsContainerToLocal[port]
	if !ok {
		return ""
	}

	// Do not use "localhost" cause it doesn't work with the AWS DynamoDB client.
	return fmt.Sprintf("127.0.0.1:%d", localPort)
}

// NetworkEndpoint returns internal service endpoint (host:port) for given internal port.
// Internal means that it will be accessible only from docker containers within the network that this
// service is running in. If you configure your local resolver with docker DNS namespace you can access it from host
// as well. Use `Endpoint` for host access.
//
// If your service is not running, use `NetworkEndpointFor` instead.
func (s *ConcreteService) NetworkEndpoint(port int) string {
	if s.usedNetworkName == "" {
		return "stopped"
	}
	return s.NetworkEndpointFor(s.usedNetworkName, port)
}

// NetworkEndpointFor returns internal service endpoint (host:port) for given internal port and network.
// Internal means that it will be accessible only from docker containers within the given network. If you configure
// your local resolver with docker DNS namespace you can access it from host as well.
//
// This method return correct endpoint for the service in any state.
func (s *ConcreteService) NetworkEndpointFor(networkName string, port int) string {
	return fmt.Sprintf("%s:%d", containerName(networkName, s.name), port)
}

func (s *ConcreteService) SetReadinessProbe(probe ReadinessProbe) {
	s.readiness = probe
}

func (s *ConcreteService) Ready() error {
	if !s.isExpectedRunning() {
		return fmt.Errorf("service %s is stopped", s.Name())
	}

	// Ensure the service has a readiness probe configure.
	if s.readiness == nil {
		return nil
	}

	return s.readiness.Ready(s)
}

func containerName(netName string, name string) string {
	return fmt.Sprintf("%s-%s", netName, name)
}

func (s *ConcreteService) containerName() string {
	return containerName(s.usedNetworkName, s.name)
}

func (s *ConcreteService) WaitStarted() (err error) {
	if !s.isExpectedRunning() {
		return fmt.Errorf("service %s is stopped", s.Name())
	}

	for s.retryBackoff.Reset(); s.retryBackoff.Ongoing(); {
		// Enforce a timeout on the command execution because we've seen some flaky tests
		// stuck here.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = exec.CommandContext(ctx, "docker", "inspect", s.containerName()).Run()
		if err == nil {
			return nil
		}
		s.retryBackoff.Wait()
	}

	return fmt.Errorf("docker container %s failed to start: %v", s.name, err)
}

func (s *ConcreteService) WaitReady() (err error) {
	if !s.isExpectedRunning() {
		return fmt.Errorf("service %s is stopped", s.Name())
	}

	for s.retryBackoff.Reset(); s.retryBackoff.Ongoing(); {
		err = s.Ready()
		if err == nil {
			return nil
		}

		s.retryBackoff.Wait()
	}

	return fmt.Errorf("the service %s is not ready; err: %v", s.name, err)
}

func (s *ConcreteService) buildDockerRunArgs(networkName, sharedDir string) []string {
	args := []string{"run", "--rm", "--net=" + networkName, "--name=" + networkName + "-" + s.name, "--hostname=" + s.name}

	// Mount the shared/ directory into the container
	args = append(args, "-v", fmt.Sprintf("%s:%s:Z", sharedDir, ContainerSharedDir))

	// Environment variables
	for name, value := range s.env {
		args = append(args, "-e", name+"="+value)
	}

	if s.user != "" {
		args = append(args, "--user", s.user)
	}

	// Published ports
	for _, port := range s.networkPorts {
		args = append(args, "-p", strconv.Itoa(port))
	}

	// Disable entrypoint if required
	if s.command != nil && s.command.entrypointDisabled {
		args = append(args, "--entrypoint", "")
	}

	args = append(args, s.image)

	if s.command != nil {
		args = append(args, s.command.cmd)
		args = append(args, s.command.args...)
	}

	return args
}

func (s *ConcreteService) Exec(command *Command) (string, error) {
	args := []string{"exec", s.containerName()}
	args = append(args, command.cmd)
	args = append(args, command.args...)

	cmd := exec.Command("docker", args...)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout

	err := cmd.Run()
	if err != nil {
		return "", err
	}

	return stdout.String(), nil
}

type Command struct {
	cmd                string
	args               []string
	entrypointDisabled bool
}

func NewCommand(cmd string, args ...string) *Command {
	return &Command{
		cmd:  cmd,
		args: args,
	}
}

func NewCommandWithoutEntrypoint(cmd string, args ...string) *Command {
	return &Command{
		cmd:                cmd,
		args:               args,
		entrypointDisabled: true,
	}
}

type ReadinessProbe interface {
	Ready(service *ConcreteService) (err error)
}

// HTTPReadinessProbe checks readiness by making HTTP call and checking for expected HTTP status code
type HTTPReadinessProbe struct {
	port                     int
	path                     string
	expectedStatusRangeStart int
	expectedStatusRangeEnd   int
}

func NewHTTPReadinessProbe(port int, path string, expectedStatusRangeStart, expectedStatusRangeEnd int) *HTTPReadinessProbe {
	return &HTTPReadinessProbe{
		port:                     port,
		path:                     path,
		expectedStatusRangeStart: expectedStatusRangeStart,
		expectedStatusRangeEnd:   expectedStatusRangeEnd,
	}
}

func (p *HTTPReadinessProbe) Ready(service *ConcreteService) (err error) {
	endpoint := service.Endpoint(p.port)
	if endpoint == "" {
		return fmt.Errorf("cannot get service endpoint for port %d", p.port)
	} else if endpoint == "stopped" {
		return errors.New("service has stopped")
	}

	res, err := GetRequest("http://" + endpoint + p.path)
	if err != nil {
		return err
	}

	defer runutil.ExhaustCloseWithErrCapture(&err, res.Body, "response readiness")

	if p.expectedStatusRangeStart <= res.StatusCode && res.StatusCode <= p.expectedStatusRangeEnd {
		return nil
	}

	return fmt.Errorf("got status code: %v, expected code in range: [%v, %v]", res.StatusCode, p.expectedStatusRangeStart, p.expectedStatusRangeEnd)
}

// TCPReadinessProbe checks readiness by ensure a TCP connection can be established.
type TCPReadinessProbe struct {
	port int
}

func NewTCPReadinessProbe(port int) *TCPReadinessProbe {
	return &TCPReadinessProbe{
		port: port,
	}
}

func (p *TCPReadinessProbe) Ready(service *ConcreteService) (err error) {
	endpoint := service.Endpoint(p.port)
	if endpoint == "" {
		return fmt.Errorf("cannot get service endpoint for port %d", p.port)
	} else if endpoint == "stopped" {
		return errors.New("service has stopped")
	}

	conn, err := net.DialTimeout("tcp", endpoint, time.Second)
	if err != nil {
		return err
	}

	return conn.Close()
}

// CmdReadinessProbe checks readiness by `Exec`ing a command (within container) which returns 0 to consider status being ready
type CmdReadinessProbe struct {
	cmd *Command
}

func NewCmdReadinessProbe(cmd *Command) *CmdReadinessProbe {
	return &CmdReadinessProbe{cmd: cmd}
}

func (p *CmdReadinessProbe) Ready(service *ConcreteService) error {
	_, err := service.Exec(p.cmd)
	return err
}

type LinePrefixLogger struct {
	prefix string
	logger log.Logger
}

func (w *LinePrefixLogger) Write(p []byte) (n int, err error) {
	for _, line := range strings.Split(string(p), "\n") {
		// Skip empty lines
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Write the prefix + line to the wrapped writer
		if err := w.logger.Log(w.prefix + line); err != nil {
			return 0, err
		}
	}

	return len(p), nil
}

// HTTPService represents opinionated microservice with at least HTTP port that as mandatory requirement,
// serves metrics.
type HTTPService struct {
	*ConcreteService

	httpPort int
}

func NewHTTPService(
	name string,
	image string,
	command *Command,
	readiness ReadinessProbe,
	httpPort int,
	otherPorts ...int,
) *HTTPService {
	return &HTTPService{
		ConcreteService: NewConcreteService(name, image, command, readiness, append(otherPorts, httpPort)...),
		httpPort:        httpPort,
	}
}

func (s *HTTPService) Metrics() (_ string, err error) {
	// Map the container port to the local port
	localPort := s.networkPortsContainerToLocal[s.httpPort]

	// Fetch metrics.
	res, err := GetRequest(fmt.Sprintf("http://localhost:%d/metrics", localPort))
	if err != nil {
		return "", err
	}

	// Check the status code.
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return "", fmt.Errorf("unexpected status code %d while fetching metrics", res.StatusCode)
	}

	defer runutil.ExhaustCloseWithErrCapture(&err, res.Body, "metrics response")
	body, err := ioutil.ReadAll(res.Body)

	return string(body), err
}

func (s *HTTPService) HTTPPort() int {
	return s.httpPort
}

func (s *HTTPService) HTTPEndpoint() string {
	return s.Endpoint(s.httpPort)
}

func (s *HTTPService) NetworkHTTPEndpoint() string {
	return s.NetworkEndpoint(s.httpPort)
}

func (s *HTTPService) NetworkHTTPEndpointFor(networkName string) string {
	return s.NetworkEndpointFor(networkName, s.httpPort)
}

func getValue(m *io_prometheus_client.Metric) float64 {
	if m.GetGauge() != nil {
		return m.GetGauge().GetValue()
	} else if m.GetCounter() != nil {
		return m.GetCounter().GetValue()
	} else if m.GetHistogram() != nil {
		return m.GetHistogram().GetSampleSum()
	} else if m.GetSummary() != nil {
		return m.GetSummary().GetSampleSum()
	} else {
		return 0
	}
}

func sumValues(family *io_prometheus_client.MetricFamily) float64 {
	sum := 0.0
	for _, m := range family.Metric {
		sum += getValue(m)
	}
	return sum
}

func EqualsSingle(expected float64) func(float64) bool {
	return func(v float64) bool {
		return v == expected || (math.IsNaN(v) && math.IsNaN(expected))
	}
}

// Equals is an isExpected function for WaitSumMetrics that returns true if given single sum is equals to given value.
func Equals(value float64) func(sums ...float64) bool {
	return func(sums ...float64) bool {
		if len(sums) != 1 {
			panic("equals: expected one value")
		}
		return sums[0] == value || math.IsNaN(sums[0]) && math.IsNaN(value)
	}
}

// Greater is an isExpected function for WaitSumMetrics that returns true if given single sum is greater than given value.
func Greater(value float64) func(sums ...float64) bool {
	return func(sums ...float64) bool {
		if len(sums) != 1 {
			panic("greater: expected one value")
		}
		return sums[0] > value
	}
}

// Less is an isExpected function for WaitSumMetrics that returns true if given single sum is less than given value.
func Less(value float64) func(sums ...float64) bool {
	return func(sums ...float64) bool {
		if len(sums) != 1 {
			panic("less: expected one value")
		}
		return sums[0] < value
	}
}

// EqualsAmongTwo is an isExpected function for WaitSumMetrics that returns true if first sum is equal to the second.
// NOTE: Be careful on scrapes in between of process that changes two metrics. Those are
// usually not atomic.
func EqualsAmongTwo(sums ...float64) bool {
	if len(sums) != 2 {
		panic("equalsAmongTwo: expected two values")
	}
	return sums[0] == sums[1]
}

// GreaterAmongTwo is an isExpected function for WaitSumMetrics that returns true if first sum is greater than second.
// NOTE: Be careful on scrapes in between of process that changes two metrics. Those are
// usually not atomic.
func GreaterAmongTwo(sums ...float64) bool {
	if len(sums) != 2 {
		panic("greaterAmongTwo: expected two values")
	}
	return sums[0] > sums[1]
}

// LessAmongTwo is an isExpected function for WaitSumMetrics that returns true if first sum is smaller than second.
// NOTE: Be careful on scrapes in between of process that changes two metrics. Those are
// usually not atomic.
func LessAmongTwo(sums ...float64) bool {
	if len(sums) != 2 {
		panic("lessAmongTwo: expected two values")
	}
	return sums[0] < sums[1]
}

// WaitSumMetrics waits for at least one instance of each given metric names to be present and their sums, returning true
// when passed to given isExpected(...).
func (s *HTTPService) WaitSumMetrics(isExpected func(sums ...float64) bool, metricNames ...string) error {
	sums := make([]float64, len(metricNames))

	for s.retryBackoff.Reset(); s.retryBackoff.Ongoing(); {
		metrics, err := s.Metrics()
		if err != nil {
			return err
		}

		var tp expfmt.TextParser
		families, err := tp.TextToMetricFamilies(strings.NewReader(metrics))
		if err != nil {
			return err
		}

		for i, m := range metricNames {
			sums[i] = 0.0

			// Check if the metric is exported.
			if mf, ok := families[m]; ok {
				sums[i] = sumValues(mf)
				continue
			}
			return errors.Errorf("metric %s not found in %s metric page", m, s.name)
		}

		if isExpected(sums...) {
			return nil
		}

		s.retryBackoff.Wait()
	}

	return fmt.Errorf("unable to find metrics %s with expected values. LastValues: %v", metricNames, sums)
}

// WaitForMetricWithLabels waits until given metric with matching labels passes `okFn`. If function returns false,
// wait continues. If no such matching metric can be found or wait times out, function returns error.
func (s *HTTPService) WaitForMetricWithLabels(okFn func(v float64) bool, metricName string, expectedLabels map[string]string) error {
	for s.retryBackoff.Reset(); s.retryBackoff.Ongoing(); {
		metrics, err := s.Metrics()
		if err != nil {
			return err
		}

		var tp expfmt.TextParser
		families, err := tp.TextToMetricFamilies(strings.NewReader(metrics))
		if err != nil {
			return err
		}

		mf, ok := families[metricName]
		if !ok {
			return errors.Errorf("metric %s not found in %s metric page", metricName, s.name)
		}

		for _, m := range mf.GetMetric() {
			// check if some metric has all required labels
			metricLabels := map[string]string{}
			for _, lp := range m.GetLabel() {
				metricLabels[lp.GetName()] = lp.GetValue()
			}

			matches := true
			for k, v := range expectedLabels {
				if mv, ok := metricLabels[k]; !ok || mv != v {
					matches = false
					break
				}
			}

			if matches && okFn(getValue(m)) {
				return nil
			}
		}

		s.retryBackoff.Wait()
	}

	return fmt.Errorf("unable to find metric %s with labels %v with expected value", metricName, expectedLabels)
}
