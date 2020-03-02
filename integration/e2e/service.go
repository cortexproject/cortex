package e2e

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

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
	readiness    *ReadinessProbe

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
	readiness *ReadinessProbe,
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
	cmd.Stdout = &LinePrefixWriter{prefix: s.name + ": ", wrapped: os.Stdout}
	cmd.Stderr = &LinePrefixWriter{prefix: s.name + ": ", wrapped: os.Stderr}
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
	fmt.Println("Ports for container:", s.containerName(), "Mapping:", s.networkPortsContainerToLocal)
	return nil
}

func (s *ConcreteService) Stop() error {
	if !s.isExpectedRunning() {
		return nil
	}

	fmt.Println("Stopping", s.name)

	if out, err := RunCommandAndGetOutput("docker", "stop", "--time=30", s.containerName()); err != nil {
		fmt.Println(string(out))
		return err
	}
	s.usedNetworkName = ""

	return nil
}

func (s *ConcreteService) Kill() error {
	if !s.isExpectedRunning() {
		return nil
	}

	fmt.Println("Killing", s.name)

	if out, err := RunCommandAndGetOutput("docker", "stop", "--time=0", s.containerName()); err != nil {
		fmt.Println(string(out))
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

func (s *ConcreteService) SetReadinessProbe(probe *ReadinessProbe) {
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

	// Map the container port to the local port
	localPort, ok := s.networkPortsContainerToLocal[s.readiness.port]
	if !ok {
		return fmt.Errorf("unknown port %d configured in the readiness probe", s.readiness.port)
	}

	return s.readiness.Ready(localPort)
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
		err = exec.Command("docker", "inspect", s.containerName()).Run()
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
	if s.command.entrypointDisabled {
		args = append(args, "--entrypoint", "")
	}

	args = append(args, s.image)
	args = append(args, s.command.cmd)
	args = append(args, s.command.args...)
	return args
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

type ReadinessProbe struct {
	port           int
	path           string
	expectedStatus int
}

func NewReadinessProbe(port int, path string, expectedStatus int) *ReadinessProbe {
	return &ReadinessProbe{
		port:           port,
		path:           path,
		expectedStatus: expectedStatus,
	}
}

func (p *ReadinessProbe) Ready(localPort int) (err error) {
	res, err := GetRequest(fmt.Sprintf("http://localhost:%d%s", localPort, p.path))
	if err != nil {
		return err
	}

	defer runutil.ExhaustCloseWithErrCapture(&err, res.Body, "response readiness")

	if res.StatusCode == p.expectedStatus {
		return nil
	}

	return fmt.Errorf("got no expected status code: %v, expected: %v", res.StatusCode, p.expectedStatus)
}

type LinePrefixWriter struct {
	prefix  string
	wrapped io.Writer
}

func (w *LinePrefixWriter) Write(p []byte) (n int, err error) {
	for _, line := range strings.Split(string(p), "\n") {
		// Skip empty lines
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Write the prefix + line to the wrapped writer
		_, err := w.wrapped.Write([]byte(w.prefix + line + "\n"))
		if err != nil {
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
	readiness *ReadinessProbe,
	httpPort int,
	otherPorts ...int,
) *HTTPService {
	return &HTTPService{
		ConcreteService: NewConcreteService(name, image, command, readiness, append(otherPorts, httpPort)...),
		httpPort:        httpPort,
	}
}

func (s *HTTPService) metrics() (_ string, err error) {
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

func sumValues(family *io_prometheus_client.MetricFamily) float64 {
	sum := 0.0
	for _, m := range family.Metric {
		if m.GetGauge() != nil {
			sum += m.GetGauge().GetValue()
		} else if m.GetCounter() != nil {
			sum += m.GetCounter().GetValue()
		} else if m.GetHistogram() != nil {
			sum += m.GetHistogram().GetSampleSum()
		} else if m.GetSummary() != nil {
			sum += m.GetSummary().GetSampleSum()
		}
	}
	return sum
}

func Equals(value float64) func(sums ...float64) bool {
	return func(sums ...float64) bool {
		if len(sums) != 1 {
			panic("equals: expected one value")
		}
		return sums[0] == value || math.IsNaN(sums[0]) && math.IsNaN(value)
	}
}

func Greater(value float64) func(sums ...float64) bool {
	return func(sums ...float64) bool {
		if len(sums) != 1 {
			panic("greater: expected one value")
		}
		return sums[0] > value
	}
}

func Less(value float64) func(sums ...float64) bool {
	return func(sums ...float64) bool {
		if len(sums) != 1 {
			panic("less: expected one value")
		}
		return sums[0] < value
	}
}

// EqualsAmongTwo returns true if first sum is equal to the second.
// NOTE: Be careful on scrapes in between of process that changes two metrics. Those are
// usually not atomic.
func EqualsAmongTwo(sums ...float64) bool {
	if len(sums) != 2 {
		panic("equalsAmongTwo: expected two values")
	}
	return sums[0] == sums[1]
}

// GreaterAmongTwo returns true if first sum is greater than second.
// NOTE: Be careful on scrapes in between of process that changes two metrics. Those are
// usually not atomic.
func GreaterAmongTwo(sums ...float64) bool {
	if len(sums) != 2 {
		panic("greaterAmongTwo: expected two values")
	}
	return sums[0] > sums[1]
}

// LessAmongTwo returns true if first sum is smaller than second.
// NOTE: Be careful on scrapes in between of process that changes two metrics. Those are
// usually not atomic.
func LessAmongTwo(sums ...float64) bool {
	if len(sums) != 2 {
		panic("lessAmongTwo: expected two values")
	}
	return sums[0] < sums[1]
}

func (s *HTTPService) WaitSumMetrics(isExpected func(sums ...float64) bool, metricNames ...string) error {
	sums := make([]float64, len(metricNames))

	for s.retryBackoff.Reset(); s.retryBackoff.Ongoing(); {
		metrics, err := s.metrics()
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
			mf, ok := families[m]
			if ok {
				sums[i] = sumValues(mf)
			}
		}

		if isExpected(sums...) {
			return nil
		}

		s.retryBackoff.Wait()
	}

	return fmt.Errorf("unable to find metrics %s with expected values. LastValues: %v", metricNames, sums)
}
