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
	"github.com/prometheus/common/expfmt"
	"github.com/thanos-io/thanos/pkg/runutil"

	"github.com/cortexproject/cortex/pkg/util"
)

var (
	dockerPortPattern = regexp.MustCompile(`^.*:(\d+)$`)
)

// ConcreteService represents microservice with optional ports which will be discoverable from docker
// with <name>:<port>. For connecting from test, use `Endpoint` method.
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

	stopped bool
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
		stopped: true,
	}
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
	s.stopped = false

	// Wait until the container has been started.
	if err = s.WaitStarted(networkName); err != nil {
		return err
	}

	// Get the dynamic local ports mapped to the container.
	for _, containerPort := range s.networkPorts {
		var out []byte
		var localPort int

		out, err = RunCommandAndGetOutput("docker", "port", s.containerName(networkName), strconv.Itoa(containerPort))
		if err != nil {
			// Catch init errors.
			if werr := s.WaitStarted(networkName); werr != nil {
				return errors.Wrapf(werr, "failed to get mapping for port as container %s exited: %v", s.containerName(networkName), err)
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
	fmt.Println("Ports for container:", s.containerName(networkName), "Mapping:", s.networkPortsContainerToLocal)
	return nil
}

func (s *ConcreteService) Stop(networkName string) error {
	if s.stopped {
		return nil
	}

	fmt.Println("Stopping", s.name)

	if out, err := RunCommandAndGetOutput("docker", "stop", "--time=30", s.containerName(networkName)); err != nil {
		fmt.Println(string(out))
		return err
	}
	s.stopped = true

	return nil
}

func (s *ConcreteService) Kill(networkName string) error {
	if s.stopped {
		return nil
	}

	fmt.Println("Killing", s.name)

	if out, err := RunCommandAndGetOutput("docker", "stop", "--time=0", s.containerName(networkName)); err != nil {
		fmt.Println(string(out))
		return err
	}
	s.stopped = true

	return nil
}

func (s *ConcreteService) Endpoint(port int) string {
	if s.stopped {
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

func (s *ConcreteService) NetworkEndpoint(networkName string, port int) string {
	return fmt.Sprintf("%s:%d", s.containerName(networkName), port)
}

func (s *ConcreteService) Ready() error {
	if s.stopped {
		return fmt.Errorf("service %s stopped", s.Name())
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

func (s *ConcreteService) containerName(networkName string) string {
	return fmt.Sprintf("%s-%s", networkName, s.name)
}

func (s *ConcreteService) WaitStarted(networkName string) (err error) {
	if s.stopped {
		return fmt.Errorf("service %s stopped", s.Name())
	}

	for s.retryBackoff.Reset(); s.retryBackoff.Ongoing(); {
		err = exec.Command("docker", "inspect", s.containerName(networkName)).Run()
		if err == nil {
			return nil
		}
		s.retryBackoff.Wait()
	}

	return fmt.Errorf("docker container %s failed to start: %w", s.name, err)
}

func (s *ConcreteService) WaitReady() (err error) {
	if s.stopped {
		return fmt.Errorf("service %s stopped", s.Name())
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

func (s *HTTPService) HTTPEndpoint() string {
	return s.Endpoint(s.httpPort)
}

func (s *HTTPService) NetworkHTTPEndpoint(networkName string) string {
	return s.NetworkEndpoint(networkName, s.httpPort)
}

func (s *HTTPService) WaitSumMetric(metric string, value float64) error {
	lastValue := 0.0

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

		sum := 0.0
		// Check if the metric is exported.
		mf, ok := families[metric]
		if ok {
			for _, m := range mf.Metric {
				if m.GetGauge() != nil {
					sum += m.GetGauge().GetValue()
				} else if m.GetCounter() != nil {
					sum += m.GetCounter().GetValue()
				} else if m.GetHistogram() != nil {
					sum += float64(m.GetHistogram().GetSampleCount())
				}
			}
		}

		if sum == value || math.IsNaN(sum) && math.IsNaN(value) {
			return nil
		}
		lastValue = sum

		s.retryBackoff.Wait()
	}

	return fmt.Errorf("unable to find a metric %s with value %v. LastValue: %v", metric, value, lastValue)
}
