package e2e

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/expfmt"

	"github.com/cortexproject/cortex/pkg/util"
)

var (
	dockerPortPattern = regexp.MustCompile(`^.*:(\d+)$`)
)

type Service struct {
	name         string
	image        string
	networkName  string
	networkPorts []int
	env          map[string]string
	command      *Command
	readiness    *ReadinessProbe

	// Maps container ports to dynamically binded local ports.
	networkPortsContainerToLocal map[int]int

	// Generic retry backoff.
	retryBackoff *util.Backoff
}

func NewService(
	name string,
	image string,
	networkName string,
	networkPorts []int,
	env map[string]string,
	command *Command,
	readiness *ReadinessProbe,
) *Service {
	return &Service{
		name:                         name,
		image:                        image,
		networkName:                  networkName,
		networkPorts:                 networkPorts,
		env:                          env,
		command:                      command,
		networkPortsContainerToLocal: map[int]int{},
		readiness:                    readiness,
		retryBackoff: util.NewBackoff(context.Background(), util.BackoffConfig{
			MinBackoff: 100 * time.Millisecond,
			MaxBackoff: 200 * time.Millisecond,
			MaxRetries: 100, // Sometimes the CI is slow
		}),
	}
}

func (s *Service) Name() string { return s.name }

func (s *Service) SetBackoff(cfg util.BackoffConfig) {
	s.retryBackoff = util.NewBackoff(context.Background(), cfg)
}

func (s *Service) start(sharedDir string) (err error) {
	// In case of any error, if the container was already created, we
	// have to cleanup removing it. We ignore the error of the "docker rm"
	// because we don't know if the container was created or not.
	defer func() {
		if err != nil {
			_, _ = RunCommandAndGetOutput("docker", "rm", "--force", s.name)
		}
	}()

	cmd := exec.Command("docker", s.buildDockerRunArgs(sharedDir)...)
	cmd.Stdout = &LinePrefixWriter{prefix: s.name + ": ", wrapped: os.Stdout}
	cmd.Stderr = &LinePrefixWriter{prefix: s.name + ": ", wrapped: os.Stderr}
	if err = cmd.Start(); err != nil {
		return err
	}

	// Wait until the container has been started
	if err = s.WaitStarted(); err != nil {
		return err
	}

	// Get the dynamic local ports mapped to the container
	for _, containerPort := range s.networkPorts {
		var out []byte
		var localPort int

		out, err = RunCommandAndGetOutput("docker", "port", s.name, strconv.Itoa(containerPort))
		if err != nil {
			err = errors.Wrapf(err, "unable to get mapping for port %d", containerPort)
			return err
		}

		stdout := strings.TrimSpace(string(out))
		matches := dockerPortPattern.FindStringSubmatch(stdout)
		if len(matches) != 2 {
			err = fmt.Errorf("unable to get mapping for port %d (output: %s)", containerPort, stdout)
			return err
		}

		localPort, err = strconv.Atoi(matches[1])
		if err != nil {
			err = errors.Wrapf(err, "unable to get mapping for port %d", containerPort)
			return err
		}

		s.networkPortsContainerToLocal[containerPort] = localPort
	}

	return nil
}

func (s *Service) stop() error {
	fmt.Println("Stopping", s.name)

	if out, err := RunCommandAndGetOutput("docker", "stop", "--time=30", s.name); err != nil {
		fmt.Println(string(out))
		return err
	}

	return nil
}

func (s *Service) kill() error {
	fmt.Println("Killing", s.name)

	if out, err := RunCommandAndGetOutput("docker", "stop", "--time=0", s.name); err != nil {
		fmt.Println(string(out))
		return err
	}

	return nil
}

func (s *Service) Endpoint(port int) string {
	// Map the container port to the local port
	localPort, ok := s.networkPortsContainerToLocal[port]
	if !ok {
		return ""
	}

	// Do not use "localhost" cause it doesn't work with the AWS DynamoDB client.
	return fmt.Sprintf("127.0.0.1:%d", localPort)
}

func (s *Service) Ready() (bool, error) {
	// Ensure the service has a readiness probe configure
	if s.readiness == nil {
		return true, nil
	}

	// Map the container port to the local port
	localPort, ok := s.networkPortsContainerToLocal[s.readiness.port]
	if !ok {
		return false, fmt.Errorf("unknown port %d configured in the readiness probe", s.readiness.port)
	}

	return s.readiness.Ready(localPort), nil
}

func (s *Service) Metrics(port int) (string, error) {
	// Map the container port to the local port
	localPort, ok := s.networkPortsContainerToLocal[port]
	if !ok {
		return "", fmt.Errorf("unknown port %d", port)
	}

	// Fetch metrics
	res, err := GetRequest(fmt.Sprintf("http://localhost:%d/metrics", localPort))
	if err != nil {
		return "", err
	}

	// Check the status code
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return "", fmt.Errorf("unexpected status code %d while fetching metrics", res.StatusCode)
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)

	return string(body), err
}

func (s *Service) WaitStarted() error {
	for s.retryBackoff.Reset(); s.retryBackoff.Ongoing(); {
		if err := exec.Command("docker", "inspect", s.name).Run(); err == nil {
			return nil
		}

		s.retryBackoff.Wait()
	}

	return fmt.Errorf("docker container %s failed to start", s.name)
}

func (s *Service) WaitReady() error {
	for s.retryBackoff.Reset(); s.retryBackoff.Ongoing(); {
		ready, err := s.Ready()
		if ready {
			return nil
		}
		if err != nil {
			return err
		}

		s.retryBackoff.Wait()
	}

	return fmt.Errorf("the service %s is not ready", s.name)
}

func (s *Service) WaitMetric(metric string, value float64) error {
	lastFoundValue := 0.0

	for s.retryBackoff.Reset(); s.retryBackoff.Ongoing(); {
		metrics, err := s.Metrics(80)
		if err != nil {
			return err
		}

		var tp expfmt.TextParser
		families, err := tp.TextToMetricFamilies(strings.NewReader(metrics))
		if err != nil {
			return err
		}

		// Check if the metric is exported
		mf, ok := families[metric]
		if ok {
			for _, m := range mf.Metric {
				if m.GetGauge() != nil {
					lastFoundValue = m.GetGauge().GetValue()
					if lastFoundValue == value {
						return nil
					}
				} else if m.GetCounter() != nil {
					lastFoundValue = m.GetCounter().GetValue()
					if lastFoundValue == value {
						return nil
					}
				} else if m.GetHistogram() != nil {
					lastFoundValue = float64(m.GetHistogram().GetSampleCount())
					if lastFoundValue == value {
						return nil
					}
				}
			}
		}

		s.retryBackoff.Wait()
	}

	return fmt.Errorf("unable to find a metric %s with value %v. Last value found: %v", metric, value, lastFoundValue)
}

func (s *Service) buildDockerRunArgs(sharedDir string) []string {
	args := []string{"run", "--rm", "--net=" + s.networkName, "--name=" + s.name, "--hostname=" + s.name}

	// Mount the shared/ directory into the container
	args = append(args, "-v", fmt.Sprintf("%s:%s", sharedDir, ContainerSharedDir))

	// Environment variables
	for name, value := range s.env {
		args = append(args, "-e", name+"="+value)
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

func (p *ReadinessProbe) Ready(localPort int) bool {
	res, err := GetRequest(fmt.Sprintf("http://localhost:%d%s", localPort, p.path))
	if err != nil {
		return false
	}

	res.Body.Close()

	return res.StatusCode == p.expectedStatus
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
