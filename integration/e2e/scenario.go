package e2e

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

const (
	NetworkName        = "e2e-test"
	ContainerSharedDir = "/shared"
)

type Scenario struct {
	services []*Service

	sharedDir string
}

func NewScenario() (*Scenario, error) {
	s := &Scenario{
		services: []*Service{},
	}

	tmpDir, err := ioutil.TempDir("", "e2e_integration_test")
	if err != nil {
		return nil, err
	}
	s.sharedDir, err = filepath.Abs(tmpDir)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return nil, err
	}

	// Force a shutdown in order to cleanup from a spurious situation in case
	// the previous tests run didn't cleanup correctly
	s.shutdown()

	// Setup the docker network
	if out, err := RunCommandAndGetOutput("docker", "network", "create", NetworkName); err != nil {
		fmt.Println(string(out))
		s.clean()
		return nil, errors.Wrapf(err, "create docker network '%s'", NetworkName)
	}

	return s, nil
}

// SharedDir returns the absolute path of the directory on the host that is shared with all services in docker.
func (s *Scenario) SharedDir() string {
	return s.sharedDir
}

func (s *Scenario) Service(name string) *Service {
	for _, service := range s.services {
		if service.name == name {
			return service
		}
	}

	return nil
}

func (s *Scenario) Endpoint(name string, port int) string {
	service := s.Service(name)
	if service == nil {
		return ""
	}

	return service.Endpoint(port)
}

func (s *Scenario) StartService(service *Service) error {
	// TODO(bwplotka): Some basic logger would be nice.
	fmt.Println("Starting", service.name)

	// Ensure another service with the same name doesn't exist
	if s.Service(service.name) != nil {
		return fmt.Errorf("another service with the same name '%s' has already been started", service.name)
	}

	// Start the service
	if err := service.start(s.SharedDir()); err != nil {
		return err
	}

	// Add to the list of services
	s.services = append(s.services, service)
	return nil
}

func (s *Scenario) StopService(name string) error {
	service := s.Service(name)
	if service == nil {
		return fmt.Errorf("unable to stop service %s because does not exist", name)
	}

	if err := service.stop(); err != nil {
		return err
	}

	// Remove the service from the list of services
	for i, entry := range s.services {
		if entry.name == name {
			s.services = append(s.services[:i], s.services[i+1:]...)
			break
		}
	}

	return nil
}

// WaitReady waits until one or more services are ready. A service
// is ready when its readiness probe succeed. If a service has no
// readiness probe, it's considered ready without doing any check.
func (s *Scenario) WaitReady(services ...string) error {
	for _, name := range services {
		service := s.Service(name)
		if service == nil {
			return fmt.Errorf("unable to wait for service '%s' ready because the service does not exist", name)
		}

		if err := service.WaitReady(); err != nil {
			return err
		}
	}

	return nil
}

func (s *Scenario) Close() {
	if s == nil {
		return
	}

	s.shutdown()
	s.clean()
}

// TODO(bwplotka): Add comments.
func (s *Scenario) clean() {
	if err := os.RemoveAll(s.sharedDir); err != nil {
		fmt.Println("error while removing sharedDir", s.sharedDir, "err:", err)
	}
}

func (s *Scenario) shutdown() {
	// Kill the services in the opposite order
	for i := len(s.services) - 1; i >= 0; i-- {
		if err := s.services[i].kill(); err != nil {
			fmt.Println("Unable to kill service", s.services[i].name, ":", err.Error())
		}
	}

	// Ensure there are no leftover containers
	if out, err := RunCommandAndGetOutput(
		"docker",
		"ps",
		"-a",
		"--quiet",
		"--filter",
		fmt.Sprintf("network=%s", NetworkName),
	); err == nil {
		for _, containerID := range strings.Split(string(out), "\n") {
			containerID = strings.TrimSpace(containerID)
			if containerID == "" {
				continue
			}

			if out, err = RunCommandAndGetOutput("docker", "rm", "--force", containerID); err != nil {
				fmt.Println(string(out))
				fmt.Println("Unable to cleanup leftover container", containerID, ":", err.Error())
			}
		}
	} else {
		fmt.Println(string(out))
		fmt.Println("Unable to cleanup leftover containers:", err.Error())
	}

	// Teardown the docker network. In case the network does not exists (ie. this function
	// is called during the setup of the scenario) we skip the removal in order to not log
	// an error which may be misleading.
	if ok, err := existDockerNetwork(); ok || err != nil {
		if out, err := RunCommandAndGetOutput("docker", "network", "rm", NetworkName); err != nil {
			fmt.Println(string(out))
			fmt.Println("Unable to remove docker network", NetworkName, ":", err.Error())
		}
	}
}

func existDockerNetwork() (bool, error) {
	out, err := RunCommandAndGetOutput("docker", "network", "ls", "--quiet", "--filter", fmt.Sprintf("name=%s", NetworkName))
	if err != nil {
		fmt.Println(string(out))
		fmt.Println("Unable to check if docker network", NetworkName, "exists:", err.Error())
	}

	return strings.TrimSpace(string(out)) != "", nil
}
