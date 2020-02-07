package framework

import (
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
)

const (
	MinioAccessKey = "cortex"
	MinioSecretKey = "supersecret"
	NetworkName    = "cortex-integration-test"
)

type Scenario struct {
	services []*Service
}

func NewScenario() (*Scenario, error) {
	s := &Scenario{
		services: []*Service{},
	}

	// Force a shutdown in order to cleanup from a spurious situation in case
	// the previous tests run didn't cleanup correctly
	s.Shutdown()

	// Setup the docker network
	if out, err := RunCommandAndGetOutput("docker", "network", "create", NetworkName); err != nil {
		fmt.Println(string(out))
		return nil, errors.Wrapf(err, "create docker network '%s'", NetworkName)
	}

	return s, nil
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
	fmt.Println("Starting", service.name)

	// Ensure another service with the same name doesn't exist
	if s.Service(service.name) != nil {
		return fmt.Errorf("Another service with the same name '%s' has already been started", service.name)
	}

	// Start the service
	if err := service.Start(); err != nil {
		return err
	}

	// Add to the list of services
	s.services = append(s.services, service)
	return nil
}

func (s *Scenario) StartConsul() error {
	return s.StartService(NewService(
		"consul",
		// If you change the image tag, remember to update it in the preloading done
		// by CircleCI too (see .circleci/config.yml).
		"consul:0.9",
		NetworkName,
		[]int{},
		nil,
		// Run consul in "dev" mode so that the initial leader election is immediate
		NewCommand("agent", "-server", "-client=0.0.0.0", "-dev", "-log-level=err"),
		nil,
	))
}

// StartMinio starts minio server, used as a local replacement for S3.
func (s *Scenario) StartMinio() error {
	return s.StartService(NewService(
		"minio",
		// If you change the image tag, remember to update it in the preloading done
		// by CircleCI too (see .circleci/config.yml).
		"minio/minio:RELEASE.2019-12-30T05-45-39Z",
		NetworkName,
		[]int{9000},
		map[string]string{
			"MINIO_ACCESS_KEY": MinioAccessKey,
			"MINIO_SECRET_KEY": MinioSecretKey,
			"MINIO_BROWSER":    "off",
			"ENABLE_HTTPS":     "0",
		},
		// Create the "cortex" bucket before starting minio
		NewCommandWithoutEntrypoint("sh", "-c", "mkdir -p /data/cortex && minio server --quiet /data"),
		NewReadinessProbe(9000, "/minio/health/ready", 200),
	))
}

func (s *Scenario) StartDynamoDB() error {
	return s.StartService(NewService(
		"dynamodb",
		// If you change the image tag, remember to update it in the preloading done
		// by CircleCI too (see .circleci/config.yml).
		"amazon/dynamodb-local:1.11.477",
		NetworkName,
		[]int{8000},
		nil,
		NewCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb"),
		// DynamoDB doesn't have a readiness probe, so we check if the / works even if returns 400
		NewReadinessProbe(8000, "/", 400),
	))
}

func (s *Scenario) StartDistributor(name string, flags map[string]string, image string) error {
	if image == "" {
		image = getDefaultCortexImage()
	}

	return s.StartService(NewService(
		name,
		image,
		NetworkName,
		[]int{80},
		nil,
		NewCommandWithoutEntrypoint("cortex", BuildArgs(MergeFlags(map[string]string{
			"-target":                         "distributor",
			"-log.level":                      "warn",
			"-auth.enabled":                   "true",
			"-distributor.replication-factor": "1",
			// Configure the ingesters ring backend
			"-ring.store":      "consul",
			"-consul.hostname": "consul:8500",
		}, flags))...),
		NewReadinessProbe(80, "/ring", 200),
	))
}

func (s *Scenario) StartQuerier(name string, flags map[string]string, image string) error {
	if image == "" {
		image = getDefaultCortexImage()
	}

	return s.StartService(NewService(
		name,
		image,
		NetworkName,
		[]int{80},
		nil,
		NewCommandWithoutEntrypoint("cortex", BuildArgs(MergeFlags(map[string]string{
			"-target":                         "querier",
			"-log.level":                      "warn",
			"-distributor.replication-factor": "1",
			// Configure the ingesters ring backend
			"-ring.store":      "consul",
			"-consul.hostname": "consul:8500",
		}, flags))...),
		NewReadinessProbe(80, "/ready", 204),
	))
}

func (s *Scenario) StartIngester(name string, flags map[string]string, image string) error {
	if image == "" {
		image = getDefaultCortexImage()
	}

	return s.StartService(NewService(
		name,
		image,
		NetworkName,
		[]int{80},
		nil,
		NewCommandWithoutEntrypoint("cortex", BuildArgs(MergeFlags(map[string]string{
			"-target":                        "ingester",
			"-log.level":                     "warn",
			"-ingester.final-sleep":          "0s",
			"-ingester.join-after":           "0s",
			"-ingester.min-ready-duration":   "0s",
			"-ingester.concurrent-flushes":   "10",
			"-ingester.max-transfer-retries": "10",
			"-ingester.num-tokens":           "512",
			// Configure the ingesters ring backend
			"-ring.store":      "consul",
			"-consul.hostname": "consul:8500",
		}, flags))...),
		NewReadinessProbe(80, "/ready", 204),
	))
}

func (s *Scenario) StartTableManager(name string, flags map[string]string, image string) error {
	if image == "" {
		image = getDefaultCortexImage()
	}

	return s.StartService(NewService(
		name,
		image,
		NetworkName,
		[]int{80},
		nil,
		NewCommandWithoutEntrypoint("cortex", BuildArgs(MergeFlags(map[string]string{
			"-target":    "table-manager",
			"-log.level": "warn",
		}, flags))...),
		// The table-manager doesn't expose a readiness probe, so we just check if the / returns 404
		NewReadinessProbe(80, "/", 404),
	))
}

func (s *Scenario) StopService(name string) error {
	service := s.Service(name)
	if service == nil {
		return fmt.Errorf("unable to stop service %s because does not exist", name)
	}

	if err := service.Stop(); err != nil {
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
			return fmt.Errorf("Unable to wait for service '%s' ready because the service does not exist", name)
		}

		if err := service.WaitReady(); err != nil {
			return err
		}
	}

	return nil
}

func (s *Scenario) Shutdown() {
	// Kill the services in the opposite order
	for i := len(s.services) - 1; i >= 0; i-- {
		if err := s.services[i].Kill(); err != nil {
			fmt.Println("Unable to kill service", s.services[i].name, ":", err.Error())
		}
	}

	// Ensure there are no leftover containers
	if out, err := RunCommandAndGetOutput("docker", "ps", "-a", "--quiet", "--filter", fmt.Sprintf("network=%s", NetworkName)); err == nil {
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

// getDefaultCortexImage returns the Docker image to use to run Cortex.
func getDefaultCortexImage() string {
	// Get the cortex image from the CORTEX_IMAGE env variable,
	// falling back to "quay.io/cortexproject/cortex:latest"
	if os.Getenv("CORTEX_IMAGE") != "" {
		return os.Getenv("CORTEX_IMAGE")
	}

	return "quay.io/cortexproject/cortex:latest"
}

// getIntegrationDir returns the absolute path of the integration dir on the host.
func getIntegrationDir() string {
	if os.Getenv("CORTEX_INTEGRATION_DIR") != "" {
		return os.Getenv("CORTEX_INTEGRATION_DIR")
	}

	return os.Getenv("GOPATH") + "/src/github.com/cortexproject/cortex/integration"
}
