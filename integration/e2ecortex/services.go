package e2ecortex

import (
	"os"
	"path/filepath"

	"github.com/cortexproject/cortex/integration/e2e"
)

const (
	httpPort = 80
	grpcPort = 9095
)

// GetDefaultImage returns the Docker image to use to run Cortex.
func GetDefaultImage() string {
	// Get the cortex image from the CORTEX_IMAGE env variable,
	// falling back to "quay.io/cortexproject/cortex:latest"
	if os.Getenv("CORTEX_IMAGE") != "" {
		return os.Getenv("CORTEX_IMAGE")
	}

	return "quay.io/cortexproject/cortex:latest"
}

func NewDistributor(name string, consulAddress string, flags map[string]string, image string) *CortexService {
	return NewDistributorWithConfigFile(name, consulAddress, "", flags, image)
}

func NewDistributorWithConfigFile(name, consulAddress, configFile string, flags map[string]string, image string) *CortexService {
	if configFile != "" {
		flags["-config.file"] = filepath.Join(e2e.ContainerSharedDir, configFile)
	}

	if image == "" {
		image = GetDefaultImage()
	}

	return NewCortexService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":                         "distributor",
			"-log.level":                      "warn",
			"-auth.enabled":                   "true",
			"-distributor.replication-factor": "1",
			// Configure the ingesters ring backend
			"-ring.store":      "consul",
			"-consul.hostname": consulAddress,
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewQuerier(name string, consulAddress string, flags map[string]string, image string) *CortexService {
	return NewQuerierWithConfigFile(name, consulAddress, "", flags, image)
}

func NewQuerierWithConfigFile(name, consulAddress, configFile string, flags map[string]string, image string) *CortexService {
	if configFile != "" {
		flags["-config.file"] = filepath.Join(e2e.ContainerSharedDir, configFile)
	}

	if image == "" {
		image = GetDefaultImage()
	}

	return NewCortexService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":                         "querier",
			"-log.level":                      "warn",
			"-distributor.replication-factor": "1",
			// Configure the ingesters ring backend
			"-ring.store":      "consul",
			"-consul.hostname": consulAddress,
			// Query-frontend worker
			"-querier.frontend-client.backoff-min-period": "100ms",
			"-querier.frontend-client.backoff-max-period": "100ms",
			"-querier.frontend-client.backoff-retries":    "1",
			"-querier.worker-parallelism":                 "1",
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewIngester(name string, consulAddress string, flags map[string]string, image string) *CortexService {
	return NewIngesterWithConfigFile(name, consulAddress, "", flags, image)
}

func NewIngesterWithConfigFile(name, consulAddress, configFile string, flags map[string]string, image string) *CortexService {
	if configFile != "" {
		flags["-config.file"] = filepath.Join(e2e.ContainerSharedDir, configFile)
	}
	if image == "" {
		image = GetDefaultImage()
	}

	return NewCortexService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
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
			"-consul.hostname": consulAddress,
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewTableManager(name string, flags map[string]string, image string) *CortexService {
	return NewTableManagerWithConfigFile(name, "", flags, image)
}

func NewTableManagerWithConfigFile(name, configFile string, flags map[string]string, image string) *CortexService {
	if configFile != "" {
		flags["-config.file"] = filepath.Join(e2e.ContainerSharedDir, configFile)
	}

	if image == "" {
		image = GetDefaultImage()
	}

	return NewCortexService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "table-manager",
			"-log.level": "warn",
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewQueryFrontend(name string, flags map[string]string, image string) *CortexService {
	return NewQueryFrontendWithConfigFile(name, "", flags, image)
}

func NewQueryFrontendWithConfigFile(name, configFile string, flags map[string]string, image string) *CortexService {
	if configFile != "" {
		flags["-config.file"] = filepath.Join(e2e.ContainerSharedDir, configFile)
	}

	if image == "" {
		image = GetDefaultImage()
	}

	return NewCortexService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "query-frontend",
			"-log.level": "warn",
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewSingleBinary(name string, flags map[string]string, image string, httpPort, grpcPort int, otherPorts ...int) *CortexService {
	if image == "" {
		image = GetDefaultImage()
	}

	return NewCortexService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-log.level": "warn",
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
		otherPorts...,
	)
}

func NewAlertmanager(name string, flags map[string]string, image string) *CortexService {
	if image == "" {
		image = GetDefaultImage()
	}

	return NewCortexService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "alertmanager",
			"-log.level": "warn",
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewRuler(name string, flags map[string]string, image string) *CortexService {
	if image == "" {
		image = GetDefaultImage()
	}

	return NewCortexService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "ruler",
			"-log.level": "warn",
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}
