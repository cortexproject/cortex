package e2ecortex

import (
	"os"
	"path/filepath"

	"github.com/cortexproject/cortex/integration/e2e"
)

const (
	httpPort   = 80
	grpcPort   = 9095
	GossipPort = 9094
)

type RingStore string

const (
	RingStoreConsul RingStore = "consul"
	RingStoreEtcd   RingStore = "etcd"
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

func NewDistributor(name string, store RingStore, address string, flags map[string]string, image string) *CortexService {
	return NewDistributorWithConfigFile(name, store, address, "", flags, image)
}

func NewDistributorWithConfigFile(name string, store RingStore, address, configFile string, flags map[string]string, image string) *CortexService {
	if configFile != "" {
		flags["-config.file"] = filepath.Join(e2e.ContainerSharedDir, configFile)
	}

	// Configure the ingesters ring backend
	flags["-ring.store"] = string(store)
	if store == RingStoreConsul {
		flags["-consul.hostname"] = address
	} else if store == RingStoreEtcd {
		flags["-etcd.endpoints"] = address
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
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewQuerier(name string, store RingStore, address string, flags map[string]string, image string) *CortexService {
	return NewQuerierWithConfigFile(name, store, address, "", flags, image)
}

func NewQuerierWithConfigFile(name string, store RingStore, address, configFile string, flags map[string]string, image string) *CortexService {
	if configFile != "" {
		flags["-config.file"] = filepath.Join(e2e.ContainerSharedDir, configFile)
	}

	// Configure the ingesters ring backend and the store-gateway ring backend.
	ringBackendFlags := map[string]string{
		"-ring.store":                        string(store),
		"-store-gateway.sharding-ring.store": string(store),
	}

	if store == RingStoreConsul {
		ringBackendFlags["-consul.hostname"] = address
		ringBackendFlags["-store-gateway.sharding-ring.consul.hostname"] = address
	} else if store == RingStoreEtcd {
		ringBackendFlags["-etcd.endpoints"] = address
		ringBackendFlags["-store-gateway.sharding-ring.etcd.endpoints"] = address
	}

	// For backward compatibility
	flags = e2e.MergeFlagsWithoutRemovingEmpty(ringBackendFlags, flags)

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
			// Query-frontend worker.
			"-querier.frontend-client.backoff-min-period": "100ms",
			"-querier.frontend-client.backoff-max-period": "100ms",
			"-querier.frontend-client.backoff-retries":    "1",
			"-querier.worker-parallelism":                 "1",
			// Quickly detect query-frontend and query-scheduler when running it.
			"-querier.dns-lookup-period": "1s",
			// Store-gateway ring backend.
			"-store-gateway.sharding-enabled":                 "true",
			"-store-gateway.sharding-ring.replication-factor": "1",
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewStoreGateway(name string, store RingStore, address string, flags map[string]string, image string) *CortexService {
	return NewStoreGatewayWithConfigFile(name, store, address, "", flags, image)
}

func NewStoreGatewayWithConfigFile(name string, store RingStore, address string, configFile string, flags map[string]string, image string) *CortexService {
	if configFile != "" {
		flags["-config.file"] = filepath.Join(e2e.ContainerSharedDir, configFile)
	}

	if store == RingStoreConsul {
		flags["-consul.hostname"] = address
		flags["-store-gateway.sharding-ring.consul.hostname"] = address
	} else if store == RingStoreEtcd {
		flags["-etcd.endpoints"] = address
		flags["-store-gateway.sharding-ring.etcd.endpoints"] = address
	}

	if image == "" {
		image = GetDefaultImage()
	}

	return NewCortexService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "store-gateway",
			"-log.level": "warn",
			// Store-gateway ring backend.
			"-store-gateway.sharding-enabled":                 "true",
			"-store-gateway.sharding-ring.store":              string(store),
			"-store-gateway.sharding-ring.replication-factor": "1",
			// Startup quickly.
			"-store-gateway.sharding-ring.wait-stability-min-duration": "0",
			"-store-gateway.sharding-ring.wait-stability-max-duration": "0",
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewIngester(name string, store RingStore, address string, flags map[string]string, image string) *CortexService {
	return NewIngesterWithConfigFile(name, store, address, "", flags, image)
}

func NewIngesterWithConfigFile(name string, store RingStore, address, configFile string, flags map[string]string, image string) *CortexService {
	if configFile != "" {
		flags["-config.file"] = filepath.Join(e2e.ContainerSharedDir, configFile)
	}

	// Configure the ingesters ring backend
	flags["-ring.store"] = string(store)
	if store == RingStoreConsul {
		flags["-consul.hostname"] = address
	} else if store == RingStoreEtcd {
		flags["-etcd.endpoints"] = address
	}

	if image == "" {
		image = GetDefaultImage()
	}

	return NewCortexService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":                      "ingester",
			"-log.level":                   "warn",
			"-ingester.final-sleep":        "0s",
			"-ingester.join-after":         "0s",
			"-ingester.min-ready-duration": "0s",
			"-ingester.concurrent-flushes": "10",
			"-ingester.num-tokens":         "512",
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
			// Quickly detect query-scheduler when running it.
			"-frontend.scheduler-dns-lookup-period": "1s",
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewQueryScheduler(name string, flags map[string]string, image string) *CortexService {
	return NewQuerySchedulerWithConfigFile(name, "", flags, image)
}

func NewQuerySchedulerWithConfigFile(name, configFile string, flags map[string]string, image string) *CortexService {
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
			"-target":    "query-scheduler",
			"-log.level": "warn",
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewCompactor(name string, consulAddress string, flags map[string]string, image string) *CortexService {
	return NewCompactorWithConfigFile(name, consulAddress, "", flags, image)
}

func NewCompactorWithConfigFile(name, consulAddress, configFile string, flags map[string]string, image string) *CortexService {
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
			"-target":    "compactor",
			"-log.level": "warn",
			// Store-gateway ring backend.
			"-compactor.sharding-enabled":     "true",
			"-compactor.ring.store":           "consul",
			"-compactor.ring.consul.hostname": consulAddress,
			// Startup quickly.
			"-compactor.ring.wait-stability-min-duration": "0",
			"-compactor.ring.wait-stability-max-duration": "0",
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewSingleBinary(name string, flags map[string]string, image string, otherPorts ...int) *CortexService {
	if image == "" {
		image = GetDefaultImage()
	}

	return NewCortexService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":       "all",
			"-log.level":    "warn",
			"-auth.enabled": "true",
			// Query-frontend worker.
			"-querier.frontend-client.backoff-min-period": "100ms",
			"-querier.frontend-client.backoff-max-period": "100ms",
			"-querier.frontend-client.backoff-retries":    "1",
			"-querier.worker-parallelism":                 "1",
			// Distributor.
			"-distributor.replication-factor": "1",
			// Ingester.
			"-ingester.final-sleep":        "0s",
			"-ingester.join-after":         "0s",
			"-ingester.min-ready-duration": "0s",
			"-ingester.concurrent-flushes": "10",
			"-ingester.num-tokens":         "512",
			// Startup quickly.
			"-store-gateway.sharding-ring.wait-stability-min-duration": "0",
			"-store-gateway.sharding-ring.wait-stability-max-duration": "0",
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
		otherPorts...,
	)
}

func NewSingleBinaryWithConfigFile(name string, configFile string, flags map[string]string, image string, httpPort, grpcPort int, otherPorts ...int) *CortexService {
	if image == "" {
		image = GetDefaultImage()
	}

	return NewCortexService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			// Do not pass any extra default flags because the config should be drive by the config file.
			"-target":      "all",
			"-log.level":   "warn",
			"-config.file": filepath.Join(e2e.ContainerSharedDir, configFile),
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
			"-target":                               "alertmanager",
			"-log.level":                            "warn",
			"-experimental.alertmanager.enable-api": "true",
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
		GossipPort,
	)
}

func NewAlertmanagerWithTLS(name string, flags map[string]string, image string) *CortexService {
	if image == "" {
		image = GetDefaultImage()
	}

	return NewCortexService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":                               "alertmanager",
			"-log.level":                            "warn",
			"-experimental.alertmanager.enable-api": "true",
		}, flags))...),
		e2e.NewTCPReadinessProbe(httpPort),
		httpPort,
		grpcPort,
		GossipPort,
	)
}

func NewRuler(name string, consulAddress string, flags map[string]string, image string) *CortexService {
	if image == "" {
		image = GetDefaultImage()
	}

	return NewCortexService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "ruler",
			"-log.level": "warn",
			// Configure the ring backend
			"-ring.store":                                  "consul",
			"-store-gateway.sharding-ring.store":           "consul",
			"-consul.hostname":                             consulAddress,
			"-store-gateway.sharding-ring.consul.hostname": consulAddress,
			// Store-gateway ring backend.
			"-store-gateway.sharding-enabled":                 "true",
			"-store-gateway.sharding-ring.replication-factor": "1",
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewPurger(name string, flags map[string]string, image string) *CortexService {
	return NewPurgerWithConfigFile(name, "", flags, image)
}

func NewPurgerWithConfigFile(name, configFile string, flags map[string]string, image string) *CortexService {
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
			"-target":                   "purger",
			"-log.level":                "warn",
			"-purger.object-store-type": "filesystem",
			"-local.chunk-directory":    e2e.ContainerSharedDir,
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}
