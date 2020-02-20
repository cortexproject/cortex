package e2ecortex

import (
	"os"

	"github.com/cortexproject/cortex/integration/e2e"
)

const (
	HTTPPort = 80
	GRPCPort = 9095
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

func NewDistributor(name string, consulAddress string, flags map[string]string, image string) *e2e.HTTPService {
	if image == "" {
		image = GetDefaultImage()
	}

	return e2e.NewHTTPService(
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
		e2e.NewReadinessProbe(HTTPPort, "/ring", 200),
		HTTPPort,
	)
}

func NewQuerier(name string, consulAddress string, flags map[string]string, image string) *e2e.HTTPService {
	if image == "" {
		image = GetDefaultImage()
	}

	return e2e.NewHTTPService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":                         "querier",
			"-log.level":                      "info", // TODO warn
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
		e2e.NewReadinessProbe(HTTPPort, "/ready", 204),
		HTTPPort,
	)
}

func NewIngester(name string, consulAddress string, flags map[string]string, image string) *e2e.HTTPService {
	if image == "" {
		image = GetDefaultImage()
	}

	return e2e.NewHTTPService(
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
		e2e.NewReadinessProbe(HTTPPort, "/ready", 204),
		HTTPPort,
	)
}

func NewTableManager(name string, flags map[string]string, image string) *e2e.HTTPService {
	if image == "" {
		image = GetDefaultImage()
	}

	return e2e.NewHTTPService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "table-manager",
			"-log.level": "warn",
		}, flags))...),
		// The table-manager doesn't expose a readiness probe, so we just check if the / returns 404
		e2e.NewReadinessProbe(HTTPPort, "/", 404),
		HTTPPort,
	)
}

func NewQueryFrontend(name string, flags map[string]string, image string) *e2e.HTTPService {
	if image == "" {
		image = GetDefaultImage()
	}

	return e2e.NewHTTPService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "query-frontend",
			"-log.level": "info", // TODO warn
		}, flags))...),
		// The query-frontend doesn't expose a readiness probe, so we just check if the / returns 404
		e2e.NewReadinessProbe(HTTPPort, "/", 404),
		HTTPPort,
		GRPCPort,
	)
}

func NewSingleBinary(name string, flags map[string]string, image string, httpPort int, otherPorts ...int) *e2e.HTTPService {
	if image == "" {
		image = GetDefaultImage()
	}

	return e2e.NewHTTPService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-log.level": "warn",
		}, flags))...),
		e2e.NewReadinessProbe(httpPort, "/ready", 204),
		httpPort,
		otherPorts...,
	)
}

func NewAlertmanager(name string, flags map[string]string, image string) *e2e.HTTPService {
	if image == "" {
		image = GetDefaultImage()
	}

	return e2e.NewHTTPService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "alertmanager",
			"-log.level": "warn",
		}, flags))...),
		// The alertmanager doesn't expose a readiness probe, so we just check if the / returns 404
		e2e.NewReadinessProbe(HTTPPort, "/", 404),
		HTTPPort,
	)
}
