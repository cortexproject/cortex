package e2ecortex

import (
	"os"

	"github.com/cortexproject/cortex/integration/e2e"
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

func NewDistributor(name string, flags map[string]string, image string) *e2e.Service {
	if image == "" {
		image = GetDefaultImage()
	}

	return e2e.NewService(
		name,
		image,
		e2e.NetworkName,
		[]int{80},
		nil,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":                         "distributor",
			"-log.level":                      "warn",
			"-auth.enabled":                   "true",
			"-distributor.replication-factor": "1",
			// Configure the ingesters ring backend
			"-ring.store":      "consul",
			"-consul.hostname": "consul:8500",
		}, flags))...),
		e2e.NewReadinessProbe(80, "/ring", 200),
	)
}

func NewQuerier(name string, flags map[string]string, image string) *e2e.Service {
	if image == "" {
		image = GetDefaultImage()
	}

	return e2e.NewService(
		name,
		image,
		e2e.NetworkName,
		[]int{80},
		nil,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":                         "querier",
			"-log.level":                      "warn",
			"-distributor.replication-factor": "1",
			// Configure the ingesters ring backend
			"-ring.store":      "consul",
			"-consul.hostname": "consul:8500",
		}, flags))...),
		e2e.NewReadinessProbe(80, "/ready", 204),
	)
}

func NewIngester(name string, flags map[string]string, image string) *e2e.Service {
	if image == "" {
		image = GetDefaultImage()
	}

	return e2e.NewService(
		name,
		image,
		e2e.NetworkName,
		[]int{80},
		nil,
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
			"-consul.hostname": "consul:8500",
		}, flags))...),
		e2e.NewReadinessProbe(80, "/ready", 204),
	)
}

func NewTableManager(name string, flags map[string]string, image string) *e2e.Service {
	if image == "" {
		image = GetDefaultImage()
	}

	return e2e.NewService(
		name,
		image,
		e2e.NetworkName,
		[]int{80},
		nil,
		e2e.NewCommandWithoutEntrypoint("cortex", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "table-manager",
			"-log.level": "warn",
		}, flags))...),
		// The table-manager doesn't expose a readiness probe, so we just check if the / returns 404
		e2e.NewReadinessProbe(80, "/", 404),
	)
}
