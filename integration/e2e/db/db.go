package e2edb

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/cortexproject/cortex/integration/e2e/images"
)

const (
	MinioAccessKey = "Cheescake"
	MinioSecretKey = "supersecret"
)

// NewMinio returns minio server, used as a local replacement for S3.
func NewMinio(port int, bktNames ...string) *e2e.HTTPService {
	return newMinio(port, map[string]string{}, bktNames...)
}

func newMinio(port int, envVars map[string]string, bktNames ...string) *e2e.HTTPService {
	commands := []string{}
	for _, bkt := range bktNames {
		commands = append(commands, fmt.Sprintf("mkdir -p /data/%s", bkt))
	}
	commands = append(commands, fmt.Sprintf("minio server --address :%v --quiet /data", port))

	m := e2e.NewHTTPService(
		fmt.Sprintf("minio-%v", port),
		images.Minio,
		// Create the "cortex" bucket before starting minio
		e2e.NewCommandWithoutEntrypoint("sh", "-c", strings.Join(commands, " && ")),
		e2e.NewHTTPReadinessProbe(port, "/minio/health/live", 200, 200),
		port,
	)
	envVars["MINIO_ACCESS_KEY"] = MinioAccessKey
	envVars["MINIO_SECRET_KEY"] = MinioSecretKey
	envVars["MINIO_BROWSER"] = "off"
	envVars["ENABLE_HTTPS"] = "0"
	m.SetEnvVars(envVars)
	return m
}

func NewConsul() *e2e.HTTPService {
	return NewConsulWithName("consul")
}

func NewConsulWithName(name string) *e2e.HTTPService {
	return e2e.NewHTTPService(
		name,
		images.Consul,
		// Run consul in "dev" mode so that the initial leader election is immediate
		e2e.NewCommand("agent", "-server", "-client=0.0.0.0", "-dev", "-log-level=err"),
		e2e.NewHTTPReadinessProbe(8500, "/v1/operator/autopilot/health", 200, 200, `"Healthy": true`),
		8500,
	)
}

func NewETCD() *e2e.HTTPService {
	return e2e.NewHTTPService(
		"etcd",
		images.ETCD,
		e2e.NewCommand("/usr/local/bin/etcd", "--listen-client-urls=http://0.0.0.0:2379", "--advertise-client-urls=http://0.0.0.0:2379", "--listen-metrics-urls=http://0.0.0.0:9000", "--log-level=error"),
		e2e.NewHTTPReadinessProbe(9000, "/health", 200, 204),
		2379,
		9000, // Metrics
	)
}
func NewPrometheus(flags map[string]string) *e2e.HTTPService {
	return NewPrometheusWithName("prometheus", flags)
}

func NewPrometheusWithName(name string, flags map[string]string) *e2e.HTTPService {
	prom := e2e.NewHTTPService(
		name,
		images.Prometheus,
		e2e.NewCommandWithoutEntrypoint("prometheus", e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"--storage.tsdb.path":               filepath.Join(e2e.ContainerSharedDir, "data"),
			"--storage.tsdb.max-block-duration": "2h",
			"--log.level":                       "info",
			"--web.listen-address":              ":9090",
			"--config.file":                     filepath.Join(e2e.ContainerSharedDir, "prometheus.yml"),
		}, flags))...),
		e2e.NewHTTPReadinessProbe(9090, "/-/ready", 200, 200),
		9090,
	)
	prom.SetUser(strconv.Itoa(os.Getuid()))
	return prom
}
