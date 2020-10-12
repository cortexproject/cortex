// +build requires_docker

package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
)

type storeConfig struct {
	From, IndexStore string
}

const (
	defaultNetworkName     = "e2e-cortex-test"
	bucketName             = "cortex"
	cortexConfigFile       = "config.yaml"
	cortexSchemaConfigFile = "schema.yaml"
	blocksStorageEngine    = "blocks"
	clientCertFile         = "certs/client.crt"
	clientKeyFile          = "certs/client.key"
	caCertFile             = "certs/root.crt"
	serverCertFile         = "certs/server.crt"
	serverKeyFile          = "certs/server.key"
)

// GetNetworkName returns the docker network name to run tests within.
func GetNetworkName() string {
	// If the E2E_NETWORK_NAME is set, use that for the network name.
	// Otherwise, return the default network name.
	if os.Getenv("E2E_NETWORK_NAME") != "" {
		return os.Getenv("E2E_NETWORK_NAME")
	}

	return defaultNetworkName
}

var (
	networkName         = GetNetworkName()
	storeConfigTemplate = `
- from: {{.From}}
  store: {{.IndexStore}}
  schema: v9
  index:
    prefix: cortex_
    period: 168h
  chunks:
    prefix: cortex_chunks_
    period: 168h
`

	cortexAlertmanagerUserConfigYaml = `route:
  receiver: "example_receiver"
  group_by: ["example_groupby"]
receivers:
  - name: "example_receiver"
`

	cortexRulerUserConfigYaml = `groups:
- name: rule
  interval: 100s
  rules:
  - record: test_rule
    alert: ""
    expr: up
    for: 0s
    labels: {}
    annotations: {}	
`

	cortexRulerEvalTimeConfigYaml = `groups:
- name: rule
  interval: 1s
  rules:
  - record: time_eval
    alert: ""
    expr: time()
    for: 0s
    labels: {}
    annotations: {}	
`
)

var (
	cortexSchemaConfigYaml = buildSchemaConfigWith([]storeConfig{{From: "2019-03-20", IndexStore: "aws-dynamo"}})

	AlertmanagerFlags = map[string]string{
		"-alertmanager.configs.poll-interval": "1s",
		"-alertmanager.web.external-url":      "http://localhost/api/prom",
	}

	AlertmanagerLocalFlags = map[string]string{
		"-alertmanager.storage.type":       "local",
		"-alertmanager.storage.local.path": filepath.Join(e2e.ContainerSharedDir, "alertmanager_configs"),
	}

	AlertmanagerS3Flags = map[string]string{
		"-alertmanager.storage.type":                "s3",
		"-alertmanager.storage.s3.buckets":          "cortex-alerts",
		"-alertmanager.storage.s3.force-path-style": "true",
		"-alertmanager.storage.s3.url":              fmt.Sprintf("s3://%s:%s@%s-minio-9000.:9000", e2edb.MinioAccessKey, e2edb.MinioSecretKey, networkName),
	}

	RulerConfigs = map[string]string{
		"-ruler.enable-sharding":             "false",
		"-ruler.poll-interval":               "2s",
		"-experimental.ruler.enable-api":     "true",
		"-ruler.storage.type":                "s3",
		"-ruler.storage.s3.buckets":          "cortex-rules",
		"-ruler.storage.s3.force-path-style": "true",
		"-ruler.storage.s3.url":              fmt.Sprintf("s3://%s:%s@%s-minio-9000.:9000", e2edb.MinioAccessKey, e2edb.MinioSecretKey, networkName),
	}

	BlocksStorageFlags = map[string]string{
		"-store.engine":                                 blocksStorageEngine,
		"-blocks-storage.backend":                       "s3",
		"-blocks-storage.tsdb.block-ranges-period":      "1m",
		"-blocks-storage.bucket-store.sync-interval":    "5s",
		"-blocks-storage.tsdb.retention-period":         "5m",
		"-blocks-storage.tsdb.ship-interval":            "1m",
		"-blocks-storage.tsdb.head-compaction-interval": "1s",
		"-blocks-storage.s3.access-key-id":              e2edb.MinioAccessKey,
		"-blocks-storage.s3.secret-access-key":          e2edb.MinioSecretKey,
		"-blocks-storage.s3.bucket-name":                bucketName,
		"-blocks-storage.s3.endpoint":                   fmt.Sprintf("%s-minio-9000:9000", networkName),
		"-blocks-storage.s3.insecure":                   "true",
	}

	BlocksStorageConfig = buildConfigFromTemplate(`
storage:
  engine: blocks

blocks_storage:
  backend:             s3

  tsdb:
    block_ranges_period: ["1m"]
    retention_period:    5m
    ship_interval:       1m

  bucket_store:
    sync_interval: 5s

  s3:
    bucket_name:       cortex
    access_key_id:     {{.MinioAccessKey}}
    secret_access_key: {{.MinioSecretKey}}
    endpoint:          {{.MinioEndpoint}}
    insecure:          true
`, struct {
		MinioAccessKey string
		MinioSecretKey string
		MinioEndpoint  string
	}{
		MinioAccessKey: e2edb.MinioAccessKey,
		MinioSecretKey: e2edb.MinioSecretKey,
		MinioEndpoint:  fmt.Sprintf("%s-minio-9000:9000", networkName),
	})

	ChunksStorageFlags = map[string]string{
		"-dynamodb.url":                   fmt.Sprintf("dynamodb://u:p@%s-dynamodb.:8000", networkName),
		"-table-manager.poll-interval":    "1m",
		"-schema-config-file":             filepath.Join(e2e.ContainerSharedDir, cortexSchemaConfigFile),
		"-table-manager.retention-period": "168h",
	}

	ChunksStorageConfig = buildConfigFromTemplate(`
storage:
  aws:
    dynamodb:
      dynamodb_url: {{.DynamoDBURL}}

table_manager:
  poll_interval:    1m
  retention_period: 168h

schema:
{{.SchemaConfig}}
`, struct {
		DynamoDBURL  string
		SchemaConfig string
	}{
		DynamoDBURL:  fmt.Sprintf("dynamodb://u:p@%s-dynamodb.:8000", networkName),
		SchemaConfig: indentConfig(cortexSchemaConfigYaml, 2),
	})
)

func buildConfigFromTemplate(tmpl string, data interface{}) string {
	t, err := template.New("config").Parse(tmpl)
	if err != nil {
		panic(err)
	}

	w := &strings.Builder{}
	if err = t.Execute(w, data); err != nil {
		panic(err)
	}

	return w.String()
}

func indentConfig(config string, indentation int) string {
	output := strings.Builder{}

	for _, line := range strings.Split(config, "\n") {
		if line == "" {
			output.WriteString("\n")
			continue
		}

		output.WriteString(strings.Repeat(" ", indentation))
		output.WriteString(line)
		output.WriteString("\n")
	}

	return output.String()
}

func buildSchemaConfigWith(configs []storeConfig) string {
	configYamls := ""
	for _, config := range configs {
		configYamls += buildConfigFromTemplate(
			storeConfigTemplate,
			config,
		)
	}

	return "configs:" + configYamls
}
