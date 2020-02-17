package main

import (
	"fmt"
	"path/filepath"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
)

const (
	cortexConfigFile       = "config.yaml"
	cortexSchemaConfigFile = "schema.yaml"
	cortexSchemaConfigYaml = `configs:
- from: "2019-03-20"
  store: aws-dynamo
  schema: v9
  index:
    prefix: cortex_
    period: 168h0m0s
  chunks:
    prefix: cortex_chunks_
    period: 168h0m0s
`

	cortexAlertmanagerUserConfigYaml = `route:
  receiver: "example_receiver"
  group_by: ["example_groupby"]
receivers:
  - name: "example_receiver"
`
)

var (
	AlertmanagerConfigs = map[string]string{
		"-alertmanager.storage.local.path": filepath.Join(e2e.ContainerSharedDir, "alertmanager_configs"),
		"-alertmanager.storage.type":       "local",
		"-alertmanager.web.external-url":   "http://localhost/api/prom",
	}

	BlocksStorage = map[string]string{
		"-store.engine":                                 "tsdb",
		"-experimental.tsdb.backend":                    "s3",
		"-experimental.tsdb.block-ranges-period":        "1m",
		"-experimental.tsdb.bucket-store.sync-interval": "5s",
		"-experimental.tsdb.retention-period":           "5m",
		"-experimental.tsdb.ship-interval":              "1m",
		"-experimental.tsdb.s3.access-key-id":           e2edb.MinioAccessKey,
		"-experimental.tsdb.s3.secret-access-key":       e2edb.MinioSecretKey,
		"-experimental.tsdb.s3.bucket-name":             "cortex",
		"-experimental.tsdb.s3.endpoint":                fmt.Sprintf("%s-minio-9000:9000", networkName),
		"-experimental.tsdb.s3.insecure":                "true",
	}

	ChunksStorage = map[string]string{
		"-dynamodb.url":                   fmt.Sprintf("dynamodb://u:p@%s-dynamodb.:8000", networkName),
		"-dynamodb.poll-interval":         "1m",
		"-config-yaml":                    filepath.Join(e2e.ContainerSharedDir, cortexSchemaConfigFile),
		"-table-manager.retention-period": "168h",
	}
)
