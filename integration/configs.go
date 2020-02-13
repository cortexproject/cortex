package main

import (
	"path/filepath"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
)

const (
	cortexSchemaConfigFile = "chunks-storage-schema-dynamodb.yaml"
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
)

var (
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
		"-experimental.tsdb.s3.endpoint":                "minio:9000",
		"-experimental.tsdb.s3.insecure":                "true",
	}

	ChunksStorage = map[string]string{
		"-dynamodb.url":                   "dynamodb://u:p@dynamodb.:8000",
		"-dynamodb.poll-interval":         "1m",
		"-config-yaml":                    filepath.Join(e2e.ContainerSharedDir, cortexSchemaConfigFile),
		"-table-manager.retention-period": "168h",
	}
)
