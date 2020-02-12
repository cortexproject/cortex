package main

import "github.com/cortexproject/cortex/integration/framework"

var (
	BlocksStorage = map[string]string{
		"-store.engine":                                 "tsdb",
		"-experimental.tsdb.backend":                    "s3",
		"-experimental.tsdb.block-ranges-period":        "1m",
		"-experimental.tsdb.bucket-store.sync-interval": "5s",
		"-experimental.tsdb.retention-period":           "5m",
		"-experimental.tsdb.ship-interval":              "1m",
		"-experimental.tsdb.s3.access-key-id":           framework.MinioAccessKey,
		"-experimental.tsdb.s3.secret-access-key":       framework.MinioSecretKey,
		"-experimental.tsdb.s3.bucket-name":             "cortex",
		"-experimental.tsdb.s3.endpoint":                "minio:9000",
		"-experimental.tsdb.s3.insecure":                "true",
	}

	ChunksStorage = map[string]string{
		"-dynamodb.url":                   "dynamodb://u:p@dynamodb.:8000",
		"-dynamodb.poll-interval":         "1m",
		"-config-yaml":                    "/integration/chunks-storage-schema-dynamodb.yaml",
		"-table-manager.retention-period": "168h",
	}
)
