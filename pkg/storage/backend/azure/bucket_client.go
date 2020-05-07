package azure

import (
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/azure"
	yaml "gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/storage/backend/instrumentation"
)

func NewBucketClient(cfg Config, name string, logger log.Logger, reg prometheus.Registerer) (objstore.Bucket, error) {
	bucketConfig := azure.Config{
		StorageAccountName: cfg.StorageAccountName,
		StorageAccountKey:  cfg.StorageAccountKey.Value,
		ContainerName:      cfg.ContainerName,
		Endpoint:           cfg.Endpoint,
		MaxRetries:         cfg.MaxRetries,
	}

	// Thanos currently doesn't support passing the config as is, but expects a YAML,
	// so we're going to serialize it.
	serialized, err := yaml.Marshal(bucketConfig)
	if err != nil {
		return nil, err
	}

	client, err := azure.NewBucket(logger, serialized, name)
	if err != nil {
		return nil, err
	}

	return instrumentation.BucketWithMetrics(client, name, reg), nil
}
