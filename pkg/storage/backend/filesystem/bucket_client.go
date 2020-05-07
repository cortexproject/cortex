package filesystem

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/filesystem"

	"github.com/cortexproject/cortex/pkg/storage/backend/instrumentation"
)

// NewBucketClient creates a new filesystem bucket client
func NewBucketClient(cfg Config, name string, reg prometheus.Registerer) (objstore.Bucket, error) {
	client, err := filesystem.NewBucket(cfg.Directory)
	if err != nil {
		return nil, err
	}

	return instrumentation.BucketWithMetrics(client, name, reg), nil
}
