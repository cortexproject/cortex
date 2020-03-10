package filesystem

import (
	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/filesystem"
)

// NewBucketClient creates a new filesystem bucket client
func NewBucketClient(cfg Config, name string, logger log.Logger) (objstore.Bucket, error) {
	return filesystem.NewBucket(cfg.Directory)
}
