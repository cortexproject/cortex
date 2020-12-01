package backend

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/storage/backend/azure"
	"github.com/cortexproject/cortex/pkg/storage/backend/filesystem"
	"github.com/cortexproject/cortex/pkg/storage/backend/gcs"
	"github.com/cortexproject/cortex/pkg/storage/backend/s3"
	"github.com/cortexproject/cortex/pkg/storage/backend/swift"
	"github.com/cortexproject/cortex/pkg/util"
)

const (
	// S3 is the value for the S3 storage backend.
	S3 = "s3"

	// GCS is the value for the GCS storage backend.
	GCS = "gcs"

	// Azure is the value for the Azure storage backend.
	Azure = "azure"

	// Swift is the value for the Openstack Swift storage backend.
	Swift = "swift"

	// Filesystem is the value for the filesystem storage backend.
	Filesystem = "filesystem"
)

var (
	supportedBackends = []string{S3, GCS, Azure, Swift, Filesystem}

	ErrUnsupportedStorageBackend = errors.New("unsupported storage backend")
)

// BucketConfig holds configuration for accessing long-term storage.
type BucketConfig struct {
	Backend string `yaml:"backend"`
	// Backends
	S3         s3.Config         `yaml:"s3"`
	GCS        gcs.Config        `yaml:"gcs"`
	Azure      azure.Config      `yaml:"azure"`
	Swift      swift.Config      `yaml:"swift"`
	Filesystem filesystem.Config `yaml:"filesystem"`

	// Not used internally, meant to allow callers to wrap Buckets
	// created using this config
	Middlewares []func(objstore.Bucket) (objstore.Bucket, error) `yaml:"-"`
}

// RegisterFlags registers the TSDB Backend
func (cfg *BucketConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.S3.RegisterFlags(f)
	cfg.GCS.RegisterFlags(f)
	cfg.Azure.RegisterFlags(f)
	cfg.Swift.RegisterFlags(f)
	cfg.Filesystem.RegisterFlags(f)

	f.StringVar(&cfg.Backend, "blocks-storage.backend", "s3", fmt.Sprintf("Backend storage to use. Supported backends are: %s.", strings.Join(supportedBackends, ", ")))
}

func (cfg *BucketConfig) Validate() error {
	if !util.StringsContain(supportedBackends, cfg.Backend) {
		return ErrUnsupportedStorageBackend
	}

	if cfg.Backend == S3 {
		if err := cfg.S3.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// NewBucketClient creates a new bucket client based on the configured backend
func NewBucketClient(ctx context.Context, cfg BucketConfig, name string, logger log.Logger, reg prometheus.Registerer) (client objstore.Bucket, err error) {
	switch cfg.Backend {
	case S3:
		client, err = s3.NewBucketClient(cfg.S3, name, logger)
	case GCS:
		client, err = gcs.NewBucketClient(ctx, cfg.GCS, name, logger)
	case Azure:
		client, err = azure.NewBucketClient(cfg.Azure, name, logger)
	case Swift:
		client, err = swift.NewBucketClient(cfg.Swift, name, logger)
	case Filesystem:
		client, err = filesystem.NewBucketClient(cfg.Filesystem)
	default:
		return nil, ErrUnsupportedStorageBackend
	}

	if err != nil {
		return nil, err
	}

	client = objstore.NewTracingBucket(bucketWithMetrics(client, name, reg))

	// Wrap the client with any provided middleware
	for _, wrap := range cfg.Middlewares {
		client, err = wrap(client)
		if err != nil {
			return nil, err
		}
	}

	return client, nil
}

func bucketWithMetrics(bucketClient objstore.Bucket, name string, reg prometheus.Registerer) objstore.Bucket {
	if reg == nil {
		return bucketClient
	}

	return objstore.BucketWithMetrics(
		"", // bucket label value
		bucketClient,
		prometheus.WrapRegistererWith(prometheus.Labels{"component": name}, reg))
}
