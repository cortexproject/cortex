package cortex

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk/aws"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/storage/backend/s3"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestCortex(t *testing.T) {
	rulerURL, err := url.Parse("inmemory:///rules")
	require.NoError(t, err)

	cfg := Config{
		Storage: storage.Config{
			Engine: storage.StorageEngineBlocks, // makes config easier
		},
		Ingester: ingester.Config{
			BlocksStorageConfig: tsdb.BlocksStorageConfig{
				Bucket: tsdb.BucketConfig{
					Backend: tsdb.BackendS3,
					S3: s3.Config{
						Endpoint: "localhost",
					},
				},
			},
			LifecyclerConfig: ring.LifecyclerConfig{
				RingConfig: ring.Config{
					KVStore: kv.Config{
						Store: "inmemory",
					},
					ReplicationFactor: 3,
				},
				InfNames: []string{"en0", "eth0", "lo0", "lo"},
			},
		},
		BlocksStorage: tsdb.BlocksStorageConfig{
			Bucket: tsdb.BucketConfig{
				Backend: tsdb.BackendS3,
				S3: s3.Config{
					Endpoint: "localhost",
				},
			},
			BucketStore: tsdb.BucketStoreConfig{
				IndexCache: tsdb.IndexCacheConfig{
					Backend: tsdb.IndexCacheBackendInMemory,
				},
			},
		},
		Ruler: ruler.Config{
			StoreConfig: ruler.RuleStoreConfig{
				Type: "s3",
				S3: aws.S3Config{
					S3: flagext.URLValue{
						URL: rulerURL,
					},
				},
			},
		},

		Target: []string{All, Compactor},
	}

	c, err := New(cfg)
	require.NoError(t, err)

	serviceMap, err := c.ModuleManager.InitModuleServices(cfg.Target...)
	require.NoError(t, err)
	require.NotNil(t, serviceMap)

	for m, s := range serviceMap {
		// make sure each service is still New
		require.Equal(t, services.New, s.State(), "module: %s", m)
	}

	// check random modules that we expect to be configured when using Target=All
	require.NotNil(t, serviceMap[Server])
	require.NotNil(t, serviceMap[IngesterService])
	require.NotNil(t, serviceMap[Ring])
	require.NotNil(t, serviceMap[DistributorService])

	// check that compactor is configured which is not part of Target=All
	require.NotNil(t, serviceMap[Compactor])
}
