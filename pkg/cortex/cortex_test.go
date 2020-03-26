package cortex

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/storage/backend/s3"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestCortex(t *testing.T) {
	cfg := Config{
		Storage: storage.Config{
			Engine: storage.StorageEngineTSDB, // makes config easier
		},
		Ingester: ingester.Config{
			TSDBConfig: tsdb.Config{
				Backend: tsdb.BackendS3,
				S3: s3.Config{
					Endpoint: "localhost",
				},
			},
			LifecyclerConfig: ring.LifecyclerConfig{
				RingConfig: ring.Config{
					KVStore: kv.Config{
						Store: "inmemory",
					},
					ReplicationFactor: 3,
				},
				InfNames: []string{"en0", "eth0", "lo0"},
			},
		},
		TSDB: tsdb.Config{
			Backend: tsdb.BackendS3,
			S3: s3.Config{
				Endpoint: "localhost",
			},
			BucketStore: tsdb.BucketStoreConfig{
				IndexCache: tsdb.IndexCacheConfig{
					Backend: tsdb.IndexCacheBackendInMemory,
				},
			},
		},
		Target: All,
	}

	c, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, c.serviceMap)

	for m, s := range c.serviceMap {
		// make sure each service is still New
		require.Equal(t, services.New, s.State(), "module: %s", m)
	}

	// check random modules that we expect to be configured when using Target=All
	require.NotNil(t, c.serviceMap[Server])
	require.NotNil(t, c.serviceMap[Ingester])
	require.NotNil(t, c.serviceMap[Ring])
	require.NotNil(t, c.serviceMap[Distributor])

	// check that findInverseDependencie for Ring -- querier and distributor depend on Ring, so should be returned.
	require.ElementsMatch(t, []ModuleName{Distributor, Querier}, findInverseDependencies(Ring, modules[cfg.Target].deps))
}
