//go:build integration

package integration

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/integration/e2e"
	e2ecache "github.com/cortexproject/cortex/integration/e2e/cache"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/log"
	cortex_testutil "github.com/cortexproject/cortex/pkg/util/test"
)

func TestParquetConverter_NoConvertMarkWithTooManyLabels(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsulWithName("consul")
	memcached := e2ecache.NewMemcached()
	require.NoError(t, s.StartAndWaitReady(consul, memcached))

	baseFlags := mergeFlags(AlertmanagerLocalFlags(), BlocksStorageFlags())
	flags := mergeFlags(
		baseFlags,
		map[string]string{
			"-target": "all,parquet-converter",
			"-blocks-storage.tsdb.block-ranges-period":                             "1m,24h",
			"-blocks-storage.tsdb.ship-interval":                                   "1s",
			"-blocks-storage.bucket-store.sync-interval":                           "1s",
			"-blocks-storage.bucket-store.metadata-cache.bucket-index-content-ttl": "1s",
			"-blocks-storage.bucket-store.bucket-index.idle-timeout":               "1s",
			"-blocks-storage.bucket-store.bucket-index.enabled":                    "true",
			"-blocks-storage.bucket-store.index-cache.backend":                     tsdb.IndexCacheBackendInMemory,
			// compactor
			"-compactor.cleanup-interval": "1s",
			// Ingester.
			"-ring.store":      "consul",
			"-consul.hostname": consul.NetworkHTTPEndpoint(),
			// Distributor.
			"-distributor.replication-factor": "1",
			// Store-gateway.
			"-store-gateway.sharding-enabled":   "false",
			"--querier.store-gateway-addresses": "nonExistent", // Make sure we do not call Store gateways
			// alert manager
			"-alertmanager.web.external-url": "http://localhost/alertmanager",
			// Enable vertical sharding.
			"-frontend.query-vertical-shard-size": "3",
			"-frontend.max-cache-freshness":       "1m",
			// enable experimental promQL funcs
			"-querier.enable-promql-experimental-functions": "true",
			// parquet-converter
			"-parquet-converter.ring.consul.hostname":  consul.NetworkHTTPEndpoint(),
			"-parquet-converter.conversion-interval":   "1s",
			"-parquet-converter.enabled":               "true",
			"-parquet-converter.max-block-label-names": "1",
			// Querier
			"-querier.enable-parquet-queryable": "true",
			// Enable cache for parquet labels and chunks
			"-blocks-storage.bucket-store.parquet-labels-cache.backend":             "inmemory,memcached",
			"-blocks-storage.bucket-store.parquet-labels-cache.memcached.addresses": "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
			"-blocks-storage.bucket-store.chunks-cache.backend":                     "inmemory,memcached",
			"-blocks-storage.bucket-store.chunks-cache.memcached.addresses":         "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		},
	)

	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	ctx := context.Background()
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	dir := filepath.Join(s.SharedDir(), "data")
	lbls := []labels.Labels{
		labels.FromStrings("__name__", "test_series_a", "job", "test"),
	}

	numSamples := 60
	scrapeInterval := time.Minute
	now := time.Now()
	start := now.Add(-time.Hour * 24)
	end := now.Add(-time.Hour)

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))
	storage, err := e2ecortex.NewS3ClientForMinio(minio, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, err)
	bkt := bucket.NewUserBucketClient("user-1", storage.GetBucket(), nil)

	id, err := e2e.CreateBlock(ctx, rnd, dir, lbls, numSamples,
		start.UnixMilli(),
		end.UnixMilli(),
		scrapeInterval.Milliseconds(), 10,
	)
	require.NoError(t, err)

	err = block.Upload(ctx, log.Logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc)
	require.NoError(t, err)

	// Wait for the converter to write the no-convert marker
	cortex_testutil.Poll(t, 30*time.Second, true, func() interface{} {
		noConvertMarkerPath := fmt.Sprintf("%s/parquet-no-convert-mark.json", id.String())
		found := false
		err := bkt.Iter(ctx, "", func(name string) error {
			if name == noConvertMarkerPath {
				found = true
			}
			return nil
		}, objstore.WithRecursiveIter())
		require.NoError(t, err)
		return found
	})

	// confirm the conversion did not happen (check both paths)
	blockID := id.String()
	markerPaths := []string{
		fmt.Sprintf("%s/parquet-converter-mark.json", blockID),
		fmt.Sprintf("parquet-markers/%s-parquet-converter-mark.json", blockID),
	}
	for _, markerPath := range markerPaths {
		exists, err := bkt.Exists(ctx, markerPath)
		require.NoError(t, err)
		require.False(t, exists, "converter mark should not exist at %s", markerPath)
	}
}
