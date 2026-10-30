package storegateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/types"
	ulidv2 "github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	thanos_metadata "github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/bucket/filesystem"
	cortex_parquet "github.com/cortexproject/cortex/pkg/storage/parquet"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestHybridBucketStores_Series_ShouldReturnErrorIfMaxInflightRequestIsReached(t *testing.T) {
	cfg := prepareStorageConfig(t)
	cfg.BucketStore.BucketStoreType = string(cortex_tsdb.ParquetBucketStore)
	cfg.BucketStore.MaxInflightRequests = 10
	reg := prometheus.NewPedanticRegistry()
	storageDir := t.TempDir()
	generateStorageBlock(t, storageDir, "user_id", "series_1", 0, 100, 15)
	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	stores, err := NewBucketStores(cfg, NewNoShardingStrategy(log.NewNopLogger(), nil), objstore.WithNoopInstr(bucket), defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, stores.InitialSync(context.Background()))

	hybridStores := stores.(*HybridBucketStores)
	// Set inflight requests to the limit
	for range 10 {
		hybridStores.inflightRequests.Inc()
	}
	series, warnings, err := querySeries(stores, "user_id", "series_1", 0, 100)
	assert.ErrorIs(t, err, ErrTooManyInflightRequests)
	assert.Empty(t, series)
	assert.Empty(t, warnings)
}

func TestHybridBucketStores_Series_ShouldNotCheckMaxInflightRequestsIfTheLimitIsDisabled(t *testing.T) {
	cfg := prepareStorageConfig(t)
	cfg.BucketStore.BucketStoreType = string(cortex_tsdb.ParquetBucketStore)
	reg := prometheus.NewPedanticRegistry()
	storageDir := t.TempDir()
	userId := "user_id"
	generateStorageBlock(t, storageDir, userId, "series_1", 0, 100, 15)
	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	stores, err := NewBucketStores(cfg, NewNoShardingStrategy(log.NewNopLogger(), nil), objstore.WithNoopInstr(bkt), defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, stores.InitialSync(context.Background()))

	hybridStores := stores.(*HybridBucketStores)
	// Set inflight requests to the limit (max_inflight_request is set to 0 by default = disabled)
	for range 10 {
		hybridStores.inflightRequests.Inc()
	}

	userPath := fmt.Sprintf("%s/%s", storageDir, userId)

	limits := validation.Limits{}
	overrides := validation.NewOverrides(limits, nil)
	uBucket := bucket.NewUserBucketClient(userId, bkt, overrides)
	blockIds, err := convertToParquetBlocksForTesting(userPath, uBucket)
	require.NoError(t, err)

	series, _, err := querySeries(stores, userId, "series_1", 0, 100, blockIds...)
	require.NoError(t, err)
	assert.Equal(t, 1, len(series))
}

// TestHybridBucketStores_SharesCaches verifies that, in Parquet mode, the Parquet
// store and its TSDB fallback share the caching bucket and the matchers cache.
func TestHybridBucketStores_SharesCaches(t *testing.T) {
	cfg := prepareStorageConfig(t)
	cfg.BucketStore.BucketStoreType = string(cortex_tsdb.ParquetBucketStore)
	// Enable the caches that both stores would otherwise register independently.
	cfg.BucketStore.ChunksCache.Backend = cortex_tsdb.CacheBackendInMemory
	cfg.BucketStore.MetadataCache.Backend = cortex_tsdb.CacheBackendInMemory
	cfg.BucketStore.MatchersCacheMaxItems = 100

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
	require.NoError(t, err)

	// A pedantic registry makes any duplicate metric registration fail.
	reg := prometheus.NewPedanticRegistry()
	stores, err := NewBucketStores(cfg, NewNoShardingStrategy(log.NewNopLogger(), nil), objstore.WithNoopInstr(bkt), defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NotNil(t, stores)
}

// setupHybridParquetTSDB creates one Parquet-converted block ([0,100) with metricParquet) and one
// plain TSDB block ([100,200) with metricTSDB) in Parquet store-gateway mode, writes a bucket index
// recording the Parquet conversion, then builds and initial-syncs the hybrid stores.
func setupHybridParquetTSDB(t *testing.T, userID string, metricParquet, metricTSDB string) (BucketStores, *prometheus.Registry, []string) {
	t.Helper()

	storageDir := t.TempDir()
	cfg := prepareStorageConfig(t)
	cfg.BucketStore.BucketStoreType = string(cortex_tsdb.ParquetBucketStore)
	cfg.BucketStore.BucketIndex.Enabled = true

	// Parquet block [0,100): create alone so only this block is converted.
	generateStorageBlock(t, storageDir, userID, metricParquet, 0, 100, 15)

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)
	overrides := validation.NewOverrides(validation.Limits{}, nil)
	uBucket := bucket.NewUserBucketClient(userID, bkt, overrides)
	userPath := filepath.Join(storageDir, userID)

	parquetBlockIDs, err := convertToParquetBlocksForTesting(userPath, uBucket)
	require.NoError(t, err)
	require.Len(t, parquetBlockIDs, 1)

	// TSDB block [100,200): not converted.
	generateStorageBlock(t, storageDir, userID, metricTSDB, 100, 200, 15)

	entries, err := os.ReadDir(userPath)
	require.NoError(t, err)
	var tsdbBlockIDs []string
	for _, e := range entries {
		if _, parseErr := ulidv2.Parse(e.Name()); parseErr == nil && e.Name() != parquetBlockIDs[0] {
			tsdbBlockIDs = append(tsdbBlockIDs, e.Name())
		}
	}
	require.Len(t, tsdbBlockIDs, 1)
	allBlockIDs := append(parquetBlockIDs, tsdbBlockIDs...)

	writeParquetBucketIndex(t, bkt, uBucket, userID, parquetBlockIDs[0])

	reg := prometheus.NewRegistry()
	stores, err := NewBucketStores(cfg, NewNoShardingStrategy(log.NewNopLogger(), nil), objstore.WithNoopInstr(bkt), defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, stores.InitialSync(context.Background()))

	return stores, reg, allBlockIDs
}

// writeParquetBucketIndex writes a bucket index recording parquetBlockID as converted, so the
// TSDB sub-store drops it via IgnoreParquetBlocksFilter and hybrid routing serves it from Parquet.
func writeParquetBucketIndex(t *testing.T, bkt objstore.Bucket, uBucket objstore.Bucket, userID, parquetBlockID string) {
	t.Helper()

	parquetUID, err := ulidv2.Parse(parquetBlockID)
	require.NoError(t, err)
	require.NoError(t, uBucket.Upload(context.Background(), bucketindex.ConverterMarkFilePath(parquetUID), bytes.NewReader([]byte("{}"))))
	idx, _, _, err := bucketindex.NewUpdater(bkt, userID, nil, log.NewNopLogger()).EnableParquet().UpdateIndex(context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, bucketindex.WriteIndex(context.Background(), bkt, userID, nil, idx))
}

// TestHybridBucketStores_Series verifies that with one Parquet-converted block and one plain TSDB
// block, both are served correctly in Parquet store-gateway mode, and that the same series
// spanning both stores is merged into one.
func TestHybridBucketStores_Series(t *testing.T) {
	const userID = "user-1"

	assertBothServed := func(t *testing.T, stores BucketStores, allBlockIDs []string) {
		t.Helper()
		seriesFromParquet, _, err := querySeries(stores, userID, "series_parquet", 0, 100, allBlockIDs...)
		require.NoError(t, err)
		require.Len(t, seriesFromParquet, 1, "series from Parquet-converted block must be returned")

		seriesFromTSDB, _, err := querySeries(stores, userID, "series_tsdb", 100, 200, allBlockIDs...)
		require.NoError(t, err)
		require.Len(t, seriesFromTSDB, 1, "series from TSDB block must be returned")
	}

	t.Run("parquet and tsdb blocks both served", func(t *testing.T) {
		stores, _, allBlockIDs := setupHybridParquetTSDB(t, userID, "series_parquet", "series_tsdb")
		assertBothServed(t, stores, allBlockIDs)
	})

	t.Run("same series spanning both stores is merged into one", func(t *testing.T) {
		stores, _, allBlockIDs := setupHybridParquetTSDB(t, userID, "same_series", "same_series")

		// Same label set in both blocks: MergeSeriesSets must yield one series covering [0,200).
		series, _, err := querySeries(stores, userID, "same_series", 0, 200, allBlockIDs...)
		require.NoError(t, err)
		require.Len(t, series, 1, "same series across two blocks must be merged into one")

		require.NotEmpty(t, series[0].Chunks)
		minT, maxT := int64(math.MaxInt64), int64(math.MinInt64)
		for _, chk := range series[0].Chunks {
			if chk.MinTime < minT {
				minT = chk.MinTime
			}
			if chk.MaxTime > maxT {
				maxT = chk.MaxTime
			}
		}
		assert.Less(t, minT, int64(100), "chunks must cover the Parquet block [0, 100)")
		assert.GreaterOrEqual(t, maxT, int64(100), "chunks must cover the TSDB block [100, 200)")
	})
}

// TestHybridBucketStores_SyncedBlocksMetrics verifies that IgnoreParquetBlocksFilter excludes the
// converted block from the TSDB sub-store sync (loaded=1, parquet-converted=1).
func TestHybridBucketStores_SyncedBlocksMetrics(t *testing.T) {
	const userID = "user-1"

	_, reg, _ := setupHybridParquetTSDB(t, userID, "series_parquet", "series_tsdb")
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_blocks_meta_synced Reflects current state of synced blocks (over all tenants).
		# TYPE cortex_blocks_meta_synced gauge
		cortex_blocks_meta_synced{state="corrupted-bucket-index"} 0
		cortex_blocks_meta_synced{state="corrupted-meta-json"} 0
		cortex_blocks_meta_synced{state="duplicate"} 0
		cortex_blocks_meta_synced{state="failed"} 0
		cortex_blocks_meta_synced{state="label-excluded"} 0
		cortex_blocks_meta_synced{state="loaded"} 1
		cortex_blocks_meta_synced{state="marked-for-deletion"} 0
		cortex_blocks_meta_synced{state="marked-for-no-compact"} 0
		cortex_blocks_meta_synced{state="no-bucket-index"} 0
		cortex_blocks_meta_synced{state="no-meta-json"} 0
		cortex_blocks_meta_synced{state="parquet-converted"} 1
		cortex_blocks_meta_synced{state="parquet-migrated"} 0
		cortex_blocks_meta_synced{state="time-excluded"} 0
		cortex_blocks_meta_synced{state="too-fresh"} 0
	`), "cortex_blocks_meta_synced"))
}

// newHybridStoresForRouting returns hybrid stores whose TSDB sub-store has the given Parquet filters,
// which is all splitRequestedBlocks needs.
func newHybridStoresForRouting(filters map[string]*IgnoreParquetBlocksFilter) *HybridBucketStores {
	return &HybridBucketStores{
		tsdb:    &ThanosBucketStores{parquetFilters: filters},
		metrics: newHybridBucketStoresMetrics(prometheus.NewRegistry()),
	}
}

// TestHybridBucketStores_splitRequestedBlocks_UsesParquetBlockIDs verifies that routing is driven by
// the Parquet block IDs recorded by the TSDB store's Parquet filter, so a block is routed to the
// Parquet store iff it is converted in the bucket index.
func TestHybridBucketStores_splitRequestedBlocks_UsesParquetBlockIDs(t *testing.T) {
	const userID = "user-1"

	converted := ulidv2.MustNew(1, nil) // converted -> excluded from TSDB -> parquet
	kept := ulidv2.MustNew(2, nil)      // not converted -> stays in TSDB -> tsdb
	unknown := ulidv2.MustNew(3, nil)   // not in the index -> tsdb

	// Run the filter over a fresh index so it records the Parquet block IDs.
	filter := NewIgnoreParquetBlocksFilter(log.NewNopLogger())
	metas := map[ulidv2.ULID]*thanos_metadata.Meta{
		converted: {},
		kept:      {},
	}
	idx := &bucketindex.Index{
		Blocks: bucketindex.Blocks{
			{ID: converted, Parquet: &cortex_parquet.ConverterMarkMeta{Version: cortex_parquet.CurrentVersion}},
			{ID: kept},
		},
	}
	require.NoError(t, filter.FilterWithBucketIndex(context.Background(), metas, idx, extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{Name: "synced"}, []string{"state"})))

	h := newHybridStoresForRouting(map[string]*IgnoreParquetBlocksFilter{userID: filter})

	parquetIDs, tsdbIDs, err := h.splitRequestedBlocks(userID, blockIDsMatcher([]string{converted.String(), kept.String(), unknown.String()}), "Series")
	require.NoError(t, err)
	assert.Equal(t, []string{converted.String()}, parquetIDs, "converted block must be routed to parquet")
	assert.Equal(t, []string{kept.String(), unknown.String()}, tsdbIDs, "non-converted and unknown blocks must be routed to tsdb")
}

// TestHybridBucketStores_splitRequestedBlocks_UnknownParquetBlockIDsRoutesTSDB verifies that every
// block is routed to the TSDB store when the Parquet block IDs are not known yet (e.g. the user has
// not been synced yet).
func TestHybridBucketStores_splitRequestedBlocks_UnknownParquetBlockIDsRoutesTSDB(t *testing.T) {
	const userID = "user-1"

	b1 := ulidv2.MustNew(1, nil)
	b2 := ulidv2.MustNew(2, nil)

	for name, filters := range map[string]map[string]*IgnoreParquetBlocksFilter{
		"no filter for user":           {},
		"filter registered, never run": {userID: NewIgnoreParquetBlocksFilter(log.NewNopLogger())},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHybridStoresForRouting(filters)

			parquetIDs, tsdbIDs, err := h.splitRequestedBlocks(userID, blockIDsMatcher([]string{b1.String(), b2.String()}), "Series")
			require.NoError(t, err)
			assert.Empty(t, parquetIDs)
			assert.Equal(t, []string{b1.String(), b2.String()}, tsdbIDs, "blocks must be routed to TSDB when Parquet block IDs are unknown")
		})
	}
}

// TestNewBucketStores_ParquetFallbackDisabled verifies that disabling the Parquet fallback keeps
// the Parquet-only store, which runs without the TSDB sub-store and does not need the bucket index.
func TestNewBucketStores_ParquetFallbackDisabled(t *testing.T) {
	cfg := prepareStorageConfig(t)
	cfg.BucketStore.BucketStoreType = string(cortex_tsdb.ParquetBucketStore)
	cfg.BucketStore.ParquetFallbackDisabled = true
	cfg.BucketStore.BucketIndex.Enabled = false

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
	require.NoError(t, err)

	stores, err := NewBucketStores(cfg, NewNoShardingStrategy(log.NewNopLogger(), nil), objstore.WithNoopInstr(bkt), defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	assert.IsType(t, &ParquetBucketStores{}, stores)
}

func TestThanosBucketStores_parquetBlockIDs(t *testing.T) {
	const userID = "user-1"

	t.Run("no filter for user -> nil", func(t *testing.T) {
		u := &ThanosBucketStores{parquetFilters: map[string]*IgnoreParquetBlocksFilter{}}
		assert.Nil(t, u.parquetBlockIDs(userID))
	})

	t.Run("filter registered but never run -> nil", func(t *testing.T) {
		u := &ThanosBucketStores{parquetFilters: map[string]*IgnoreParquetBlocksFilter{
			userID: NewIgnoreParquetBlocksFilter(log.NewNopLogger()),
		}}
		assert.Nil(t, u.parquetBlockIDs(userID))
	})

	t.Run("filter run -> recorded set", func(t *testing.T) {
		parquetBlock := ulidv2.MustNew(1, nil)
		filter := NewIgnoreParquetBlocksFilter(log.NewNopLogger())
		require.NoError(t, filter.FilterWithBucketIndex(context.Background(),
			map[ulidv2.ULID]*thanos_metadata.Meta{parquetBlock: {}},
			&bucketindex.Index{Blocks: bucketindex.Blocks{
				{ID: parquetBlock, Parquet: &cortex_parquet.ConverterMarkMeta{Version: cortex_parquet.CurrentVersion}},
			}},
			extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{Name: "synced"}, []string{"state"}),
		))

		u := &ThanosBucketStores{parquetFilters: map[string]*IgnoreParquetBlocksFilter{userID: filter}}
		assert.Contains(t, u.parquetBlockIDs(userID), parquetBlock.String())
	})
}

func TestHybridBucketStores_splitRequestedBlocks_NoParquetBlocksRoutesTSDB(t *testing.T) {
	const userID = "user-1"

	b1 := ulidv2.MustNew(1, nil)
	b2 := ulidv2.MustNew(2, nil)

	filter := NewIgnoreParquetBlocksFilter(log.NewNopLogger())
	require.NoError(t, filter.FilterWithBucketIndex(context.Background(),
		map[ulidv2.ULID]*thanos_metadata.Meta{b1: {}, b2: {}},
		&bucketindex.Index{Blocks: bucketindex.Blocks{{ID: b1}, {ID: b2}}}, // no Parquet blocks
		extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{Name: "synced"}, []string{"state"}),
	))

	h := newHybridStoresForRouting(map[string]*IgnoreParquetBlocksFilter{userID: filter})

	parquetIDs, tsdbIDs, err := h.splitRequestedBlocks(userID, blockIDsMatcher([]string{b1.String(), b2.String()}), "Series")
	require.NoError(t, err)
	assert.Empty(t, parquetIDs)
	assert.Equal(t, []string{b1.String(), b2.String()}, tsdbIDs, "blocks must be routed to TSDB when no block is converted")
}

// TestHybridBucketStores_Series_MergedSortedByLabels verifies that when distinct series come
// from both the Parquet store and the TSDB store in the same query, the hybrid layer returns
// them as a single label-sorted stream.
func TestHybridBucketStores_Series_MergedSortedByLabels(t *testing.T) {
	const (
		userID     = "user-1"
		metricName = "merge_metric"
	)

	storageDir := t.TempDir()
	cfg := prepareStorageConfig(t)
	cfg.BucketStore.BucketStoreType = string(cortex_tsdb.ParquetBucketStore)
	cfg.BucketStore.BucketIndex.Enabled = true

	// Parquet block [0,100): series "a" and "c".
	generateStorageBlockWithSeriesValues(t, storageDir, userID, metricName, []string{"a", "c"}, 0, 100, 15)

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)
	overrides := validation.NewOverrides(validation.Limits{}, nil)
	uBucket := bucket.NewUserBucketClient(userID, bkt, overrides)
	userPath := filepath.Join(storageDir, userID)

	parquetBlockIDs, err := convertToParquetBlocksForTesting(userPath, uBucket)
	require.NoError(t, err)
	require.Len(t, parquetBlockIDs, 1)

	// TSDB block [0,100): series "b" and "d", which interleave with the Parquet series once sorted.
	generateStorageBlockWithSeriesValues(t, storageDir, userID, metricName, []string{"b", "d"}, 0, 100, 15)

	entries, err := os.ReadDir(userPath)
	require.NoError(t, err)
	var tsdbBlockIDs []string
	for _, e := range entries {
		if _, parseErr := ulidv2.Parse(e.Name()); parseErr == nil && e.Name() != parquetBlockIDs[0] {
			tsdbBlockIDs = append(tsdbBlockIDs, e.Name())
		}
	}
	require.Len(t, tsdbBlockIDs, 1)
	allBlockIDs := append(parquetBlockIDs, tsdbBlockIDs...)

	// The bucket index routes the Parquet block to the Parquet store and the TSDB block to the TSDB store.
	writeParquetBucketIndex(t, bkt, uBucket, userID, parquetBlockIDs[0])

	reg := prometheus.NewRegistry()
	stores, err := NewBucketStores(cfg, NewNoShardingStrategy(log.NewNopLogger(), nil), objstore.WithNoopInstr(bkt), defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, stores.InitialSync(context.Background()))

	// Exercise both the unbatched path (batchSize 0/1 => individual Series responses) and the
	// batched path (batchSize >= 2 => Batch responses).
	for _, batchSize := range []int64{0, 1, 2, 3} {
		t.Run(fmt.Sprintf("batchSize=%d", batchSize), func(t *testing.T) {
			series, _, err := querySeriesWithBatchSize(stores, userID, metricName, 0, 100, batchSize, allBlockIDs...)
			require.NoError(t, err)

			// Union across both stores: a (parquet), b (tsdb), c (parquet), d (tsdb).
			require.Len(t, series, 4, "hybrid must return the union of series from both stores")

			got := make([]string, 0, len(series))
			for i, s := range series {
				lset := s.PromLabels()
				got = append(got, lset.Get("series"))
				if i > 0 {
					require.Negative(t, labels.Compare(series[i-1].PromLabels(), lset),
						"merged series must be strictly increasing by labels")
				}
			}
			assert.Equal(t, []string{"a", "b", "c", "d"}, got, "series must be label-sorted and interleave both stores")
		})
	}
}

// TestHybridBucketStores_LabelNamesAndValues_Merged verifies that when a query spans both a
// Parquet-converted block and a plain TSDB block, the hybrid layer merges label names and label
// values from both sub-stores (sorted and de-duplicated).
func TestHybridBucketStores_LabelNamesAndValues_Merged(t *testing.T) {
	const (
		userID     = "user-1"
		metricName = "merge_metric"
	)

	storageDir := t.TempDir()
	cfg := prepareStorageConfig(t)
	cfg.BucketStore.BucketStoreType = string(cortex_tsdb.ParquetBucketStore)
	cfg.BucketStore.BucketIndex.Enabled = true

	// Parquet block: a Parquet-only label name "pk" and series values a, c.
	generateStorageBlockWithLabelSets(t, storageDir, userID, []labels.Labels{
		labels.FromStrings(labels.MetricName, metricName, "series", "a", "pk", "1"),
		labels.FromStrings(labels.MetricName, metricName, "series", "c", "pk", "1"),
	}, 0, 100, 15)

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)
	overrides := validation.NewOverrides(validation.Limits{}, nil)
	uBucket := bucket.NewUserBucketClient(userID, bkt, overrides)
	userPath := filepath.Join(storageDir, userID)

	parquetBlockIDs, err := convertToParquetBlocksForTesting(userPath, uBucket)
	require.NoError(t, err)
	require.Len(t, parquetBlockIDs, 1)

	// TSDB block: a TSDB-only label name "tk" and series values b, c, d. The value "c" is shared
	// with the Parquet block to exercise de-duplication in the merged LabelValues response.
	generateStorageBlockWithLabelSets(t, storageDir, userID, []labels.Labels{
		labels.FromStrings(labels.MetricName, metricName, "series", "b", "tk", "1"),
		labels.FromStrings(labels.MetricName, metricName, "series", "c", "tk", "1"),
		labels.FromStrings(labels.MetricName, metricName, "series", "d", "tk", "1"),
	}, 0, 100, 15)

	entries, err := os.ReadDir(userPath)
	require.NoError(t, err)
	var tsdbBlockIDs []string
	for _, e := range entries {
		if _, parseErr := ulidv2.Parse(e.Name()); parseErr == nil && e.Name() != parquetBlockIDs[0] {
			tsdbBlockIDs = append(tsdbBlockIDs, e.Name())
		}
	}
	require.Len(t, tsdbBlockIDs, 1)
	allBlockIDs := append(parquetBlockIDs, tsdbBlockIDs...)

	// The bucket index routes the Parquet block to the Parquet store and the TSDB block to the TSDB store.
	writeParquetBucketIndex(t, bkt, uBucket, userID, parquetBlockIDs[0])

	reg := prometheus.NewRegistry()
	stores, err := NewBucketStores(cfg, NewNoShardingStrategy(log.NewNopLogger(), nil), objstore.WithNoopInstr(bkt), defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, stores.InitialSync(context.Background()))

	t.Run("LabelNames merges names from both stores", func(t *testing.T) {
		resp, err := queryLabelsNamesWithBlocks(stores, userID, metricName, 0, 100, allBlockIDs...)
		require.NoError(t, err)
		// __name__ and series from both, pk only from Parquet, tk only from TSDB.
		assert.Equal(t, []string{labels.MetricName, "pk", "series", "tk"}, resp.Names)
	})

	t.Run("LabelValues merges and de-duplicates values from both stores", func(t *testing.T) {
		resp, err := queryLabelsValuesWithBlocks(stores, userID, "series", metricName, 0, 100, allBlockIDs...)
		require.NoError(t, err)
		// a,c (Parquet) + b,c,d (TSDB) -> deduplicated, sorted union.
		assert.Equal(t, []string{"a", "b", "c", "d"}, resp.Values)
	})
}

// TestHybridBucketStores_Series_StaleIndexRoutingGap guards against a data-gap that can happen
// right after a block is converted to Parquet: the TSDB sub-store reads the bucket index fresh on
// every sync and drops the converted block via IgnoreParquetBlocksFilter. The hybrid layer must
// still serve that block.
func TestHybridBucketStores_Series_StaleIndexRoutingGap(t *testing.T) {
	const userID = "user-1"

	storageDir := t.TempDir()
	cfg := prepareStorageConfig(t)
	cfg.BucketStore.BucketStoreType = string(cortex_tsdb.ParquetBucketStore)
	cfg.BucketStore.BucketIndex.Enabled = true
	// A long sync interval keeps background refreshes from interfering with the manual re-sync below.
	cfg.BucketStore.SyncInterval = time.Hour

	// Block A [0,100): metric_a. Will be converted to Parquet.
	generateStorageBlock(t, storageDir, userID, "metric_a", 0, 100, 15)

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)
	overrides := validation.NewOverrides(validation.Limits{}, nil)
	uBucket := bucket.NewUserBucketClient(userID, bkt, overrides)
	userPath := filepath.Join(storageDir, userID)

	parquetBlockIDs, err := convertToParquetBlocksForTesting(userPath, uBucket)
	require.NoError(t, err)
	require.Len(t, parquetBlockIDs, 1)
	parquetUID, err := ulidv2.Parse(parquetBlockIDs[0])
	require.NoError(t, err)

	// v1 = the "before conversion" bucket index snapshot (A recorded as a plain TSDB block).
	idxV1, _, _, err := bucketindex.NewUpdater(bkt, userID, nil, log.NewNopLogger()).UpdateIndex(context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, bucketindex.WriteIndex(context.Background(), bkt, userID, nil, idxV1))

	reg := prometheus.NewRegistry()
	stores, err := NewBucketStores(cfg, NewNoShardingStrategy(log.NewNopLogger(), nil), objstore.WithNoopInstr(bkt), defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, stores.InitialSync(context.Background()))

	hStores := stores.(*HybridBucketStores)

	// v2 = the "after conversion" bucket index snapshot (A recorded as Parquet). It overwrites
	// the index file in storage, so the TSDB sub-store drops A on the next sync.
	require.NoError(t, uBucket.Upload(context.Background(), bucketindex.ConverterMarkFilePath(parquetUID), bytes.NewReader([]byte("{}"))))
	idxV2, _, _, err := bucketindex.NewUpdater(bkt, userID, nil, log.NewNopLogger()).EnableParquet().UpdateIndex(context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, bucketindex.WriteIndex(context.Background(), bkt, userID, nil, idxV2))

	// Re-sync the TSDB sub-store: it reads the fresh index, excludes block A, and records A as a
	// Parquet block so hybrid routing sends A to the Parquet store.
	require.NoError(t, hStores.tsdb.SyncBlocks(context.Background()))

	// Block A was excluded from the TSDB store; its recorded Parquet block ID makes hybrid route it
	// to the Parquet store instead. Block A's series must still be returned — otherwise data has silently vanished
	// during the TSDB -> Parquet handover.
	series, _, err := querySeries(stores, userID, "metric_a", 0, 100, parquetBlockIDs...)
	require.NoError(t, err)
	require.Len(t, series, 1, "series from the just-converted block must not silently disappear after the TSDB store drops it")
}

func queryLabelsNamesWithBlocks(stores BucketStores, userID, metricName string, start, end int64, blockIDs ...string) (*storepb.LabelNamesResponse, error) {
	req := &storepb.LabelNamesRequest{
		Start: start,
		End:   end,
		Matchers: []storepb.LabelMatcher{{
			Type:  storepb.LabelMatcher_EQ,
			Name:  labels.MetricName,
			Value: metricName,
		}},
		PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
	}
	if len(blockIDs) > 0 {
		hints := &hintspb.LabelNamesRequestHints{
			BlockMatchers: []storepb.LabelMatcher{{
				Type:  storepb.LabelMatcher_RE,
				Name:  block.BlockIDLabel,
				Value: strings.Join(blockIDs, "|"),
			}},
		}
		anyHints, err := types.MarshalAny(hints)
		if err != nil {
			return nil, err
		}
		req.Hints = anyHints
	}

	ctx := setUserIDToGRPCContext(context.Background(), userID)
	return stores.LabelNames(ctx, req)
}

func queryLabelsValuesWithBlocks(stores BucketStores, userID, labelName, metricName string, start, end int64, blockIDs ...string) (*storepb.LabelValuesResponse, error) {
	req := &storepb.LabelValuesRequest{
		Start: start,
		End:   end,
		Label: labelName,
		Matchers: []storepb.LabelMatcher{{
			Type:  storepb.LabelMatcher_EQ,
			Name:  labels.MetricName,
			Value: metricName,
		}},
		PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
	}
	if len(blockIDs) > 0 {
		hints := &hintspb.LabelValuesRequestHints{
			BlockMatchers: []storepb.LabelMatcher{{
				Type:  storepb.LabelMatcher_RE,
				Name:  block.BlockIDLabel,
				Value: strings.Join(blockIDs, "|"),
			}},
		}
		anyHints, err := types.MarshalAny(hints)
		if err != nil {
			return nil, err
		}
		req.Hints = anyHints
	}

	ctx := setUserIDToGRPCContext(context.Background(), userID)
	return stores.LabelValues(ctx, req)
}

// generateStorageBlockWithSeriesValues creates a single TSDB block containing one series per
// provided "series" label value (all sharing the same metric name). It lets tests control the
// exact label ordering across blocks.
func generateStorageBlockWithSeriesValues(t testing.TB, storageDir, userID, metricName string, seriesValues []string, minT, maxT int64, step int) {
	t.Helper()
	userDir := filepath.Join(storageDir, userID)
	if _, err := os.Stat(userDir); os.IsNotExist(err) {
		require.NoError(t, os.Mkdir(userDir, os.ModePerm))
	}

	tmpDir := t.TempDir()
	db, err := tsdb.Open(tmpDir, promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	app := db.Appender(context.Background())
	for i, v := range seriesValues {
		lbls := labels.FromStrings(labels.MetricName, metricName, "series", v)
		for ts := minT; ts < maxT; ts += int64(step) {
			_, err = app.Append(0, lbls, ts, float64(i))
			require.NoError(t, err)
		}
	}
	require.NoError(t, app.Commit())
	require.NoError(t, db.Snapshot(userDir, true))
}

// generateStorageBlockWithLabelSets creates a single TSDB block containing exactly the provided
// series label sets. It lets tests control both the label names and values present in each block.
func generateStorageBlockWithLabelSets(t *testing.T, storageDir, userID string, seriesLabels []labels.Labels, minT, maxT int64, step int) {
	t.Helper()
	userDir := filepath.Join(storageDir, userID)
	if _, err := os.Stat(userDir); os.IsNotExist(err) {
		require.NoError(t, os.Mkdir(userDir, os.ModePerm))
	}

	tmpDir := t.TempDir()
	db, err := tsdb.Open(tmpDir, promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	app := db.Appender(context.Background())
	for i, lbls := range seriesLabels {
		for ts := minT; ts < maxT; ts += int64(step) {
			_, err = app.Append(0, lbls, ts, float64(i))
			require.NoError(t, err)
		}
	}
	require.NoError(t, app.Commit())
	require.NoError(t, db.Snapshot(userDir, true))
}

// setupHybridMixedStores creates one Parquet-converted block holding parquetSeries and one plain
// TSDB block holding tsdbSeries (both [0,100) for metricName), writes the bucket index and returns
// the initial-synced hybrid stores along with the storage dir, the Parquet block ID and all block IDs.
func setupHybridMixedStores(t *testing.T, cfg cortex_tsdb.BlocksStorageConfig, userID, metricName string, parquetSeries, tsdbSeries []string) (*HybridBucketStores, string, string, []string) {
	t.Helper()

	cfg.BucketStore.BucketStoreType = string(cortex_tsdb.ParquetBucketStore)
	cfg.BucketStore.BucketIndex.Enabled = true

	storageDir := t.TempDir()
	generateStorageBlockWithSeriesValues(t, storageDir, userID, metricName, parquetSeries, 0, 100, 15)

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)
	uBucket := bucket.NewUserBucketClient(userID, bkt, validation.NewOverrides(validation.Limits{}, nil))
	userPath := filepath.Join(storageDir, userID)

	parquetBlockIDs, err := convertToParquetBlocksForTesting(userPath, uBucket)
	require.NoError(t, err)
	require.Len(t, parquetBlockIDs, 1)

	generateStorageBlockWithSeriesValues(t, storageDir, userID, metricName, tsdbSeries, 0, 100, 15)

	entries, err := os.ReadDir(userPath)
	require.NoError(t, err)
	allBlockIDs := append([]string{}, parquetBlockIDs...)
	for _, e := range entries {
		if _, parseErr := ulidv2.Parse(e.Name()); parseErr == nil && e.Name() != parquetBlockIDs[0] {
			allBlockIDs = append(allBlockIDs, e.Name())
		}
	}
	require.Len(t, allBlockIDs, 2)

	writeParquetBucketIndex(t, bkt, uBucket, userID, parquetBlockIDs[0])

	stores, err := NewBucketStores(cfg, NewNoShardingStrategy(log.NewNopLogger(), nil), objstore.WithNoopInstr(bkt), defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, stores.InitialSync(context.Background()))

	return stores.(*HybridBucketStores), storageDir, parquetBlockIDs[0], allBlockIDs
}

func seriesValues(prefix string, n int) []string {
	values := make([]string, 0, n)
	for i := range n {
		values = append(values, fmt.Sprintf("%s%05d", prefix, i))
	}
	return values
}

func newMixedSeriesRequest(t *testing.T, userID, metricName string, enableQueryStats bool, blockIDs []string) (*storepb.SeriesRequest, context.Context) {
	t.Helper()

	anyHints, err := types.MarshalAny(&hintspb.SeriesRequestHints{
		BlockMatchers:    blockIDsMatcher(blockIDs),
		EnableQueryStats: enableQueryStats,
	})
	require.NoError(t, err)

	req := &storepb.SeriesRequest{
		MinTime: 0,
		MaxTime: 100,
		Matchers: []storepb.LabelMatcher{{
			Type:  storepb.LabelMatcher_EQ,
			Name:  labels.MetricName,
			Value: metricName,
		}},
		PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		Hints:                   anyHints,
	}
	ctx := user.InjectOrgID(setUserIDToGRPCContext(context.Background(), userID), userID)
	return req, ctx
}

// TestHybridBucketStores_Series_MixedPropagatesSubStoreError verifies that when one sub-store fails
// while serving a mixed query, the client receives that sub-store's error rather than the
// cancellation error of the other sub-store, so the querier can still classify it (retry, limit, ...).
func TestHybridBucketStores_Series_MixedPropagatesSubStoreError(t *testing.T) {
	const (
		userID     = "user-1"
		metricName = "mixed_metric"
	)

	t.Run("tsdb store fails", func(t *testing.T) {
		cfg := prepareStorageConfig(t)
		cfg.BucketStore.MaxInflightRequests = 1
		// Many Parquet series keep the Parquet producer running when the TSDB store fails, so it
		// gets canceled and would mask the TSDB error.
		h, _, _, allBlockIDs := setupHybridMixedStores(t, cfg, userID, metricName, seriesValues("p", 2000), seriesValues("t", 1))

		// Saturate only the TSDB sub-store so it fails with ErrTooManyInflightRequests.
		h.tsdb.inflightRequests.Inc()

		for range 5 {
			_, _, err := querySeries(h, userID, metricName, 0, 100, allBlockIDs...)
			require.ErrorIs(t, err, ErrTooManyInflightRequests)
			assert.Equal(t, codes.ResourceExhausted, status.Code(err))
		}
	})

	t.Run("parquet store fails", func(t *testing.T) {
		h, storageDir, parquetBlockID, allBlockIDs := setupHybridMixedStores(t, prepareStorageConfig(t), userID, metricName, seriesValues("p", 1), seriesValues("t", 2000))

		// Remove the Parquet files so the Parquet store fails to open the shard.
		parquetFiles, err := filepath.Glob(filepath.Join(storageDir, userID, parquetBlockID, "*.parquet"))
		require.NoError(t, err)
		require.NotEmpty(t, parquetFiles)
		for _, f := range parquetFiles {
			require.NoError(t, os.Remove(f))
		}

		_, _, err = querySeries(h, userID, metricName, 0, 100, allBlockIDs...)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to open parquet shard")
		assert.NotContains(t, err.Error(), context.Canceled.Error())
	})
}

// failingSeriesServer fails every Send once failAfter series have been sent.
type failingSeriesServer struct {
	*bucketStoreSeriesServer

	failAfter int
	sent      int
}

func (s *failingSeriesServer) Send(r *storepb.SeriesResponse) error {
	if r.GetSeries() != nil {
		s.sent++
	}
	if batch := r.GetBatch(); batch != nil {
		s.sent += len(batch.Series)
	}
	if s.sent > s.failAfter {
		return errors.New("client stream closed")
	}
	return s.bucketStoreSeriesServer.Send(r)
}

// TestHybridBucketStores_Series_MixedSendFailureStopsProducers verifies that when sending a merged
// series to the client fails, the hybrid store cancels both sub-stores, waits for them, and returns
// the send error.
func TestHybridBucketStores_Series_MixedSendFailureStopsProducers(t *testing.T) {
	const (
		userID     = "user-1"
		metricName = "mixed_metric"
	)

	// Enough series on both sides that both producers are blocked on the channel when the send fails.
	h, _, _, allBlockIDs := setupHybridMixedStores(t, prepareStorageConfig(t), userID, metricName, seriesValues("p", 1000), seriesValues("t", 1000))

	req, ctx := newMixedSeriesRequest(t, userID, metricName, false, allBlockIDs)
	srv := &failingSeriesServer{bucketStoreSeriesServer: newBucketStoreSeriesServer(ctx), failAfter: 10}

	errCh := make(chan error, 1)
	go func() { errCh <- h.Series(req, srv) }()

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "send merged series response")
		assert.Contains(t, err.Error(), "client stream closed")
		assert.NotContains(t, err.Error(), context.Canceled.Error())
	case <-time.After(30 * time.Second):
		t.Fatal("Series did not return after a send failure: sub-store producers are stuck")
	}
	assert.Len(t, srv.SeriesSet, 10)
}

// TestHybridBucketStores_Series_MixedMergesHints verifies that the hints sent for a mixed query
// cover the blocks queried by both sub-stores and carry the query stats.
func TestHybridBucketStores_Series_MixedMergesHints(t *testing.T) {
	const (
		userID     = "user-1"
		metricName = "mixed_metric"
	)

	h, _, _, allBlockIDs := setupHybridMixedStores(t, prepareStorageConfig(t), userID, metricName, []string{"a", "c"}, []string{"b", "d"})

	req, ctx := newMixedSeriesRequest(t, userID, metricName, true, allBlockIDs)
	srv := newBucketStoreSeriesServer(ctx)
	require.NoError(t, h.Series(req, srv))
	require.Len(t, srv.SeriesSet, 4)

	// The querier consistency check relies on every queried block being reported.
	queried := make([]string, 0, len(srv.Hints.QueriedBlocks))
	for _, b := range srv.Hints.QueriedBlocks {
		queried = append(queried, b.Id)
	}
	assert.ElementsMatch(t, allBlockIDs, queried)

	// Only the TSDB store reports query stats; they must survive the merge.
	require.NotNil(t, srv.Hints.QueryStats)
	assert.Equal(t, int64(1), srv.Hints.QueryStats.BlocksQueried)
}
