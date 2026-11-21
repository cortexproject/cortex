//go:build requires_docker

package integration

import (
	"context"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/util/log"
	cortex_testutil "github.com/cortexproject/cortex/pkg/util/test"
)

func TestCompactorMergesBlocks(t *testing.T) {
	const (
		userID    = "user-1"
		level1    = time.Hour
		level2    = 2 * time.Hour
		numSeries = 3
	)

	// Blocks well in the past, aligned to the level 2 range so that two level 1
	// blocks fill it.
	mint := (time.Now().Add(-6*time.Hour).UnixMilli() / level2.Milliseconds()) * level2.Milliseconds()
	ms := func(d time.Duration) int64 { return mint + d.Milliseconds() }

	type testBlock struct {
		series     []string
		mint, maxt int64
	}

	tests := map[string]struct {
		blocks           []testBlock
		expectedMaxt     int64
		expectedVertical int
	}{
		"adjacent blocks are merged into the next range": {
			blocks: []testBlock{
				{series: []string{"series_1", "series_2"}, mint: ms(0), maxt: ms(level1)},
				{series: []string{"series_3"}, mint: ms(level1), maxt: ms(level2)},
			},
			expectedMaxt: ms(level2),
		},
		"overlapping blocks are merged vertically": {
			blocks: []testBlock{
				{series: []string{"series_1", "series_2"}, mint: ms(0), maxt: ms(level1)},
				{series: []string{"series_2", "series_3"}, mint: ms(0), maxt: ms(level1)},
			},
			expectedMaxt:     ms(level1),
			expectedVertical: 1,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			flags := mergeFlags(BlocksStorageFlags(), map[string]string{
				"-compactor.block-ranges":                    level1.String() + "," + level2.String(),
				"-compactor.compaction-interval":             "5s",
				"-compactor.cleanup-interval":                "1s",
				"-compactor.deletion-delay":                  "1s",
				"-blocks-storage.bucket-store.sync-interval": "1s",
				// The blocks are hours old, so the querier doesn't need ingesters.
				"-querier.query-ingesters-within": "1h",
			})

			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(consul, minio))

			storage, err := e2ecortex.NewS3ClientForMinio(minio, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, err)
			bkt := bucket.NewUserBucketClient(userID, storage.GetBucket(), nil)

			// Upload the level 1 blocks, as if they were shipped by ingesters.
			ctx := context.Background()
			rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
			dir := filepath.Join(s.SharedDir(), "blocks")
			sources := make([]ulid.ULID, 0, len(tc.blocks))
			for _, b := range tc.blocks {
				lbls := make([]labels.Labels, 0, len(b.series))
				for _, name := range b.series {
					lbls = append(lbls, labels.FromStrings("__name__", name, "job", "test"))
				}
				id, err := e2e.CreateBlock(ctx, rnd, dir, lbls, 60, b.mint, b.maxt, time.Minute.Milliseconds(), 10)
				require.NoError(t, err)
				require.NoError(t, block.Upload(ctx, log.Logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc))
				sources = append(sources, id)
			}

			compactor := e2ecortex.NewCompactor("compactor", consul.NetworkHTTPEndpoint(), flags, "")
			require.NoError(t, s.StartAndWaitReady(compactor))

			require.NoError(t, compactor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_compactor_group_compactions_total"}, e2e.WaitMissingMetrics))
			require.NoError(t, compactor.WaitSumMetricsWithOptions(e2e.Equals(float64(tc.expectedVertical)), []string{"cortex_compactor_group_vertical_compactions_total"}, e2e.SkipMissingMetrics))
			require.NoError(t, compactor.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"cortex_compactor_group_compactions_failures_total"}, e2e.SkipMissingMetrics))

			// The source blocks are marked for deletion and then removed by the
			// cleaner, leaving only the compacted block.
			var compacted ulid.ULID
			cortex_testutil.Poll(t, 30*time.Second, 1, func() any {
				ids := listBlocks(t, ctx, bkt)
				if len(ids) == 1 {
					compacted = ids[0]
				}
				return len(ids)
			})
			require.NotContains(t, sources, compacted)

			meta, err := block.DownloadMeta(ctx, log.Logger, bkt, compacted)
			require.NoError(t, err)
			require.Equal(t, 2, meta.Compaction.Level)
			require.ElementsMatch(t, sources, meta.Compaction.Sources)
			require.Equal(t, mint, meta.MinTime)
			require.Equal(t, tc.expectedMaxt, meta.MaxTime)
			require.Equal(t, uint64(numSeries), meta.Stats.NumSeries)

			// The compacted block can be queried through the store-gateway.
			storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			require.NoError(t, s.StartAndWaitReady(storeGateway, querier))
			require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(1), "cortex_bucket_store_blocks_loaded"))
			require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "store-gateway-client"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"),
			)))

			c, err := e2ecortex.NewClient("", querier.HTTPEndpoint(), "", "", userID)
			require.NoError(t, err)
			result, err := c.Query(`count(count_over_time({job="test"}[2h]))`, time.UnixMilli(tc.expectedMaxt))
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			vector := result.(model.Vector)
			require.Len(t, vector, 1)
			require.Equal(t, model.SampleValue(numSeries), vector[0].Value)
		})
	}
}

// listBlocks returns the IDs of the blocks in the tenant's bucket.
func listBlocks(t *testing.T, ctx context.Context, bkt objstore.Bucket) []ulid.ULID {
	t.Helper()
	var ids []ulid.ULID
	require.NoError(t, bkt.Iter(ctx, "", func(name string) error {
		if id, ok := block.IsBlockDir(name); ok {
			ids = append(ids, id)
		}
		return nil
	}))
	return ids
}
