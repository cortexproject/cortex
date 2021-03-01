package builder

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extprom"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/util"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

func TestTsdbBuilder(t *testing.T) {
	dir, err := ioutil.TempDir("", "tsdb")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})

	yesterdayStart := time.Now().Add(-24 * time.Hour).Truncate(24 * time.Hour)
	yesterdayEnd := yesterdayStart.Add(24 * time.Hour)

	seriesCounter := prometheus.NewCounter(prometheus.CounterOpts{})
	samplesCounter := prometheus.NewCounter(prometheus.CounterOpts{})
	inMemory := prometheus.NewGauge(prometheus.GaugeOpts{})

	b, err := newTsdbBuilder(dir, yesterdayStart, yesterdayEnd, 33, 0, util_log.Logger, seriesCounter, samplesCounter, inMemory)
	require.NoError(t, err)

	seriesCount := 200
	totalSamples := atomic.NewInt64(0)
	concurrency := 15

	ch := make(chan int, seriesCount)
	for i := 0; i < seriesCount; i++ {
		ch <- i
	}
	close(ch)

	// Test that we can add series concurrently.
	wg := sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := range ch {
				lbls, samples := metricInfo(i)

				err := b.buildSingleSeries(lbls, generateSingleSeriesWithOverlappingChunks(t, lbls, yesterdayStart, yesterdayEnd, samples))
				require.NoError(t, err)

				totalSamples.Add(int64(samples))
			}
		}()
	}

	// Wait until all series are added to the builder.
	wg.Wait()

	id, err := b.finishBlock("unit test", map[string]string{"ext_label": "12345"})
	require.NoError(t, err)

	db, err := tsdb.Open(dir, util_log.Logger, prometheus.NewRegistry(), tsdb.DefaultOptions())
	require.NoError(t, err)

	blocks := db.Blocks()

	// Verify basic blocks details.
	{
		require.Equal(t, 1, len(blocks))
		require.Equal(t, id, blocks[0].Meta().ULID)
		require.Equal(t, id, blocks[0].Meta().Compaction.Sources[0])
		require.Equal(t, uint64(seriesCount), blocks[0].Meta().Stats.NumSeries)
		require.Equal(t, uint64(totalSamples.Load()), blocks[0].Meta().Stats.NumSamples)
	}

	// Make sure we can query expected number of samples back.
	{
		q, err := db.Querier(context.Background(), util.TimeToMillis(yesterdayStart), util.TimeToMillis(yesterdayEnd))
		require.NoError(t, err)
		res := q.Select(true, nil, labels.MustNewMatcher(labels.MatchNotEqual, labels.MetricName, "")) // Select all

		for i := 0; i < seriesCount; i++ {
			require.True(t, res.Next())
			s := res.At()

			lbls, samples := metricInfo(i)
			require.True(t, labels.Equal(lbls, s.Labels()))

			cnt := 0
			it := s.Iterator()
			for it.Next() {
				cnt++
			}

			require.NoError(t, it.Err())
			require.Equal(t, samples, cnt)
		}
		require.NoError(t, res.Err())
		require.False(t, res.Next())
		require.NoError(t, q.Close())
	}

	// Verify that chunks are stored in sorted order (based on series).
	{
		idx, err := blocks[0].Index()
		require.NoError(t, err)

		allPostings, err := idx.Postings(index.AllPostingsKey())
		require.NoError(t, err)

		lastChunkRef := uint64(0)
		// Postings must be sorted wrt. series. Here we check if chunks are sorted too.
		for allPostings.Next() {
			seriesID := allPostings.At()
			var lset labels.Labels
			var chks []chunks.Meta

			require.NoError(t, idx.Series(seriesID, &lset, &chks))

			for _, c := range chks {
				require.True(t, lastChunkRef < c.Ref, "lastChunkRef: %d, c.Ref: %d", lastChunkRef, c.Ref)
				lastChunkRef = c.Ref
			}
		}
		require.NoError(t, allPostings.Err())
		require.NoError(t, idx.Close())
	}

	require.NoError(t, db.Close())

	m, err := metadata.ReadFromDir(filepath.Join(dir, id.String()))
	require.NoError(t, err)

	otherID := ulid.MustNew(ulid.Now(), nil)

	// Make sure that deduplicate filter doesn't remove this block (thanks to correct sources).
	df := block.NewDeduplicateFilter()
	inp := map[ulid.ULID]*metadata.Meta{
		otherID: {
			BlockMeta: tsdb.BlockMeta{
				ULID:    otherID,
				MinTime: 0,
				MaxTime: 0,
				Compaction: tsdb.BlockMetaCompaction{
					Sources: []ulid.ULID{otherID},
				},
				Version: 0,
			},
		},

		id: m,
	}

	err = df.Filter(context.Background(), inp, extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"state"}))
	require.NoError(t, err)
	require.NotNil(t, inp[id])
}

func metricInfo(ix int) (labels.Labels, int) {
	return labels.Labels{{Name: labels.MetricName, Value: fmt.Sprintf("metric_%04d", ix)}}, (ix + 1) * 100
}

func generateSingleSeriesWithOverlappingChunks(t *testing.T, metric labels.Labels, start, end time.Time, samples int) []chunk.Chunk {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	tsStep := end.Sub(start).Milliseconds() / int64(samples)

	// We keep adding new chunks with samples until we reach required number.
	// To make sure we do that quickly, we align timestamps on "tsStep",
	// and also start with chunk at the beginning of time range.
	samplesMap := make(map[int64]float64, samples)

	var ecs []encoding.Chunk
	for len(samplesMap) < samples {
		var ts int64
		if len(samplesMap) < samples/10 {
			ts = util.TimeToMillis(start)
		} else {
			ts = util.TimeToMillis(start) + ((r.Int63n(end.Sub(start).Milliseconds()) / tsStep) * tsStep)
		}

		pc := encoding.New()
		for sc := r.Intn(samples/10) * 10; sc > 0 && len(samplesMap) < samples; sc-- {
			val, ok := samplesMap[ts]
			if !ok {
				val = r.Float64()
				samplesMap[ts] = val
			}

			overflow, err := pc.Add(model.SamplePair{
				Timestamp: model.Time(ts),
				Value:     model.SampleValue(val),
			})
			require.NoError(t, err)

			if overflow != nil {
				ecs = append(ecs, pc)
				pc = overflow
			}

			ts += tsStep
			if ts >= util.TimeToMillis(end) {
				break
			}
		}

		ecs = append(ecs, pc)
	}

	r.Shuffle(len(ecs), func(i, j int) {
		ecs[i], ecs[j] = ecs[j], ecs[i]
	})

	var cs []chunk.Chunk
	for _, ec := range ecs {
		c := chunk.NewChunk("test", 0, metric, ec, model.TimeFromUnixNano(start.UnixNano()), model.TimeFromUnixNano(end.UnixNano()))
		cs = append(cs, c)
	}

	return cs
}
