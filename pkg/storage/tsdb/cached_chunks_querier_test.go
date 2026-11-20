package tsdb

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	prom_tsdb "github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

// postingsReturningCache returns fixed postings regardless of the caller's context,
// the same way an expanded postings cache entry computed by another caller does.
type postingsReturningCache struct {
	ExpandedPostingsCache
	refs []storage.SeriesRef
}

func (c *postingsReturningCache) PostingsForMatchers(context.Context, ulid.ULID, prom_tsdb.IndexReader, ...*labels.Matcher) (index.Postings, error) {
	return index.NewListPostings(c.refs), nil
}

// sortCountingIndexReader counts SortedPostings calls. Any other method panics.
type sortCountingIndexReader struct {
	prom_tsdb.IndexReader
	sorted atomic.Int64
}

func (r *sortCountingIndexReader) SortedPostings(p index.Postings) index.Postings {
	r.sorted.Inc()
	return p
}

func TestSelectChunkSeriesSet_ShouldNotSortPostingsWhenContextIsCanceled(t *testing.T) {
	cache := &postingsReturningCache{refs: []storage.SeriesRef{1, 2, 3}}
	ms := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test_metric")}

	t.Run("live context sorts postings", func(t *testing.T) {
		ir := &sortCountingIndexReader{}
		set := selectChunkSeriesSet(context.Background(), true, nil, ms, headULID, ir, nil, nil, 0, 1, cache)
		require.NotNil(t, set)
		require.Equal(t, int64(1), ir.sorted.Load())
	})

	t.Run("canceled context returns the cancellation without sorting", func(t *testing.T) {
		ir := &sortCountingIndexReader{}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		set := selectChunkSeriesSet(ctx, true, nil, ms, headULID, ir, nil, nil, 0, 1, cache)
		require.False(t, set.Next())
		require.ErrorIs(t, set.Err(), context.Canceled)
		require.Equal(t, int64(0), ir.sorted.Load())
	})
}
