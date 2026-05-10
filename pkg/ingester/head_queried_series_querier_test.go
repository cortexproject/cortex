package ingester

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsHead(t *testing.T) {
	tests := []struct {
		name     string
		ulid     ulid.ULID
		expected bool
	}{
		{"rangeHead", rangeHeadULID, true},
		{"head", headULID, true},
		{"random block", ulid.MustNew(1, nil), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := &mockBlockReader{meta: tsdb.BlockMeta{ULID: tc.ulid}}
			assert.Equal(t, tc.expected, isHead(b))
		})
	}
}

func TestHeadQueriedSeriesSet_CollectsHashes(t *testing.T) {
	lbls1 := labels.FromStrings("__name__", "metric_a", "job", "foo")
	lbls2 := labels.FromStrings("__name__", "metric_a", "job", "bar")

	inner := &mockChunkSeriesSet{
		series: []storage.ChunkSeries{
			&mockChunkSeries{lbls: lbls1},
			&mockChunkSeries{lbls: lbls2},
		},
	}

	hll := NewActiveQueriedSeries(
		[]time.Duration{2 * time.Hour},
		15*time.Minute,
		1.0,
		nil,
	)

	// Use a real service to process the hashes.
	svc := NewActiveQueriedSeriesService(log.NewNopLogger(), nil)
	require.NoError(t, svc.StartAsync(context.Background()))
	defer svc.StopAsync()
	require.NoError(t, svc.AwaitRunning(context.Background()))

	wrapper := &headQueriedSeriesChunkQuerier{
		ChunkQuerier:               &mockChunkQuerierWithSeries{inner: inner},
		headQueriedSeries:          hll,
		activeQueriedSeriesService: svc,
		userID:                     "user-1",
		sampled:                    true,
	}

	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "metric_a"),
	}

	ss := wrapper.Select(context.Background(), false, nil, matchers...)
	count := 0
	for ss.Next() {
		count++
	}
	assert.Equal(t, 2, count)

	// Give the async worker time to process.
	time.Sleep(50 * time.Millisecond)

	estimate, err := hll.GetSeriesQueried(time.Now(), 2*time.Hour)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), estimate)
}

// Mock implementations

type mockBlockReader struct {
	meta tsdb.BlockMeta
}

func (m *mockBlockReader) Meta() tsdb.BlockMeta                   { return m.meta }
func (m *mockBlockReader) Index() (tsdb.IndexReader, error)       { return nil, nil }
func (m *mockBlockReader) Chunks() (tsdb.ChunkReader, error)      { return nil, nil }
func (m *mockBlockReader) Tombstones() (tombstones.Reader, error) { return nil, nil }
func (m *mockBlockReader) Size() int64                            { return 0 }
func (m *mockBlockReader) String() string                         { return "" }
func (m *mockBlockReader) MinTime() int64                         { return 0 }
func (m *mockBlockReader) MaxTime() int64                         { return 0 }

type mockChunkQuerierWithSeries struct {
	inner *mockChunkSeriesSet
}

func (q *mockChunkQuerierWithSeries) Select(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.ChunkSeriesSet {
	return q.inner
}
func (q *mockChunkQuerierWithSeries) LabelValues(_ context.Context, _ string, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (q *mockChunkQuerierWithSeries) LabelNames(_ context.Context, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (q *mockChunkQuerierWithSeries) Close() error { return nil }

type mockChunkSeriesSet struct {
	series []storage.ChunkSeries
	idx    int
}

func (m *mockChunkSeriesSet) Next() bool {
	if m.idx >= len(m.series) {
		return false
	}
	m.idx++
	return true
}
func (m *mockChunkSeriesSet) At() storage.ChunkSeries           { return m.series[m.idx-1] }
func (m *mockChunkSeriesSet) Err() error                        { return nil }
func (m *mockChunkSeriesSet) Warnings() annotations.Annotations { return nil }

type mockChunkSeries struct {
	lbls labels.Labels
}

func (m *mockChunkSeries) Labels() labels.Labels                      { return m.lbls }
func (m *mockChunkSeries) Iterator(_ chunks.Iterator) chunks.Iterator { return nil }
