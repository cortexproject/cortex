package compactor

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

const (
	MetricLabelName = "__name__"
	MetricName      = "test_metric"
	TestLabelName   = "test_label"
	ConstLabelName  = "const_label"
	ConstLabelValue = "const_value"
)

func TestShardPostingAndSymbolBasedOnPartitionID(t *testing.T) {
	partitionCount := 8

	tmpdir, err := os.MkdirTemp("", "sharded_posting_test")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(tmpdir))
	})

	r := rand.New(rand.NewSource(0))
	var series []labels.Labels
	expectedSymbols := make(map[string]bool)
	metricName := labels.Label{Name: MetricLabelName, Value: MetricName}
	expectedSymbols[MetricLabelName] = false
	expectedSymbols[MetricName] = false
	expectedSymbols[ConstLabelName] = false
	expectedSymbols[ConstLabelValue] = false
	expectedSeriesCount := 10
	for range expectedSeriesCount {
		labelValue := strconv.Itoa(r.Int())
		series = append(series, labels.FromStrings(metricName.Name, metricName.Value, ConstLabelName, ConstLabelValue, TestLabelName, labelValue))
		expectedSymbols[TestLabelName] = false
		expectedSymbols[labelValue] = false
	}
	blockID, err := e2eutil.CreateBlock(context.Background(), tmpdir, series, 10, time.Now().Add(-10*time.Minute).UnixMilli(), time.Now().UnixMilli(), labels.EmptyLabels(), 0, metadata.NoneFunc, nil)
	require.NoError(t, err)

	var closers []io.Closer
	defer func() {
		for _, c := range closers {
			c.Close()
		}
	}()
	seriesCount := 0
	for partitionID := range partitionCount {
		ir, err := index.NewFileReader(filepath.Join(tmpdir, blockID.String(), "index"), index.DecodePostingsRaw)
		closers = append(closers, ir)
		require.NoError(t, err)
		k, v := index.AllPostingsKey()
		postings, err := ir.Postings(context.Background(), k, v)
		require.NoError(t, err)
		postings = ir.SortedPostings(postings)
		shardedPostings, syms, err := NewShardedPosting(context.Background(), postings, uint64(partitionCount), uint64(partitionID), ir.Series)
		require.NoError(t, err)
		bufChks := make([]chunks.Meta, 0)
		expectedShardedSymbols := make(map[string]struct{})
		for shardedPostings.Next() {
			var builder labels.ScratchBuilder
			err = ir.Series(shardedPostings.At(), &builder, &bufChks)
			require.NoError(t, err)
			require.Equal(t, uint64(partitionID), builder.Labels().Hash()%uint64(partitionCount))
			seriesCount++
			builder.Labels().Range(func(l labels.Label) {
				expectedShardedSymbols[l.Name] = struct{}{}
				expectedShardedSymbols[l.Value] = struct{}{}
			})
		}
		err = ir.Close()
		if err == nil {
			closers = closers[0 : len(closers)-1]
		}
		symbolsCount := 0
		for s := range syms {
			symbolsCount++
			_, ok := expectedSymbols[s]
			require.True(t, ok)
			expectedSymbols[s] = true
			_, ok = expectedShardedSymbols[s]
			require.True(t, ok)
		}
		require.Equal(t, len(expectedShardedSymbols), symbolsCount)
	}
	require.Equal(t, expectedSeriesCount, seriesCount)
	for _, visited := range expectedSymbols {
		require.True(t, visited)
	}
}

// seriesThenErrPostings yields refs and then fails, so that Err() only becomes
// non-nil once the iterator is exhausted. This is the shape of a postings
// iterator that breaks part way through a block.
type seriesThenErrPostings struct {
	refs []storage.SeriesRef
	idx  int
	err  error
}

func (p *seriesThenErrPostings) Next() bool {
	if p.idx >= len(p.refs) {
		return false
	}
	p.idx++
	return true
}

func (p *seriesThenErrPostings) Seek(storage.SeriesRef) bool { return false }

func (p *seriesThenErrPostings) At() storage.SeriesRef { return p.refs[p.idx-1] }

func (p *seriesThenErrPostings) Err() error {
	if p.idx >= len(p.refs) {
		return p.err
	}
	return nil
}

// labelsFnForRef builds a single label per series ref, enough for sharding.
func labelsFnForRef(ref storage.SeriesRef, builder *labels.ScratchBuilder, _ *[]chunks.Meta) error {
	builder.Reset()
	builder.Add(MetricLabelName, MetricName)
	builder.Add(TestLabelName, strconv.FormatUint(uint64(ref), 10))
	builder.Sort()
	return nil
}

func TestNewShardedPostingPropagatesPostingsError(t *testing.T) {
	expectedErr := errors.New("postings failed")

	t.Run("error before any series", func(t *testing.T) {
		shardedPostings, syms, err := NewShardedPosting(context.Background(), index.ErrPostings(expectedErr), 1, 0, labelsFnForRef)
		require.ErrorIs(t, err, expectedErr)
		require.Nil(t, shardedPostings)
		require.Nil(t, syms)
	})

	t.Run("error after some series", func(t *testing.T) {
		postings := &seriesThenErrPostings{
			refs: []storage.SeriesRef{1, 2, 3},
			err:  expectedErr,
		}
		shardedPostings, syms, err := NewShardedPosting(context.Background(), postings, 1, 0, labelsFnForRef)
		require.ErrorIs(t, err, expectedErr)
		require.Nil(t, shardedPostings)
		require.Nil(t, syms)
	})

	t.Run("no error keeps returning the sharded postings", func(t *testing.T) {
		postings := &seriesThenErrPostings{
			refs: []storage.SeriesRef{1, 2, 3},
		}
		shardedPostings, syms, err := NewShardedPosting(context.Background(), postings, 1, 0, labelsFnForRef)
		require.NoError(t, err)
		require.NotNil(t, syms)

		var got []storage.SeriesRef
		for shardedPostings.Next() {
			got = append(got, shardedPostings.At())
		}
		require.NoError(t, shardedPostings.Err())
		require.Equal(t, []storage.SeriesRef{1, 2, 3}, got)
	})
}

func TestNewShardedPostingReturnsContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Fewer series than util.CheckContextEveryNIterations, so the in-loop check
	// never runs and only the post-loop check can catch the cancellation.
	postings := &seriesThenErrPostings{refs: []storage.SeriesRef{1, 2, 3}}
	shardedPostings, syms, err := NewShardedPosting(ctx, postings, 1, 0, labelsFnForRef)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, shardedPostings)
	require.Nil(t, syms)
}
