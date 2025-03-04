package compactor

import (
	"context"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
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
	for i := 0; i < expectedSeriesCount; i++ {
		labelValue := strconv.Itoa(r.Int())
		series = append(series, labels.Labels{
			metricName,
			{Name: ConstLabelName, Value: ConstLabelValue},
			{Name: TestLabelName, Value: labelValue},
		})
		expectedSymbols[TestLabelName] = false
		expectedSymbols[labelValue] = false
	}
	blockID, err := e2eutil.CreateBlock(context.Background(), tmpdir, series, 10, time.Now().Add(-10*time.Minute).UnixMilli(), time.Now().UnixMilli(), nil, 0, metadata.NoneFunc)
	require.NoError(t, err)

	var closers []io.Closer
	defer func() {
		for _, c := range closers {
			c.Close()
		}
	}()
	seriesCount := 0
	for partitionID := 0; partitionID < partitionCount; partitionID++ {
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
			for _, label := range builder.Labels() {
				expectedShardedSymbols[label.Name] = struct{}{}
				expectedShardedSymbols[label.Value] = struct{}{}
			}
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
