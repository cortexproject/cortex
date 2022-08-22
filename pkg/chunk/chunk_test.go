package chunk

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/util"
)

func init() {
	encoding.DefaultEncoding = encoding.PrometheusXorChunk
}

var labelsForDummyChunks = labels.Labels{
	{Name: labels.MetricName, Value: "foo"},
	{Name: "bar", Value: "baz"},
	{Name: "toms", Value: "code"},
}

func dummyChunkFor(now model.Time, metric labels.Labels) Chunk {
	c, _ := encoding.NewForEncoding(encoding.DefaultEncoding)
	chunkStart := now.Add(-time.Hour)

	t := 15 * time.Second
	nc, err := c.Add(model.SamplePair{Timestamp: chunkStart.Add(t), Value: model.SampleValue(1)})
	if err != nil {
		panic(err)
	}
	if nc != nil {
		panic("returned chunk was not nil")
	}

	chunk := NewChunk(
		metric,
		c,
		chunkStart,
		now,
	)
	return chunk
}

func TestChunksToMatrix(t *testing.T) {
	// Create 2 chunks which have the same metric
	now := model.Now()
	chunk1 := dummyChunkFor(now, labelsForDummyChunks)
	chunk1Samples, err := chunk1.Samples(chunk1.From, chunk1.Through)
	require.NoError(t, err)
	chunk2 := dummyChunkFor(now, labelsForDummyChunks)
	chunk2Samples, err := chunk2.Samples(chunk2.From, chunk2.Through)
	require.NoError(t, err)

	ss1 := &model.SampleStream{
		Metric: util.LabelsToMetric(chunk1.Metric),
		Values: util.MergeSampleSets(chunk1Samples, chunk2Samples),
	}

	// Create another chunk with a different metric
	otherMetric := labels.Labels{
		{Name: model.MetricNameLabel, Value: "foo2"},
		{Name: "bar", Value: "baz"},
		{Name: "toms", Value: "code"},
	}
	chunk3 := dummyChunkFor(now, otherMetric)
	chunk3Samples, err := chunk3.Samples(chunk3.From, chunk3.Through)
	require.NoError(t, err)

	ss2 := &model.SampleStream{
		Metric: util.LabelsToMetric(chunk3.Metric),
		Values: chunk3Samples,
	}

	for _, c := range []struct {
		chunks         []Chunk
		expectedMatrix model.Matrix
	}{
		{
			chunks:         []Chunk{},
			expectedMatrix: model.Matrix{},
		}, {
			chunks: []Chunk{
				chunk1,
				chunk2,
				chunk3,
			},
			expectedMatrix: model.Matrix{
				ss1,
				ss2,
			},
		},
	} {
		matrix, err := ChunksToMatrix(context.Background(), c.chunks, chunk1.From, chunk3.Through)
		require.NoError(t, err)

		sort.Sort(matrix)
		sort.Sort(c.expectedMatrix)
		require.Equal(t, c.expectedMatrix, matrix)
	}
}
