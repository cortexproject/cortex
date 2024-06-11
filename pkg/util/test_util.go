package util

import (
	"math/rand"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
	histogram_util "github.com/cortexproject/cortex/pkg/util/histogram"
)

func GenerateRandomStrings() []string {
	randomChar := "0123456789abcdef"
	randomStrings := make([]string, 0, 1000000)
	sb := strings.Builder{}
	for i := 0; i < 1000000; i++ {
		sb.Reset()
		sb.WriteString("pod://")
		for j := 0; j < 14; j++ {
			sb.WriteByte(randomChar[rand.Int()%len(randomChar)])
		}
		randomStrings = append(randomStrings, sb.String())
	}
	return randomStrings
}

func GenerateChunk(t require.TestingT, step time.Duration, from model.Time, points int, enc promchunk.Encoding) chunk.Chunk {
	metric := labels.Labels{
		{Name: model.MetricNameLabel, Value: "foo"},
	}
	pe := enc.PromChunkEncoding()
	pc, err := chunkenc.NewEmptyChunk(pe)
	require.NoError(t, err)
	appender, err := pc.Appender()
	require.NoError(t, err)
	ts := from

	switch pe {
	case chunkenc.EncXOR:
		for i := 0; i < points; i++ {
			appender.Append(int64(ts), float64(ts))
			ts = ts.Add(step)
		}
	case chunkenc.EncHistogram:
		histograms := histogram_util.GenerateTestHistograms(int(from), int(step/time.Millisecond), points, 5, 20)
		for i := 0; i < points; i++ {
			_, _, appender, err = appender.AppendHistogram(nil, int64(ts), histograms[i], true)
			require.NoError(t, err)
			ts = ts.Add(step)
		}
	case chunkenc.EncFloatHistogram:
		histograms := histogram_util.GenerateTestHistograms(int(from), int(step/time.Millisecond), points, 5, 20)
		for i := 0; i < points; i++ {
			_, _, appender, err = appender.AppendFloatHistogram(nil, int64(ts), histograms[i].ToFloat(nil), true)
			require.NoError(t, err)
			ts = ts.Add(step)
		}
	}

	ts = ts.Add(-step) // undo the add that we did just before exiting the loop
	return chunk.NewChunk(metric, pc, from, ts)
}
