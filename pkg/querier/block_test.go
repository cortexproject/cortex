package querier

import (
	"math"
	"sort"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func createAggrChunkWithSineSamples(minTime, maxTime time.Time, step time.Duration) storepb.AggrChunk {
	var samples []promql.FPoint

	minT := minTime.Unix() * 1000
	maxT := maxTime.Unix() * 1000
	stepMillis := step.Milliseconds()

	for t := minT; t < maxT; t += stepMillis {
		samples = append(samples, promql.FPoint{T: t, F: math.Sin(float64(t))})
	}

	return createAggrChunk(minT, maxT, samples...)
}

func createAggrChunkWithSamples(samples ...promql.FPoint) storepb.AggrChunk {
	return createAggrChunk(samples[0].T, samples[len(samples)-1].T, samples...)
}

func createAggrChunk(minTime, maxTime int64, samples ...promql.FPoint) storepb.AggrChunk {
	// Ensure samples are sorted by timestamp.
	sort.Slice(samples, func(i, j int) bool {
		return samples[i].T < samples[j].T
	})

	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	if err != nil {
		panic(err)
	}

	for _, s := range samples {
		appender.Append(s.T, s.F)
	}

	return storepb.AggrChunk{
		MinTime: minTime,
		MaxTime: maxTime,
		Raw: &storepb.Chunk{
			Type: storepb.Chunk_XOR,
			Data: chunk.Bytes(),
		},
	}
}
