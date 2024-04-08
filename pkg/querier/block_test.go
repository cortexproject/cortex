package querier

import (
	"sort"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

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
