package chunkcompat

import (
	"github.com/prometheus/common/model"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	prom_chunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
	"github.com/weaveworks/cortex/pkg/util"
)

// StreamsToMatrix converts slice of QueryStreamResponse to a model.Matrix.
func StreamsToMatrix(from, through model.Time, response []*client.QueryStreamResponse) (model.Matrix, error) {
	if response == nil {
		return nil, nil
	}

	result := model.Matrix{}
	for _, stream := range response {
		chunks, err := FromChunks("", stream.Chunks)
		if err != nil {
			return nil, err
		}

		samples := []model.SamplePair{}
		for _, chunk := range chunks {
			ss, err := chunk.Samples(from, through)
			if err != nil {
				return nil, err
			}
			samples = util.MergeSampleSets(samples, ss)
		}

		result = append(result, &model.SampleStream{
			Metric: client.FromLabelPairs(stream.Labels),
			Values: samples,
		})
	}
	return result, nil
}

// FromChunks converts []client.Chunk to []chunk.Chunk.
func FromChunks(userID string, in []client.Chunk) ([]chunk.Chunk, error) {
	out := make([]chunk.Chunk, 0, len(in))
	for _, i := range in {
		o, err := prom_chunk.NewForEncoding(prom_chunk.Encoding(byte(i.Encoding)))
		if err != nil {
			return nil, err
		}

		if err := o.UnmarshalFromBuf(i.Data); err != nil {
			return nil, err
		}

		firstTime, lastTime := model.Time(i.StartTimestampMs), model.Time(i.EndTimestampMs)
		// As the lifetime of this chunk is scopes to this request, we don't need
		// to supply a fingerprint or metric.
		out = append(out, chunk.NewChunk(userID, 0, nil, o, firstTime, lastTime))
	}
	return out, nil
}
