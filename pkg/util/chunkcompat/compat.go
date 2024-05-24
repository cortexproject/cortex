package chunkcompat

import (
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/cortexproject/cortex/pkg/chunk"
	prom_chunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
)

// SeriesChunksToMatrix converts slice of []client.TimeSeriesChunk to a model.Matrix.
func SeriesChunksToMatrix(from, through model.Time, serieses []client.TimeSeriesChunk) (model.Matrix, error) {
	if serieses == nil {
		return nil, nil
	}

	result := model.Matrix{}
	for _, series := range serieses {
		metric := cortexpb.FromLabelAdaptersToMetric(series.Labels)
		chunks, err := FromChunks(cortexpb.FromLabelAdaptersToLabels(series.Labels), series.Chunks)
		if err != nil {
			return nil, err
		}

		its := make([]chunkenc.Iterable, 0, len(chunks))
		samples := []model.SamplePair{}
		for _, chk := range chunks {
			its = append(its, chk.Data)
		}
		it := storage.ChainSampleIteratorFromIterables(nil, its)
		for it.Next() != chunkenc.ValNone {
			t, v := it.At()
			if model.Time(t) < from {
				continue
			}
			if model.Time(t) > through {
				break
			}
			samples = append(samples, model.SamplePair{Timestamp: model.Time(t), Value: model.SampleValue(v)})
		}
		if err := it.Err(); err != nil {
			return nil, err
		}

		result = append(result, &model.SampleStream{
			Metric: metric,
			Values: samples,
		})
	}
	return result, nil
}

// FromChunks converts []client.Chunk to []chunk.Chunk.
func FromChunks(metric labels.Labels, in []client.Chunk) ([]chunk.Chunk, error) {
	out := make([]chunk.Chunk, 0, len(in))
	for _, i := range in {
		e := prom_chunk.Encoding(byte(i.Encoding))
		chunkEnc := e.PromChunkEncoding()
		if chunkEnc == chunkenc.EncNone {
			return nil, fmt.Errorf("unknown chunk encoding: %v", e)
		}

		chk, err := chunkenc.FromData(chunkEnc, i.Data)
		if err != nil {
			return nil, err
		}

		firstTime, lastTime := model.Time(i.StartTimestampMs), model.Time(i.EndTimestampMs)
		// As the lifetime of this chunk is scopes to this request, we don't need
		// to supply a fingerprint.
		out = append(out, chunk.NewChunk(metric, chk, firstTime, lastTime))
	}
	return out, nil
}

// ToChunks converts []chunk.Chunk to []client.Chunk.
func ToChunks(in []chunk.Chunk) ([]client.Chunk, error) {
	out := make([]client.Chunk, 0, len(in))
	for _, i := range in {
		e, err := prom_chunk.FromPromChunkEncoding(i.Data.Encoding())
		if err != nil {
			return nil, err
		}
		wireChunk := client.Chunk{
			StartTimestampMs: int64(i.From),
			EndTimestampMs:   int64(i.Through),
			Encoding:         int32(e),
			Data:             i.Data.Bytes(),
		}

		out = append(out, wireChunk)
	}
	return out, nil
}
