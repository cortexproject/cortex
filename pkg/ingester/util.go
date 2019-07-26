package ingester

import (
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/prometheus/common/model"
)

type sortableUint32 []uint32

func (ts sortableUint32) Len() int           { return len(ts) }
func (ts sortableUint32) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts sortableUint32) Less(i, j int) bool { return ts[i] < ts[j] }

func modelSamplePairsToClientSamples(samplePairs []model.SamplePair) []client.Sample {
	samples := make([]client.Sample, 0, len(samplePairs))

	for _, s := range samplePairs {
		samples = append(samples, client.Sample{
			Value:       float64(s.Value),
			TimestampMs: int64(s.Timestamp),
		})
	}

	return samples
}
