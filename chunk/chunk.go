// Copyright 2016 The Prometheus Authors

package chunk

import (
	"github.com/prometheus/common/model"
	prom_chunk "github.com/prometheus/prometheus/storage/local/chunk"

	"github.com/weaveworks/cortex/util"
)

// Chunk contains encoded timeseries data
type Chunk struct {
	ID      string       `json:"-"`
	From    model.Time   `json:"from"`
	Through model.Time   `json:"through"`
	Metric  model.Metric `json:"metric"`
	Data    []byte       `json:"-"`
}

// ByID allow you to sort chunks by ID
type ByID []Chunk

func (cs ByID) Len() int           { return len(cs) }
func (cs ByID) Swap(i, j int)      { cs[i], cs[j] = cs[j], cs[i] }
func (cs ByID) Less(i, j int) bool { return cs[i].ID < cs[j].ID }

// ChunksToMatrix converts a slice of chunks into a model.Matrix.
func ChunksToMatrix(chunks []Chunk) (model.Matrix, error) {
	// Group chunks by series, sort and dedupe samples.
	sampleStreams := map[model.Fingerprint]*model.SampleStream{}

	for _, c := range chunks {
		fp := c.Metric.Fingerprint()
		ss, ok := sampleStreams[fp]
		if !ok {
			ss = &model.SampleStream{
				Metric: c.Metric,
			}
			sampleStreams[fp] = ss
		}

		samples, err := decodeChunk(c.Data)
		if err != nil {
			return nil, err
		}

		ss.Values = util.MergeSamples(ss.Values, samples)
	}

	matrix := make(model.Matrix, 0, len(sampleStreams))
	for _, ss := range sampleStreams {
		matrix = append(matrix, ss)
	}

	return matrix, nil
}

func decodeChunk(buf []byte) ([]model.SamplePair, error) {
	lc, err := prom_chunk.NewForEncoding(prom_chunk.DoubleDelta)
	if err != nil {
		return nil, err
	}
	lc.UnmarshalFromBuf(buf)
	it := lc.NewIterator()
	// TODO(juliusv): Pre-allocate this with the right length again once we
	// add a method upstream to get the number of samples in a chunk.
	var samples []model.SamplePair
	for it.Scan() {
		samples = append(samples, it.Value())
	}
	return samples, nil
}
