package querier

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage"

	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/util"
)

func mergeChunks(chunks []chunk.Chunk, from, through model.Time) storage.SeriesIterator {
	samples := make([][]model.SamplePair, 0, len(chunks))
	for _, c := range chunks {
		ss, err := c.Samples(from, through)
		if err != nil {
			return errIterator{err}
		}

		samples = append(samples, ss)
	}

	merged := util.MergeNSampleSets(samples...)
	return newConcreteSeriesIterator(newConcreteSeries(nil, merged))
}
