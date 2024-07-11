package compactor

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

func NewShardedPosting(postings index.Postings, partitionCount uint64, partitionID uint64, labelsFn func(ref storage.SeriesRef, builder *labels.ScratchBuilder, chks *[]chunks.Meta) error) (index.Postings, map[string]struct{}, error) {
	bufChks := make([]chunks.Meta, 0)
	series := make([]storage.SeriesRef, 0)
	symbols := make(map[string]struct{})
	var builder labels.ScratchBuilder
	for postings.Next() {
		err := labelsFn(postings.At(), &builder, &bufChks)
		if err != nil {
			return nil, nil, err
		}
		if builder.Labels().Hash()%partitionCount == partitionID {
			posting := postings.At()
			series = append(series, posting)
			for _, label := range builder.Labels() {
				symbols[label.Name] = struct{}{}
				symbols[label.Value] = struct{}{}
			}
		}
	}
	return index.NewListPostings(series), symbols, nil
}
