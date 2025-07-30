package compactor

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/cortexproject/cortex/pkg/util"
)

func NewShardedPosting(ctx context.Context, postings index.Postings, partitionCount uint64, partitionID uint64, labelsFn func(ref storage.SeriesRef, builder *labels.ScratchBuilder, chks *[]chunks.Meta) error) (index.Postings, map[string]struct{}, error) {
	series := make([]storage.SeriesRef, 0)
	symbols := make(map[string]struct{})
	var builder labels.ScratchBuilder
	cnt := 0
	for postings.Next() {
		cnt++
		if cnt%util.CheckContextEveryNIterations == 0 && ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		err := labelsFn(postings.At(), &builder, nil)
		if err != nil {
			return nil, nil, err
		}
		if builder.Labels().Hash()%partitionCount == partitionID {
			posting := postings.At()
			series = append(series, posting)
			builder.Labels().Range(func(l labels.Label) {
				symbols[l.Name] = struct{}{}
				symbols[l.Value] = struct{}{}
			})
		}
	}
	return index.NewListPostings(series), symbols, nil
}
