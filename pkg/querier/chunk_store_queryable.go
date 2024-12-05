package querier

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/cortexproject/cortex/pkg/chunk"
)

type chunkIteratorFunc func(it chunkenc.Iterator, chunks []chunk.Chunk, from, through model.Time) chunkenc.Iterator
