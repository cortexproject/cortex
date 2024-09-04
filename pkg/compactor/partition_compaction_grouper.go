package compactor

import (
	"context"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
)

type PartitionCompactionGrouper struct {
	ctx    context.Context
	logger log.Logger
	bkt    objstore.InstrumentedBucket
}

func NewPartitionCompactionGrouper(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.InstrumentedBucket,
) *PartitionCompactionGrouper {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	return &PartitionCompactionGrouper{
		ctx:    ctx,
		logger: logger,
		bkt:    bkt,
	}
}

// Groups function modified from https://github.com/cortexproject/cortex/pull/2616
func (g *PartitionCompactionGrouper) Groups(blocks map[ulid.ULID]*metadata.Meta) (res []*compact.Group, err error) {
	panic("PartitionCompactionGrouper not implemented")
}
