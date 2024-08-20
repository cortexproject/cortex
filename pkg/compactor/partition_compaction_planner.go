package compactor

import (
	"context"

	"github.com/go-kit/log"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

type PartitionCompactionPlanner struct {
	ctx    context.Context
	bkt    objstore.InstrumentedBucket
	logger log.Logger
}

func NewPartitionCompactionPlanner(
	ctx context.Context,
	bkt objstore.InstrumentedBucket,
	logger log.Logger,
) *PartitionCompactionPlanner {
	return &PartitionCompactionPlanner{
		ctx:    ctx,
		bkt:    bkt,
		logger: logger,
	}
}

func (p *PartitionCompactionPlanner) Plan(ctx context.Context, metasByMinTime []*metadata.Meta, errChan chan error, extensions any) ([]*metadata.Meta, error) {
	panic("PartitionCompactionPlanner not implemented")
}
