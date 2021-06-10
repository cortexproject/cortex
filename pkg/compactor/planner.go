package compactor

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
)

var (
	errPlannerNotImplemented = errors.New("planner type not implemented")
)

type Planner struct {
	blocksPlanner compact.Planner
	logger        log.Logger
}

func NewPlanner(logger log.Logger, ranges []int64, plannerType string) (*Planner, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	var blocksPlanner compact.Planner
	switch plannerType {
	case "parallel":
		blocksPlanner = NewParallelPlanner(logger, ranges)
	case "default":
		blocksPlanner = compact.NewTSDBBasedPlanner(logger, ranges)
	default:
		return nil, errPlannerNotImplemented
	}

	p := &Planner{
		logger:        logger,
		blocksPlanner: blocksPlanner,
	}

	return p, nil
}

func (p *Planner) Plan(ctx context.Context, metasByMinTime []*metadata.Meta) ([]*metadata.Meta, error) {
	return p.blocksPlanner.Plan(ctx, metasByMinTime)
}

func (p *Planner) PrintPlan(ctx context.Context, metasByMinTime []*metadata.Meta) {
	toCompact, err := p.Plan(ctx, metasByMinTime)

	if err != nil {
		return
	}

	level.Info(p.logger).Log("msg", "Compaction plan generated: ", "plan", fmt.Sprintf("%v", toCompact))
}
