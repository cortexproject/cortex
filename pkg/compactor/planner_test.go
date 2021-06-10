package compactor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

type PlannerMock struct {
	mock.Mock
}

func (p *PlannerMock) Plan(ctx context.Context, metasByMinTime []*metadata.Meta) ([]*metadata.Meta, error) {
	args := p.Called(ctx, metasByMinTime)
	return args.Get(0).([]*metadata.Meta), args.Error(1)
}

func TestPlanner_ShouldReturnParallelPlanner(t *testing.T) {
	var ranges []int64
	p, _ := NewPlanner(nil, ranges, "parallel")

	parallelPlanner := &ParallelPlanner{}

	assert.IsType(t, parallelPlanner, p.blocksPlanner)
}

func TestPlanner_ShouldReturnDefaultPlanner(t *testing.T) {
	var ranges []int64
	p, _ := NewPlanner(nil, ranges, "default")

	parallelPlanner := &ParallelPlanner{}

	assert.IsType(t, parallelPlanner, p.blocksPlanner)
}

func TestPlanner_ShouldErrorOnNonExistentPlanner(t *testing.T) {
	var ranges []int64
	_, err := NewPlanner(nil, ranges, "non-existent")

	assert.ErrorIs(t, err, errPlannerNotImplemented)
}

func TestPlanner_PlanShouldCallBlocksPlannerPlan(t *testing.T) {
	blockPlannerMock := &PlannerMock{}
	blockPlannerMock.On("Plan", mock.Anything, mock.Anything).Return([]*metadata.Meta{}, nil)

	p := &Planner{
		blocksPlanner: blockPlannerMock,
	}

	blockMetas := []*metadata.Meta{}
	p.Plan(context.Background(), blockMetas)

	blockPlannerMock.AssertCalled(t, "Plan", context.Background(), blockMetas)
}
