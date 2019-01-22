package planner

import (
	"context"
	"testing"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/stretchr/testify/require"
)

type mockBatch struct {
	queries []mockQuery
}

type mockQuery struct {
	from  int
	to    int
	table string
	user  string
}

func (c *mockBatch) Add(table, user string, from, to int) {
	c.queries = append(c.queries, mockQuery{
		user:  user,
		table: table,
		from:  from,
		to:    to,
	})
}

func (c *mockBatch) Size(context.Context) (int, error) {
	return 0, nil
}

func (c *mockBatch) Stream(context.Context, chan []chunk.Chunk) error {
	return nil
}

func TestPlan(t *testing.T) {
	type plan struct {
		firstShard int
		lastShard  int
		batchSize  int
		tables     []string
		users      []string
	}
	type result struct {
		batch mockBatch
	}
	tests := []struct {
		name   string
		plan   Planner
		result result
	}{
		{
			name: "simple",
			plan: Planner{
				firstShard: 0,
				lastShard:  240,
				batchSize:  240,
				tables:     []string{"simple"},
				users:      []string{"*"},
			},
			result: result{
				batch: mockBatch{
					queries: []mockQuery{
						{
							from:  0,
							to:    240,
							user:  "*",
							table: "simple",
						},
					},
				},
			},
		},
		{
			name: "small_simple",
			plan: Planner{
				firstShard: 0,
				lastShard:  25,
				batchSize:  24,
				tables:     []string{"simple"},
				users:      []string{"*"},
			},
			result: result{
				batch: mockBatch{
					queries: []mockQuery{
						{
							from:  0,
							to:    24,
							user:  "*",
							table: "simple",
						},
						{
							from:  24,
							to:    25,
							user:  "*",
							table: "simple",
						},
					},
				},
			},
		},
		{
			name: "bigger_simple",
			plan: Planner{
				firstShard: 0,
				lastShard:  25,
				batchSize:  26,
				tables:     []string{"simple"},
				users:      []string{"*"},
			},
			result: result{
				batch: mockBatch{
					queries: []mockQuery{
						{
							from:  0,
							to:    25,
							user:  "*",
							table: "simple",
						},
					},
				},
			},
		},
		{
			name: "max_simple",
			plan: Planner{
				firstShard: 0,
				lastShard:  240,
				batchSize:  48,
				tables:     []string{"simple"},
				users:      []string{"*"},
			},
			result: result{
				batch: mockBatch{
					queries: []mockQuery{
						{
							from:  0,
							to:    48,
							user:  "*",
							table: "simple",
						},
						{
							from:  48,
							to:    96,
							user:  "*",
							table: "simple",
						},
						{
							from:  96,
							to:    144,
							user:  "*",
							table: "simple",
						},
						{
							from:  144,
							to:    192,
							user:  "*",
							table: "simple",
						},
						{
							from:  192,
							to:    240,
							user:  "*",
							table: "simple",
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testBatch := &mockBatch{queries: []mockQuery{}}
			tt.plan.Plan(testBatch)
			require.Equal(t, tt.result.batch, *testBatch)
		})
	}
}
