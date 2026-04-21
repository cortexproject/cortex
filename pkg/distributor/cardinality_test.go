package distributor

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	ingester_client "github.com/cortexproject/cortex/pkg/ingester/client"
)

func TestTopNStats(t *testing.T) {
	items := map[string]uint64{
		"metric_a": 300,
		"metric_b": 600,
		"metric_c": 900,
	}

	// With RF=3, values should be divided by 3.
	result := topNStats(items, 3, 2)
	assert.Equal(t, 2, len(result))
	assert.Equal(t, "metric_c", result[0].Name)
	assert.Equal(t, uint64(300), result[0].Value) // 900/3
	assert.Equal(t, "metric_b", result[1].Name)
	assert.Equal(t, uint64(200), result[1].Value) // 600/3
}

func TestTopNStatsByMax(t *testing.T) {
	items := map[string]uint64{
		"label_a": 100,
		"label_b": 50,
		"label_c": 200,
	}

	result := topNStatsByMax(items, 2)
	assert.Equal(t, 2, len(result))
	assert.Equal(t, "label_c", result[0].Name)
	assert.Equal(t, uint64(200), result[0].Value)
	assert.Equal(t, "label_a", result[1].Name)
	assert.Equal(t, uint64(100), result[1].Value)
}

func TestAggregateStatItems(t *testing.T) {
	resps := []any{
		&ingester_client.CardinalityResponse{
			SeriesCountByMetricName: []*cortexpb.CardinalityStatItem{
				{Name: "metric_a", Value: 100},
				{Name: "metric_b", Value: 200},
			},
		},
		&ingester_client.CardinalityResponse{
			SeriesCountByMetricName: []*cortexpb.CardinalityStatItem{
				{Name: "metric_a", Value: 150},
				{Name: "metric_c", Value: 300},
			},
		},
	}

	result := aggregateStatItems(resps, func(r *ingester_client.CardinalityResponse) []*cortexpb.CardinalityStatItem {
		return r.SeriesCountByMetricName
	})

	assert.Equal(t, uint64(250), result["metric_a"]) // 100+150
	assert.Equal(t, uint64(200), result["metric_b"])
	assert.Equal(t, uint64(300), result["metric_c"])
}

func TestMaxStatItems(t *testing.T) {
	resps := []any{
		&ingester_client.CardinalityResponse{
			LabelValueCountByLabelName: []*cortexpb.CardinalityStatItem{
				{Name: "instance", Value: 50},
				{Name: "job", Value: 10},
			},
		},
		&ingester_client.CardinalityResponse{
			LabelValueCountByLabelName: []*cortexpb.CardinalityStatItem{
				{Name: "instance", Value: 30},
				{Name: "job", Value: 15},
			},
		},
	}

	result := maxStatItems(resps, func(r *ingester_client.CardinalityResponse) []*cortexpb.CardinalityStatItem {
		return r.LabelValueCountByLabelName
	})

	assert.Equal(t, uint64(50), result["instance"]) // max(50, 30)
	assert.Equal(t, uint64(15), result["job"])      // max(10, 15)
}
