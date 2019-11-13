package tsdb

import (
	"testing"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/stretchr/testify/assert"
)

func Test_FromLabelAdaptersToLabels(t *testing.T) {
	actual := FromLabelAdaptersToLabels([]client.LabelAdapter{
		{Name: "app", Value: "test"},
		{Name: "instance", Value: "i-1"},
	})

	expected := labels.Labels{
		{Name: "app", Value: "test"},
		{Name: "instance", Value: "i-1"},
	}

	assert.Equal(t, expected, actual)
}
