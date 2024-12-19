package distributor

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestLabelSetCounter(t *testing.T) {
	metricName := "cortex_distributor_received_samples_per_labelset_total"
	reg := prometheus.NewPedanticRegistry()
	dummyCounter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricName,
		Help: "",
	}, []string{"user", "type", "labelset"})
	counter := newLabelSetTracker(dummyCounter)
	reg.MustRegister(dummyCounter)

	userID := "1"
	userID2 := "2"
	userID3 := "3"

	counter.increaseSamplesLabelSet(userID, 0, labels.FromStrings("foo", "bar"), 10, 0)
	counter.increaseSamplesLabelSet(userID, 1, labels.FromStrings("foo", "baz"), 0, 5)
	counter.increaseSamplesLabelSet(userID, 3, labels.EmptyLabels(), 20, 20)
	counter.increaseSamplesLabelSet(userID2, 0, labels.FromStrings("foo", "bar"), 100, 5)
	counter.increaseSamplesLabelSet(userID2, 2, labels.FromStrings("cluster", "us-west-2"), 0, 100)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# TYPE cortex_distributor_received_samples_per_labelset_total counter
		cortex_distributor_received_samples_per_labelset_total{labelset="{cluster=\"us-west-2\"}",type="histogram",user="2"} 100
		cortex_distributor_received_samples_per_labelset_total{labelset="{foo=\"bar\"}",type="float",user="1"} 10
		cortex_distributor_received_samples_per_labelset_total{labelset="{foo=\"bar\"}",type="float",user="2"} 100
		cortex_distributor_received_samples_per_labelset_total{labelset="{foo=\"bar\"}",type="histogram",user="2"} 5
		cortex_distributor_received_samples_per_labelset_total{labelset="{foo=\"baz\"}",type="histogram",user="1"} 5
		cortex_distributor_received_samples_per_labelset_total{labelset="{}",type="float",user="1"} 20
		cortex_distributor_received_samples_per_labelset_total{labelset="{}",type="histogram",user="1"} 20
		`), metricName))

	// Increment metrics and add a new user.
	counter.increaseSamplesLabelSet(userID, 3, labels.EmptyLabels(), 20, 20)
	counter.increaseSamplesLabelSet(userID2, 2, labels.FromStrings("cluster", "us-west-2"), 0, 100)
	counter.increaseSamplesLabelSet(userID2, 4, labels.FromStrings("cluster", "us-west-2"), 10, 10)
	counter.increaseSamplesLabelSet(userID3, 4, labels.FromStrings("cluster", "us-east-1"), 30, 30)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# TYPE cortex_distributor_received_samples_per_labelset_total counter
		cortex_distributor_received_samples_per_labelset_total{labelset="{cluster=\"us-east-1\"}",type="float",user="3"} 30
		cortex_distributor_received_samples_per_labelset_total{labelset="{cluster=\"us-east-1\"}",type="histogram",user="3"} 30
		cortex_distributor_received_samples_per_labelset_total{labelset="{cluster=\"us-west-2\"}",type="float",user="2"} 10
		cortex_distributor_received_samples_per_labelset_total{labelset="{cluster=\"us-west-2\"}",type="histogram",user="2"} 210
		cortex_distributor_received_samples_per_labelset_total{labelset="{foo=\"bar\"}",type="float",user="1"} 10
		cortex_distributor_received_samples_per_labelset_total{labelset="{foo=\"bar\"}",type="float",user="2"} 100
		cortex_distributor_received_samples_per_labelset_total{labelset="{foo=\"bar\"}",type="histogram",user="2"} 5
		cortex_distributor_received_samples_per_labelset_total{labelset="{foo=\"baz\"}",type="histogram",user="1"} 5
		cortex_distributor_received_samples_per_labelset_total{labelset="{}",type="float",user="1"} 40
		cortex_distributor_received_samples_per_labelset_total{labelset="{}",type="histogram",user="1"} 40
		`), metricName))

	// Remove user 2. But metrics for user 2 not cleaned up as it is expected to be cleaned up
	// in cleanupInactiveUser loop. It is expected to have 3 minutes delay in this case.
	userSet := map[string]map[uint64]struct {
	}{
		userID:  {0: struct{}{}, 1: struct{}{}, 2: struct{}{}, 3: struct{}{}},
		userID3: {0: struct{}{}, 1: struct{}{}, 2: struct{}{}, 3: struct{}{}, 4: struct{}{}},
	}
	counter.updateMetrics(userSet)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# TYPE cortex_distributor_received_samples_per_labelset_total counter
		cortex_distributor_received_samples_per_labelset_total{labelset="{cluster=\"us-east-1\"}",type="float",user="3"} 30
		cortex_distributor_received_samples_per_labelset_total{labelset="{cluster=\"us-east-1\"}",type="histogram",user="3"} 30
		cortex_distributor_received_samples_per_labelset_total{labelset="{cluster=\"us-west-2\"}",type="float",user="2"} 10
		cortex_distributor_received_samples_per_labelset_total{labelset="{cluster=\"us-west-2\"}",type="histogram",user="2"} 210
		cortex_distributor_received_samples_per_labelset_total{labelset="{foo=\"bar\"}",type="float",user="1"} 10
		cortex_distributor_received_samples_per_labelset_total{labelset="{foo=\"bar\"}",type="float",user="2"} 100
		cortex_distributor_received_samples_per_labelset_total{labelset="{foo=\"bar\"}",type="histogram",user="2"} 5
		cortex_distributor_received_samples_per_labelset_total{labelset="{foo=\"baz\"}",type="histogram",user="1"} 5
		cortex_distributor_received_samples_per_labelset_total{labelset="{}",type="float",user="1"} 40
		cortex_distributor_received_samples_per_labelset_total{labelset="{}",type="histogram",user="1"} 40
		`), metricName))

	// Simulate existing limits removed for each user.
	userSet = map[string]map[uint64]struct {
	}{
		userID:  {0: struct{}{}},
		userID2: {},
		userID3: {},
	}
	counter.updateMetrics(userSet)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# TYPE cortex_distributor_received_samples_per_labelset_total counter
		cortex_distributor_received_samples_per_labelset_total{labelset="{cluster=\"us-west-2\"}",type="float",user="2"} 10
		cortex_distributor_received_samples_per_labelset_total{labelset="{cluster=\"us-west-2\"}",type="histogram",user="2"} 210
		cortex_distributor_received_samples_per_labelset_total{labelset="{foo=\"bar\"}",type="float",user="1"} 10
		cortex_distributor_received_samples_per_labelset_total{labelset="{foo=\"bar\"}",type="float",user="2"} 100
		cortex_distributor_received_samples_per_labelset_total{labelset="{foo=\"bar\"}",type="histogram",user="2"} 5
		`), metricName))
}
