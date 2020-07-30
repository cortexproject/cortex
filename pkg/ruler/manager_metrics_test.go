package ruler

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestManagerMetrics(t *testing.T) {
	mainReg := prometheus.NewPedanticRegistry()

	managerMetrics := NewManagerMetrics()
	mainReg.MustRegister(managerMetrics)
	managerMetrics.AddUserRegistry("user1", populateManager(1))
	managerMetrics.AddUserRegistry("user2", populateManager(10))
	managerMetrics.AddUserRegistry("user3", populateManager(100))

	//noinspection ALL
	err := testutil.GatherAndCompare(mainReg, bytes.NewBufferString(`
# HELP cortex_prometheus_rule_evaluation_duration_seconds The duration for a rule to execute.
# TYPE cortex_prometheus_rule_evaluation_duration_seconds summary
cortex_prometheus_rule_evaluation_duration_seconds{user="user1",quantile="0.5"} 1
cortex_prometheus_rule_evaluation_duration_seconds{user="user1",quantile="0.9"} 1
cortex_prometheus_rule_evaluation_duration_seconds{user="user1",quantile="0.99"} 1
cortex_prometheus_rule_evaluation_duration_seconds_sum{user="user1"} 1
cortex_prometheus_rule_evaluation_duration_seconds_count{user="user1"} 1
cortex_prometheus_rule_evaluation_duration_seconds{user="user2",quantile="0.5"} 10
cortex_prometheus_rule_evaluation_duration_seconds{user="user2",quantile="0.9"} 10
cortex_prometheus_rule_evaluation_duration_seconds{user="user2",quantile="0.99"} 10
cortex_prometheus_rule_evaluation_duration_seconds_sum{user="user2"} 10
cortex_prometheus_rule_evaluation_duration_seconds_count{user="user2"} 1
cortex_prometheus_rule_evaluation_duration_seconds{user="user3",quantile="0.5"} 100
cortex_prometheus_rule_evaluation_duration_seconds{user="user3",quantile="0.9"} 100
cortex_prometheus_rule_evaluation_duration_seconds{user="user3",quantile="0.99"} 100
cortex_prometheus_rule_evaluation_duration_seconds_sum{user="user3"} 100
cortex_prometheus_rule_evaluation_duration_seconds_count{user="user3"} 1
# HELP cortex_prometheus_rule_evaluation_failures_total The total number of rule evaluation failures.
# TYPE cortex_prometheus_rule_evaluation_failures_total counter
cortex_prometheus_rule_evaluation_failures_total{rule_group="group_one",user="user1"} 1
cortex_prometheus_rule_evaluation_failures_total{rule_group="group_one",user="user2"} 10
cortex_prometheus_rule_evaluation_failures_total{rule_group="group_one",user="user3"} 100
cortex_prometheus_rule_evaluation_failures_total{rule_group="group_two",user="user1"} 1
cortex_prometheus_rule_evaluation_failures_total{rule_group="group_two",user="user2"} 10
cortex_prometheus_rule_evaluation_failures_total{rule_group="group_two",user="user3"} 100
# HELP cortex_prometheus_rule_evaluations_total The total number of rule evaluations.
# TYPE cortex_prometheus_rule_evaluations_total counter
cortex_prometheus_rule_evaluations_total{rule_group="group_one",user="user1"} 1
cortex_prometheus_rule_evaluations_total{rule_group="group_one",user="user2"} 10
cortex_prometheus_rule_evaluations_total{rule_group="group_one",user="user3"} 100
cortex_prometheus_rule_evaluations_total{rule_group="group_two",user="user1"} 1
cortex_prometheus_rule_evaluations_total{rule_group="group_two",user="user2"} 10
cortex_prometheus_rule_evaluations_total{rule_group="group_two",user="user3"} 100
# HELP cortex_prometheus_rule_group_duration_seconds The duration of rule group evaluations.
# TYPE cortex_prometheus_rule_group_duration_seconds summary
cortex_prometheus_rule_group_duration_seconds_sum{user="user1"} 0
cortex_prometheus_rule_group_duration_seconds_count{user="user1"} 0
cortex_prometheus_rule_group_duration_seconds_sum{user="user2"} 0
cortex_prometheus_rule_group_duration_seconds_count{user="user2"} 0
cortex_prometheus_rule_group_duration_seconds_sum{user="user3"} 0
cortex_prometheus_rule_group_duration_seconds_count{user="user3"} 0
# HELP cortex_prometheus_rule_group_iterations_missed_total The total number of rule group evaluations missed due to slow rule group evaluation.
# TYPE cortex_prometheus_rule_group_iterations_missed_total counter
cortex_prometheus_rule_group_iterations_missed_total{user="user1"} 1
cortex_prometheus_rule_group_iterations_missed_total{user="user2"} 10
cortex_prometheus_rule_group_iterations_missed_total{user="user3"} 100
# HELP cortex_prometheus_rule_group_iterations_total The total number of scheduled rule group evaluations, whether executed or missed.
# TYPE cortex_prometheus_rule_group_iterations_total counter
cortex_prometheus_rule_group_iterations_total{user="user1"} 1
cortex_prometheus_rule_group_iterations_total{user="user2"} 10
cortex_prometheus_rule_group_iterations_total{user="user3"} 100
# HELP cortex_prometheus_rule_group_last_duration_seconds The duration of the last rule group evaluation.
# TYPE cortex_prometheus_rule_group_last_duration_seconds gauge
cortex_prometheus_rule_group_last_duration_seconds{rule_group="group_one",user="user1"} 1000
cortex_prometheus_rule_group_last_duration_seconds{rule_group="group_one",user="user2"} 10000
cortex_prometheus_rule_group_last_duration_seconds{rule_group="group_one",user="user3"} 100000
cortex_prometheus_rule_group_last_duration_seconds{rule_group="group_two",user="user1"} 1000
cortex_prometheus_rule_group_last_duration_seconds{rule_group="group_two",user="user2"} 10000
cortex_prometheus_rule_group_last_duration_seconds{rule_group="group_two",user="user3"} 100000
# HELP cortex_prometheus_rule_group_last_evaluation_timestamp_seconds The timestamp of the last rule group evaluation in seconds.
# TYPE cortex_prometheus_rule_group_last_evaluation_timestamp_seconds gauge
cortex_prometheus_rule_group_last_evaluation_timestamp_seconds{rule_group="group_one",user="user1"} 1000
cortex_prometheus_rule_group_last_evaluation_timestamp_seconds{rule_group="group_one",user="user2"} 10000
cortex_prometheus_rule_group_last_evaluation_timestamp_seconds{rule_group="group_one",user="user3"} 100000
cortex_prometheus_rule_group_last_evaluation_timestamp_seconds{rule_group="group_two",user="user1"} 1000
cortex_prometheus_rule_group_last_evaluation_timestamp_seconds{rule_group="group_two",user="user2"} 10000
cortex_prometheus_rule_group_last_evaluation_timestamp_seconds{rule_group="group_two",user="user3"} 100000
# HELP cortex_prometheus_rule_group_rules The number of rules.
# TYPE cortex_prometheus_rule_group_rules gauge
cortex_prometheus_rule_group_rules{rule_group="group_one",user="user1"} 1000
cortex_prometheus_rule_group_rules{rule_group="group_one",user="user2"} 10000
cortex_prometheus_rule_group_rules{rule_group="group_one",user="user3"} 100000
cortex_prometheus_rule_group_rules{rule_group="group_two",user="user1"} 1000
cortex_prometheus_rule_group_rules{rule_group="group_two",user="user2"} 10000
cortex_prometheus_rule_group_rules{rule_group="group_two",user="user3"} 100000
`))
	require.NoError(t, err)
}

func populateManager(base float64) *prometheus.Registry {
	r := prometheus.NewRegistry()

	metrics := newGroupMetrics(r)

	metrics.evalDuration.Observe(base)
	metrics.iterationDuration.Observe(base)
	metrics.iterationsMissed.Add(base)
	metrics.iterationsScheduled.Add(base)

	metrics.evalTotal.WithLabelValues("group_one").Add(base)
	metrics.evalTotal.WithLabelValues("group_two").Add(base)
	metrics.evalFailures.WithLabelValues("group_one").Add(base)
	metrics.evalFailures.WithLabelValues("group_two").Add(base)

	metrics.groupLastEvalTime.WithLabelValues("group_one").Add(base * 1000)
	metrics.groupLastEvalTime.WithLabelValues("group_two").Add(base * 1000)

	metrics.groupLastDuration.WithLabelValues("group_one").Add(base * 1000)
	metrics.groupLastDuration.WithLabelValues("group_two").Add(base * 1000)

	metrics.groupRules.WithLabelValues("group_one").Add(base * 1000)
	metrics.groupRules.WithLabelValues("group_two").Add(base * 1000)
	return r
}

// Copied from github.com/prometheus/rules/manager.go
type groupMetrics struct {
	evalDuration        prometheus.Summary
	iterationDuration   prometheus.Summary
	iterationsMissed    prometheus.Counter
	iterationsScheduled prometheus.Counter
	evalTotal           *prometheus.CounterVec
	evalFailures        *prometheus.CounterVec
	groupInterval       *prometheus.GaugeVec
	groupLastEvalTime   *prometheus.GaugeVec
	groupLastDuration   *prometheus.GaugeVec
	groupRules          *prometheus.GaugeVec
}

func newGroupMetrics(r prometheus.Registerer) *groupMetrics {
	m := &groupMetrics{
		evalDuration: promauto.With(r).NewSummary(
			prometheus.SummaryOpts{
				Name:       "prometheus_rule_evaluation_duration_seconds",
				Help:       "The duration for a rule to execute.",
				Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			}),
		iterationDuration: promauto.With(r).NewSummary(prometheus.SummaryOpts{
			Name:       "prometheus_rule_group_duration_seconds",
			Help:       "The duration of rule group evaluations.",
			Objectives: map[float64]float64{0.01: 0.001, 0.05: 0.005, 0.5: 0.05, 0.90: 0.01, 0.99: 0.001},
		}),
		iterationsMissed: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "prometheus_rule_group_iterations_missed_total",
			Help: "The total number of rule group evaluations missed due to slow rule group evaluation.",
		}),
		iterationsScheduled: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "prometheus_rule_group_iterations_total",
			Help: "The total number of scheduled rule group evaluations, whether executed or missed.",
		}),
		evalTotal: promauto.With(r).NewCounterVec(
			prometheus.CounterOpts{
				Name: "prometheus_rule_evaluations_total",
				Help: "The total number of rule evaluations.",
			},
			[]string{"rule_group"},
		),
		evalFailures: promauto.With(r).NewCounterVec(
			prometheus.CounterOpts{
				Name: "prometheus_rule_evaluation_failures_total",
				Help: "The total number of rule evaluation failures.",
			},
			[]string{"rule_group"},
		),
		groupInterval: promauto.With(r).NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "prometheus_rule_group_interval_seconds",
				Help: "The interval of a rule group.",
			},
			[]string{"rule_group"},
		),
		groupLastEvalTime: promauto.With(r).NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "prometheus_rule_group_last_evaluation_timestamp_seconds",
				Help: "The timestamp of the last rule group evaluation in seconds.",
			},
			[]string{"rule_group"},
		),
		groupLastDuration: promauto.With(r).NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "prometheus_rule_group_last_duration_seconds",
				Help: "The duration of the last rule group evaluation.",
			},
			[]string{"rule_group"},
		),
		groupRules: promauto.With(r).NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "prometheus_rule_group_rules",
				Help: "The number of rules.",
			},
			[]string{"rule_group"},
		),
	}

	return m
}
