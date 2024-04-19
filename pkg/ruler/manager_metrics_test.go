package ruler

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util"
)

func TestManagerMetricsWithRuleGroupLabel(t *testing.T) {
	mainReg := prometheus.NewPedanticRegistry()

	managerMetrics := NewManagerMetrics(false)
	mainReg.MustRegister(managerMetrics)
	managerMetrics.AddUserRegistry("user1", populateManager(1))
	managerMetrics.AddUserRegistry("user2", populateManager(10))
	managerMetrics.AddUserRegistry("user3", populateManager(100))

	managerMetrics.AddUserRegistry("user4", populateManager(1000))
	managerMetrics.RemoveUserRegistry("user4")

	//noinspection ALL
	err := testutil.GatherAndCompare(mainReg, bytes.NewBufferString(`
# HELP cortex_prometheus_last_evaluation_samples The number of samples returned during the last rule group evaluation.
# TYPE cortex_prometheus_last_evaluation_samples gauge
cortex_prometheus_last_evaluation_samples{rule_group="group_one",user="user1"} 1000
cortex_prometheus_last_evaluation_samples{rule_group="group_one",user="user2"} 10000
cortex_prometheus_last_evaluation_samples{rule_group="group_one",user="user3"} 100000
cortex_prometheus_last_evaluation_samples{rule_group="group_two",user="user1"} 1000
cortex_prometheus_last_evaluation_samples{rule_group="group_two",user="user2"} 10000
cortex_prometheus_last_evaluation_samples{rule_group="group_two",user="user3"} 100000
# HELP cortex_prometheus_notifications_alertmanagers_discovered The number of alertmanagers discovered and active.
# TYPE cortex_prometheus_notifications_alertmanagers_discovered gauge
cortex_prometheus_notifications_alertmanagers_discovered{user="user1"} 1
cortex_prometheus_notifications_alertmanagers_discovered{user="user2"} 10
cortex_prometheus_notifications_alertmanagers_discovered{user="user3"} 100
# HELP cortex_prometheus_notifications_dropped_total Total number of alerts dropped due to errors when sending to Alertmanager.
# TYPE cortex_prometheus_notifications_dropped_total counter
cortex_prometheus_notifications_dropped_total{user="user1"} 1
cortex_prometheus_notifications_dropped_total{user="user2"} 10
cortex_prometheus_notifications_dropped_total{user="user3"} 100
# HELP cortex_prometheus_notifications_errors_total Total number of errors sending alert notifications.
# TYPE cortex_prometheus_notifications_errors_total counter
cortex_prometheus_notifications_errors_total{alertmanager="alertmanager_1",user="user1"} 1
cortex_prometheus_notifications_errors_total{alertmanager="alertmanager_1",user="user2"} 10
cortex_prometheus_notifications_errors_total{alertmanager="alertmanager_1",user="user3"} 100
# HELP cortex_prometheus_notifications_latency_seconds Latency quantiles for sending alert notifications.
# TYPE cortex_prometheus_notifications_latency_seconds summary
cortex_prometheus_notifications_latency_seconds{user="user1",quantile="0.5"} 1
cortex_prometheus_notifications_latency_seconds{user="user1",quantile="0.9"} 1
cortex_prometheus_notifications_latency_seconds{user="user1",quantile="0.99"} 1
cortex_prometheus_notifications_latency_seconds_sum{user="user1"} 1
cortex_prometheus_notifications_latency_seconds_count{user="user1"} 1
cortex_prometheus_notifications_latency_seconds{user="user2",quantile="0.5"} 10
cortex_prometheus_notifications_latency_seconds{user="user2",quantile="0.9"} 10
cortex_prometheus_notifications_latency_seconds{user="user2",quantile="0.99"} 10
cortex_prometheus_notifications_latency_seconds_sum{user="user2"} 10
cortex_prometheus_notifications_latency_seconds_count{user="user2"} 1
cortex_prometheus_notifications_latency_seconds{user="user3",quantile="0.5"} 100
cortex_prometheus_notifications_latency_seconds{user="user3",quantile="0.9"} 100
cortex_prometheus_notifications_latency_seconds{user="user3",quantile="0.99"} 100
cortex_prometheus_notifications_latency_seconds_sum{user="user3"} 100
cortex_prometheus_notifications_latency_seconds_count{user="user3"} 1
# HELP cortex_prometheus_notifications_queue_capacity The capacity of the alert notifications queue.
# TYPE cortex_prometheus_notifications_queue_capacity gauge
cortex_prometheus_notifications_queue_capacity{user="user1"} 1
cortex_prometheus_notifications_queue_capacity{user="user2"} 10
cortex_prometheus_notifications_queue_capacity{user="user3"} 100
# HELP cortex_prometheus_notifications_queue_length The number of alert notifications in the queue.
# TYPE cortex_prometheus_notifications_queue_length gauge
cortex_prometheus_notifications_queue_length{user="user1"} 1
cortex_prometheus_notifications_queue_length{user="user2"} 10
cortex_prometheus_notifications_queue_length{user="user3"} 100
# HELP cortex_prometheus_notifications_sent_total Total number of alerts sent.
# TYPE cortex_prometheus_notifications_sent_total counter
cortex_prometheus_notifications_sent_total{alertmanager="alertmanager_1",user="user1"} 1
cortex_prometheus_notifications_sent_total{alertmanager="alertmanager_1",user="user2"} 10
cortex_prometheus_notifications_sent_total{alertmanager="alertmanager_1",user="user3"} 100
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
cortex_prometheus_rule_group_duration_seconds{user="user1",quantile="0.01"} 1
cortex_prometheus_rule_group_duration_seconds{user="user1",quantile="0.05"} 1
cortex_prometheus_rule_group_duration_seconds{user="user1",quantile="0.5"} 1
cortex_prometheus_rule_group_duration_seconds{user="user1",quantile="0.9"} 1
cortex_prometheus_rule_group_duration_seconds{user="user1",quantile="0.99"} 1
cortex_prometheus_rule_group_duration_seconds_sum{user="user1"} 1
cortex_prometheus_rule_group_duration_seconds_count{user="user1"} 1
cortex_prometheus_rule_group_duration_seconds{user="user2",quantile="0.01"} 10
cortex_prometheus_rule_group_duration_seconds{user="user2",quantile="0.05"} 10
cortex_prometheus_rule_group_duration_seconds{user="user2",quantile="0.5"} 10
cortex_prometheus_rule_group_duration_seconds{user="user2",quantile="0.9"} 10
cortex_prometheus_rule_group_duration_seconds{user="user2",quantile="0.99"} 10
cortex_prometheus_rule_group_duration_seconds_sum{user="user2"} 10
cortex_prometheus_rule_group_duration_seconds_count{user="user2"} 1
cortex_prometheus_rule_group_duration_seconds{user="user3",quantile="0.01"} 100
cortex_prometheus_rule_group_duration_seconds{user="user3",quantile="0.05"} 100
cortex_prometheus_rule_group_duration_seconds{user="user3",quantile="0.5"} 100
cortex_prometheus_rule_group_duration_seconds{user="user3",quantile="0.9"} 100
cortex_prometheus_rule_group_duration_seconds{user="user3",quantile="0.99"} 100
cortex_prometheus_rule_group_duration_seconds_sum{user="user3"} 100
cortex_prometheus_rule_group_duration_seconds_count{user="user3"} 1
# HELP cortex_prometheus_rule_group_iterations_missed_total The total number of rule group evaluations missed due to slow rule group evaluation.
# TYPE cortex_prometheus_rule_group_iterations_missed_total counter
cortex_prometheus_rule_group_iterations_missed_total{rule_group="group_one",user="user1"} 1
cortex_prometheus_rule_group_iterations_missed_total{rule_group="group_one",user="user2"} 10
cortex_prometheus_rule_group_iterations_missed_total{rule_group="group_one",user="user3"} 100
cortex_prometheus_rule_group_iterations_missed_total{rule_group="group_two",user="user1"} 1
cortex_prometheus_rule_group_iterations_missed_total{rule_group="group_two",user="user2"} 10
cortex_prometheus_rule_group_iterations_missed_total{rule_group="group_two",user="user3"} 100
# HELP cortex_prometheus_rule_group_iterations_total The total number of scheduled rule group evaluations, whether executed or missed.
# TYPE cortex_prometheus_rule_group_iterations_total counter
cortex_prometheus_rule_group_iterations_total{rule_group="group_one",user="user1"} 1
cortex_prometheus_rule_group_iterations_total{rule_group="group_one",user="user2"} 10
cortex_prometheus_rule_group_iterations_total{rule_group="group_one",user="user3"} 100
cortex_prometheus_rule_group_iterations_total{rule_group="group_two",user="user1"} 1
cortex_prometheus_rule_group_iterations_total{rule_group="group_two",user="user2"} 10
cortex_prometheus_rule_group_iterations_total{rule_group="group_two",user="user3"} 100
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

func TestManagerMetricsWithoutRuleGroupLabel(t *testing.T) {
	mainReg := prometheus.NewPedanticRegistry()

	managerMetrics := NewManagerMetrics(true)
	mainReg.MustRegister(managerMetrics)
	managerMetrics.AddUserRegistry("user1", populateManager(1))
	managerMetrics.AddUserRegistry("user2", populateManager(10))
	managerMetrics.AddUserRegistry("user3", populateManager(100))

	managerMetrics.AddUserRegistry("user4", populateManager(1000))
	managerMetrics.RemoveUserRegistry("user4")

	//noinspection ALL
	err := testutil.GatherAndCompare(mainReg, bytes.NewBufferString(`
# HELP cortex_prometheus_last_evaluation_samples The number of samples returned during the last rule group evaluation.
# TYPE cortex_prometheus_last_evaluation_samples gauge
cortex_prometheus_last_evaluation_samples{user="user1"} 2000
cortex_prometheus_last_evaluation_samples{user="user2"} 20000
cortex_prometheus_last_evaluation_samples{user="user3"} 200000
# HELP cortex_prometheus_notifications_alertmanagers_discovered The number of alertmanagers discovered and active.
# TYPE cortex_prometheus_notifications_alertmanagers_discovered gauge
cortex_prometheus_notifications_alertmanagers_discovered{user="user1"} 1
cortex_prometheus_notifications_alertmanagers_discovered{user="user2"} 10
cortex_prometheus_notifications_alertmanagers_discovered{user="user3"} 100
# HELP cortex_prometheus_notifications_dropped_total Total number of alerts dropped due to errors when sending to Alertmanager.
# TYPE cortex_prometheus_notifications_dropped_total counter
cortex_prometheus_notifications_dropped_total{user="user1"} 1
cortex_prometheus_notifications_dropped_total{user="user2"} 10
cortex_prometheus_notifications_dropped_total{user="user3"} 100
# HELP cortex_prometheus_notifications_errors_total Total number of errors sending alert notifications.
# TYPE cortex_prometheus_notifications_errors_total counter
cortex_prometheus_notifications_errors_total{alertmanager="alertmanager_1",user="user1"} 1
cortex_prometheus_notifications_errors_total{alertmanager="alertmanager_1",user="user2"} 10
cortex_prometheus_notifications_errors_total{alertmanager="alertmanager_1",user="user3"} 100
# HELP cortex_prometheus_notifications_latency_seconds Latency quantiles for sending alert notifications.
# TYPE cortex_prometheus_notifications_latency_seconds summary
cortex_prometheus_notifications_latency_seconds{user="user1",quantile="0.5"} 1
cortex_prometheus_notifications_latency_seconds{user="user1",quantile="0.9"} 1
cortex_prometheus_notifications_latency_seconds{user="user1",quantile="0.99"} 1
cortex_prometheus_notifications_latency_seconds_sum{user="user1"} 1
cortex_prometheus_notifications_latency_seconds_count{user="user1"} 1
cortex_prometheus_notifications_latency_seconds{user="user2",quantile="0.5"} 10
cortex_prometheus_notifications_latency_seconds{user="user2",quantile="0.9"} 10
cortex_prometheus_notifications_latency_seconds{user="user2",quantile="0.99"} 10
cortex_prometheus_notifications_latency_seconds_sum{user="user2"} 10
cortex_prometheus_notifications_latency_seconds_count{user="user2"} 1
cortex_prometheus_notifications_latency_seconds{user="user3",quantile="0.5"} 100
cortex_prometheus_notifications_latency_seconds{user="user3",quantile="0.9"} 100
cortex_prometheus_notifications_latency_seconds{user="user3",quantile="0.99"} 100
cortex_prometheus_notifications_latency_seconds_sum{user="user3"} 100
cortex_prometheus_notifications_latency_seconds_count{user="user3"} 1
# HELP cortex_prometheus_notifications_queue_capacity The capacity of the alert notifications queue.
# TYPE cortex_prometheus_notifications_queue_capacity gauge
cortex_prometheus_notifications_queue_capacity{user="user1"} 1
cortex_prometheus_notifications_queue_capacity{user="user2"} 10
cortex_prometheus_notifications_queue_capacity{user="user3"} 100
# HELP cortex_prometheus_notifications_queue_length The number of alert notifications in the queue.
# TYPE cortex_prometheus_notifications_queue_length gauge
cortex_prometheus_notifications_queue_length{user="user1"} 1
cortex_prometheus_notifications_queue_length{user="user2"} 10
cortex_prometheus_notifications_queue_length{user="user3"} 100
# HELP cortex_prometheus_notifications_sent_total Total number of alerts sent.
# TYPE cortex_prometheus_notifications_sent_total counter
cortex_prometheus_notifications_sent_total{alertmanager="alertmanager_1",user="user1"} 1
cortex_prometheus_notifications_sent_total{alertmanager="alertmanager_1",user="user2"} 10
cortex_prometheus_notifications_sent_total{alertmanager="alertmanager_1",user="user3"} 100
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
cortex_prometheus_rule_evaluation_failures_total{user="user1"} 2
cortex_prometheus_rule_evaluation_failures_total{user="user2"} 20
cortex_prometheus_rule_evaluation_failures_total{user="user3"} 200
# HELP cortex_prometheus_rule_evaluations_total The total number of rule evaluations.
# TYPE cortex_prometheus_rule_evaluations_total counter
cortex_prometheus_rule_evaluations_total{user="user1"} 2
cortex_prometheus_rule_evaluations_total{user="user2"} 20
cortex_prometheus_rule_evaluations_total{user="user3"} 200
# HELP cortex_prometheus_rule_group_duration_seconds The duration of rule group evaluations.
# TYPE cortex_prometheus_rule_group_duration_seconds summary
cortex_prometheus_rule_group_duration_seconds{user="user1",quantile="0.01"} 1
cortex_prometheus_rule_group_duration_seconds{user="user1",quantile="0.05"} 1
cortex_prometheus_rule_group_duration_seconds{user="user1",quantile="0.5"} 1
cortex_prometheus_rule_group_duration_seconds{user="user1",quantile="0.9"} 1
cortex_prometheus_rule_group_duration_seconds{user="user1",quantile="0.99"} 1
cortex_prometheus_rule_group_duration_seconds_sum{user="user1"} 1
cortex_prometheus_rule_group_duration_seconds_count{user="user1"} 1
cortex_prometheus_rule_group_duration_seconds{user="user2",quantile="0.01"} 10
cortex_prometheus_rule_group_duration_seconds{user="user2",quantile="0.05"} 10
cortex_prometheus_rule_group_duration_seconds{user="user2",quantile="0.5"} 10
cortex_prometheus_rule_group_duration_seconds{user="user2",quantile="0.9"} 10
cortex_prometheus_rule_group_duration_seconds{user="user2",quantile="0.99"} 10
cortex_prometheus_rule_group_duration_seconds_sum{user="user2"} 10
cortex_prometheus_rule_group_duration_seconds_count{user="user2"} 1
cortex_prometheus_rule_group_duration_seconds{user="user3",quantile="0.01"} 100
cortex_prometheus_rule_group_duration_seconds{user="user3",quantile="0.05"} 100
cortex_prometheus_rule_group_duration_seconds{user="user3",quantile="0.5"} 100
cortex_prometheus_rule_group_duration_seconds{user="user3",quantile="0.9"} 100
cortex_prometheus_rule_group_duration_seconds{user="user3",quantile="0.99"} 100
cortex_prometheus_rule_group_duration_seconds_sum{user="user3"} 100
cortex_prometheus_rule_group_duration_seconds_count{user="user3"} 1
# HELP cortex_prometheus_rule_group_iterations_missed_total The total number of rule group evaluations missed due to slow rule group evaluation.
# TYPE cortex_prometheus_rule_group_iterations_missed_total counter
cortex_prometheus_rule_group_iterations_missed_total{user="user1"} 2
cortex_prometheus_rule_group_iterations_missed_total{user="user2"} 20
cortex_prometheus_rule_group_iterations_missed_total{user="user3"} 200
# HELP cortex_prometheus_rule_group_iterations_total The total number of scheduled rule group evaluations, whether executed or missed.
# TYPE cortex_prometheus_rule_group_iterations_total counter
cortex_prometheus_rule_group_iterations_total{user="user1"} 2
cortex_prometheus_rule_group_iterations_total{user="user2"} 20
cortex_prometheus_rule_group_iterations_total{user="user3"} 200
# HELP cortex_prometheus_rule_group_last_duration_seconds The duration of the last rule group evaluation.
# TYPE cortex_prometheus_rule_group_last_duration_seconds gauge
cortex_prometheus_rule_group_last_duration_seconds{user="user1"} 2000
cortex_prometheus_rule_group_last_duration_seconds{user="user2"} 20000
cortex_prometheus_rule_group_last_duration_seconds{user="user3"} 200000
# HELP cortex_prometheus_rule_group_last_evaluation_timestamp_seconds The timestamp of the last rule group evaluation in seconds.
# TYPE cortex_prometheus_rule_group_last_evaluation_timestamp_seconds gauge
cortex_prometheus_rule_group_last_evaluation_timestamp_seconds{user="user1"} 2000
cortex_prometheus_rule_group_last_evaluation_timestamp_seconds{user="user2"} 20000
cortex_prometheus_rule_group_last_evaluation_timestamp_seconds{user="user3"} 200000
# HELP cortex_prometheus_rule_group_rules The number of rules.
# TYPE cortex_prometheus_rule_group_rules gauge
cortex_prometheus_rule_group_rules{user="user1"} 2000
cortex_prometheus_rule_group_rules{user="user2"} 20000
cortex_prometheus_rule_group_rules{user="user3"} 200000
`))
	require.NoError(t, err)
}

func populateManager(base float64) *prometheus.Registry {
	r := prometheus.NewRegistry()

	metrics := newGroupMetrics(r)

	metrics.evalDuration.Observe(base)
	metrics.iterationDuration.Observe(base)

	metrics.iterationsScheduled.WithLabelValues("group_one").Add(base)
	metrics.iterationsScheduled.WithLabelValues("group_two").Add(base)
	metrics.iterationsMissed.WithLabelValues("group_one").Add(base)
	metrics.iterationsMissed.WithLabelValues("group_two").Add(base)
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

	metrics.groupLastEvalSamples.WithLabelValues("group_one").Add(base * 1000)
	metrics.groupLastEvalSamples.WithLabelValues("group_two").Add(base * 1000)

	metrics.notificationsLatency.WithLabelValues("alertmanager_1").Observe(base)
	metrics.notificationsErrors.WithLabelValues("alertmanager_1").Add(base)
	metrics.notificationsSent.WithLabelValues("alertmanager_1").Add(base)
	metrics.notificationsDropped.Add(base)
	metrics.notificationsQueueLength.Set(base)
	metrics.notificationsQueueCapacity.Set(base)
	metrics.notificationsAlertmanagersDiscovered.Set(base)
	return r
}

// Copied from github.com/prometheus/rules/manager.go
// and github.com/prometheus/notifier/notifier.go
type groupMetrics struct {
	evalDuration                         prometheus.Summary
	iterationDuration                    prometheus.Summary
	iterationsMissed                     *prometheus.CounterVec
	iterationsScheduled                  *prometheus.CounterVec
	evalTotal                            *prometheus.CounterVec
	evalFailures                         *prometheus.CounterVec
	groupInterval                        *prometheus.GaugeVec
	groupLastEvalTime                    *prometheus.GaugeVec
	groupLastDuration                    *prometheus.GaugeVec
	groupRules                           *prometheus.GaugeVec
	groupLastEvalSamples                 *prometheus.GaugeVec
	notificationsLatency                 *prometheus.SummaryVec
	notificationsErrors                  *prometheus.CounterVec
	notificationsSent                    *prometheus.CounterVec
	notificationsDropped                 prometheus.Counter
	notificationsQueueLength             prometheus.Gauge
	notificationsQueueCapacity           prometheus.Gauge
	notificationsAlertmanagersDiscovered prometheus.Gauge
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
		iterationsMissed: promauto.With(r).NewCounterVec(
			prometheus.CounterOpts{
				Name: "prometheus_rule_group_iterations_missed_total",
				Help: "The total number of rule group evaluations missed due to slow rule group evaluation.",
			},
			[]string{"rule_group"},
		),
		iterationsScheduled: promauto.With(r).NewCounterVec(
			prometheus.CounterOpts{
				Name: "prometheus_rule_group_iterations_total",
				Help: "The total number of scheduled rule group evaluations, whether executed or missed.",
			},
			[]string{"rule_group"},
		),
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
		groupLastEvalSamples: promauto.With(r).NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "prometheus_rule_group_last_evaluation_samples",
				Help: "The number of samples returned during the last rule group evaluation.",
			},
			[]string{"rule_group"},
		),
		notificationsLatency: promauto.With(r).NewSummaryVec(
			prometheus.SummaryOpts{
				Name:       "prometheus_notifications_latency_seconds",
				Help:       "Latency quantiles for sending alert notifications.",
				Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			},
			[]string{"alertmanager"},
		),
		notificationsErrors: promauto.With(r).NewCounterVec(
			prometheus.CounterOpts{
				Name: "prometheus_notifications_errors_total",
				Help: "Latency quantiles for sending alert notifications.",
			},
			[]string{"alertmanager"},
		),
		notificationsSent: promauto.With(r).NewCounterVec(
			prometheus.CounterOpts{
				Name: "prometheus_notifications_sent_total",
				Help: "Total number of errors sending alert notifications",
			},
			[]string{"alertmanager"},
		),
		notificationsDropped: promauto.With(r).NewCounter(
			prometheus.CounterOpts{
				Name: "prometheus_notifications_dropped_total",
				Help: "Total number of alerts dropped due to errors when sending to Alertmanager.",
			},
		),
		notificationsQueueLength: promauto.With(r).NewGauge(
			prometheus.GaugeOpts{
				Name: "prometheus_notifications_queue_length",
				Help: "The number of alert notifications in the queue.",
			},
		),
		notificationsQueueCapacity: promauto.With(r).NewGauge(
			prometheus.GaugeOpts{
				Name: "prometheus_notifications_queue_capacity",
				Help: "The capacity of the alert notifications queue.",
			},
		),
		notificationsAlertmanagersDiscovered: promauto.With(r).NewGauge(
			prometheus.GaugeOpts{
				Name: "prometheus_notifications_alertmanagers_discovered",
				Help: "The number of alertmanagers discovered and active.",
			},
		),
	}
	return m
}

func TestMetricsArePerUser(t *testing.T) {
	mainReg := prometheus.NewPedanticRegistry()

	managerMetrics := NewManagerMetrics(true)
	mainReg.MustRegister(managerMetrics)
	managerMetrics.AddUserRegistry("user1", populateManager(1))
	managerMetrics.AddUserRegistry("user2", populateManager(10))
	managerMetrics.AddUserRegistry("user3", populateManager(100))

	ch := make(chan prometheus.Metric)

	defer func() {
		// drain the channel, so that collecting gouroutine can stop.
		// This is useful if test fails.
		for range ch {
		}
	}()

	go func() {
		managerMetrics.Collect(ch)
		close(ch)
	}()

	for m := range ch {
		desc := m.Desc()

		dtoM := &dto.Metric{}
		err := m.Write(dtoM)

		require.NoError(t, err)

		foundUserLabel := false
		for _, l := range dtoM.Label {
			if l.GetName() == "user" {
				foundUserLabel = true
				break
			}
		}

		assert.True(t, foundUserLabel, "user label not found for metric %s", desc.String())
	}
}

func TestRuleEvalMetricsDeletePerUserMetrics(t *testing.T) {
	dir := t.TempDir()
	reg := prometheus.NewPedanticRegistry()

	m := NewRuleEvalMetrics(Config{RulePath: dir, EnableQueryStats: true}, reg)
	m.TotalWritesVec.WithLabelValues("fake1").Add(10)
	m.TotalWritesVec.WithLabelValues("fake2").Add(10)
	m.FailedWritesVec.WithLabelValues("fake1").Add(10)
	m.FailedWritesVec.WithLabelValues("fake2").Add(10)
	m.TotalQueriesVec.WithLabelValues("fake1").Add(10)
	m.TotalQueriesVec.WithLabelValues("fake2").Add(10)
	m.FailedQueriesVec.WithLabelValues("fake1").Add(10)
	m.FailedQueriesVec.WithLabelValues("fake2").Add(10)
	m.RulerQuerySeconds.WithLabelValues("fake1").Add(10)
	m.RulerQuerySeconds.WithLabelValues("fake2").Add(10)

	metricNames := []string{"cortex_ruler_write_requests_total", "cortex_ruler_write_requests_failed_total", "cortex_ruler_queries_total", "cortex_ruler_queries_failed_total", "cortex_ruler_query_seconds_total"}
	gm, err := reg.Gather()
	require.NoError(t, err)
	mfm, err := util.NewMetricFamilyMap(gm)
	require.NoError(t, err)
	for _, name := range metricNames {
		require.Contains(t, mfm[name].String(), "value:\"fake1\"")
		require.Contains(t, mfm[name].String(), "value:\"fake2\"")
	}

	m.deletePerUserMetrics("fake1")
	gm, err = reg.Gather()
	require.NoError(t, err)
	mfm, err = util.NewMetricFamilyMap(gm)
	require.NoError(t, err)
	for _, name := range metricNames {
		require.NotContains(t, mfm[name].String(), "value:\"fake1\"")
		require.Contains(t, mfm[name].String(), "value:\"fake2\"")
	}
}

func TestRuleGroupMetrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := NewRuleGroupMetrics(reg, util.NewAllowedTenants(nil, []string{"fake3"}))
	m.UpdateRuleGroupsInStore(map[string]int{
		"fake1": 10,
		"fake2": 20,
		"fake3": 30,
	})
	gm, err := reg.Gather()
	require.NoError(t, err)
	mfm, err := util.NewMetricFamilyMap(gm)
	require.NoError(t, err)
	require.Equal(t, 2, len(mfm["cortex_ruler_rule_groups_in_store"].Metric))
	requireMetricEqual(t, mfm["cortex_ruler_rule_groups_in_store"].Metric[0], map[string]string{
		"user": "fake1",
	}, float64(10))
	requireMetricEqual(t, mfm["cortex_ruler_rule_groups_in_store"].Metric[1], map[string]string{
		"user": "fake2",
	}, float64(20))
	m.UpdateRuleGroupsInStore(map[string]int{
		"fake2": 30,
	})
	gm, err = reg.Gather()
	require.NoError(t, err)
	mfm, err = util.NewMetricFamilyMap(gm)
	require.NoError(t, err)
	require.Equal(t, 1, len(mfm["cortex_ruler_rule_groups_in_store"].Metric))
	requireMetricEqual(t, mfm["cortex_ruler_rule_groups_in_store"].Metric[0], map[string]string{
		"user": "fake2",
	}, float64(30))
	m.UpdateRuleGroupsInStore(make(map[string]int))
	gm, err = reg.Gather()
	require.NoError(t, err)
	mfm, err = util.NewMetricFamilyMap(gm)
	require.NoError(t, err)
	require.Nil(t, mfm["cortex_ruler_rule_groups_in_store"])
}
