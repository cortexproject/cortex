package ruler

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/util"
)

// ManagerMetrics aggregates metrics exported by the Prometheus
// rules package and returns them as Cortex metrics
type ManagerMetrics struct {
	regs                  *util.UserRegistries
	disableRuleGroupLabel bool

	EvalDuration         *prometheus.Desc
	IterationDuration    *prometheus.Desc
	IterationsMissed     *prometheus.Desc
	IterationsScheduled  *prometheus.Desc
	EvalTotal            *prometheus.Desc
	EvalFailures         *prometheus.Desc
	GroupInterval        *prometheus.Desc
	GroupLastEvalTime    *prometheus.Desc
	GroupLastDuration    *prometheus.Desc
	GroupRules           *prometheus.Desc
	GroupLastEvalSamples *prometheus.Desc

	NotificationLatency       *prometheus.Desc
	NotificationErrors        *prometheus.Desc
	NotificationSent          *prometheus.Desc
	NotificationDropped       *prometheus.Desc
	NotificationQueueLength   *prometheus.Desc
	NotificationQueueCapacity *prometheus.Desc
	AlertmanagersDiscovered   *prometheus.Desc
}

// NewManagerMetrics returns a ManagerMetrics struct
func NewManagerMetrics(disableRuleGroupLabel bool) *ManagerMetrics {
	commonLabels := []string{"user"}
	if !disableRuleGroupLabel {
		commonLabels = append(commonLabels, "rule_group")
	}
	return &ManagerMetrics{
		regs:                  util.NewUserRegistries(),
		disableRuleGroupLabel: disableRuleGroupLabel,

		EvalDuration: prometheus.NewDesc(
			"cortex_prometheus_rule_evaluation_duration_seconds",
			"The duration for a rule to execute.",
			[]string{"user"},
			nil,
		),
		IterationDuration: prometheus.NewDesc(
			"cortex_prometheus_rule_group_duration_seconds",
			"The duration of rule group evaluations.",
			[]string{"user"},
			nil,
		),
		IterationsMissed: prometheus.NewDesc(
			"cortex_prometheus_rule_group_iterations_missed_total",
			"The total number of rule group evaluations missed due to slow rule group evaluation.",
			commonLabels,
			nil,
		),
		IterationsScheduled: prometheus.NewDesc(
			"cortex_prometheus_rule_group_iterations_total",
			"The total number of scheduled rule group evaluations, whether executed or missed.",
			commonLabels,
			nil,
		),
		EvalTotal: prometheus.NewDesc(
			"cortex_prometheus_rule_evaluations_total",
			"The total number of rule evaluations.",
			commonLabels,
			nil,
		),
		EvalFailures: prometheus.NewDesc(
			"cortex_prometheus_rule_evaluation_failures_total",
			"The total number of rule evaluation failures.",
			commonLabels,
			nil,
		),
		GroupInterval: prometheus.NewDesc(
			"cortex_prometheus_rule_group_interval_seconds",
			"The interval of a rule group.",
			commonLabels,
			nil,
		),
		GroupLastEvalTime: prometheus.NewDesc(
			"cortex_prometheus_rule_group_last_evaluation_timestamp_seconds",
			"The timestamp of the last rule group evaluation in seconds.",
			commonLabels,
			nil,
		),
		GroupLastDuration: prometheus.NewDesc(
			"cortex_prometheus_rule_group_last_duration_seconds",
			"The duration of the last rule group evaluation.",
			commonLabels,
			nil,
		),
		GroupRules: prometheus.NewDesc(
			"cortex_prometheus_rule_group_rules",
			"The number of rules.",
			commonLabels,
			nil,
		),
		GroupLastEvalSamples: prometheus.NewDesc(
			"cortex_prometheus_last_evaluation_samples",
			"The number of samples returned during the last rule group evaluation.",
			commonLabels,
			nil,
		),

		// Prometheus' ruler's notification metrics
		NotificationLatency: prometheus.NewDesc(
			"cortex_prometheus_notifications_latency_seconds",
			"Latency quantiles for sending alert notifications.",
			[]string{"user"},
			nil,
		),

		NotificationErrors: prometheus.NewDesc(
			"cortex_prometheus_notifications_errors_total",
			"Total number of errors sending alert notifications.",
			[]string{"user", "alertmanager"},
			nil,
		),
		NotificationSent: prometheus.NewDesc(
			"cortex_prometheus_notifications_sent_total",
			"Total number of alerts sent.",
			[]string{"user", "alertmanager"},
			nil,
		),
		NotificationDropped: prometheus.NewDesc(
			"cortex_prometheus_notifications_dropped_total",
			"Total number of alerts dropped due to errors when sending to Alertmanager.",
			[]string{"user"},
			nil,
		),
		NotificationQueueLength: prometheus.NewDesc(
			"cortex_prometheus_notifications_queue_length",
			"The number of alert notifications in the queue.",
			[]string{"user"},
			nil,
		),
		NotificationQueueCapacity: prometheus.NewDesc(
			"cortex_prometheus_notifications_queue_capacity",
			"The capacity of the alert notifications queue.",
			[]string{"user"},
			nil,
		),
		AlertmanagersDiscovered: prometheus.NewDesc(
			"cortex_prometheus_notifications_alertmanagers_discovered",
			"The number of alertmanagers discovered and active.",
			[]string{"user"},
			nil,
		),
	}
}

// AddUserRegistry adds a user-specific Prometheus registry.
func (m *ManagerMetrics) AddUserRegistry(user string, reg *prometheus.Registry) {
	m.regs.AddUserRegistry(user, reg)
}

// RemoveUserRegistry removes user-specific Prometheus registry.
func (m *ManagerMetrics) RemoveUserRegistry(user string) {
	m.regs.RemoveUserRegistry(user, true)
}

// Describe implements the Collector interface
func (m *ManagerMetrics) Describe(out chan<- *prometheus.Desc) {
	out <- m.EvalDuration
	out <- m.IterationDuration
	out <- m.IterationsMissed
	out <- m.IterationsScheduled
	out <- m.EvalTotal
	out <- m.EvalFailures
	out <- m.GroupInterval
	out <- m.GroupLastEvalTime
	out <- m.GroupLastDuration
	out <- m.GroupRules
	out <- m.GroupLastEvalSamples

	out <- m.NotificationLatency
	out <- m.NotificationErrors
	out <- m.NotificationSent
	out <- m.NotificationDropped
	out <- m.NotificationQueueLength
	out <- m.NotificationQueueCapacity
	out <- m.AlertmanagersDiscovered
}

// Collect implements the Collector interface
func (m *ManagerMetrics) Collect(out chan<- prometheus.Metric) {
	data := m.regs.BuildMetricFamiliesPerUser()
	labels := []string{}
	if !m.disableRuleGroupLabel {
		labels = append(labels, "rule_group")
	}
	// WARNING: It is important that all metrics generated in this method are "Per User".
	// Thanks to that we can actually *remove* metrics for given user (see RemoveUserRegistry).
	// If same user is later re-added, all metrics will start from 0, which is fine.

	data.SendSumOfSummariesPerUser(out, m.EvalDuration, "prometheus_rule_evaluation_duration_seconds")
	data.SendSumOfSummariesPerUser(out, m.IterationDuration, "prometheus_rule_group_duration_seconds")

	data.SendSumOfCountersPerUserWithLabels(out, m.IterationsMissed, "prometheus_rule_group_iterations_missed_total", labels...)
	data.SendSumOfCountersPerUserWithLabels(out, m.IterationsScheduled, "prometheus_rule_group_iterations_total", labels...)
	data.SendSumOfCountersPerUserWithLabels(out, m.EvalTotal, "prometheus_rule_evaluations_total", labels...)
	data.SendSumOfCountersPerUserWithLabels(out, m.EvalFailures, "prometheus_rule_evaluation_failures_total", labels...)
	data.SendSumOfGaugesPerUserWithLabels(out, m.GroupInterval, "prometheus_rule_group_interval_seconds", labels...)
	data.SendSumOfGaugesPerUserWithLabels(out, m.GroupLastEvalTime, "prometheus_rule_group_last_evaluation_timestamp_seconds", labels...)
	data.SendSumOfGaugesPerUserWithLabels(out, m.GroupLastDuration, "prometheus_rule_group_last_duration_seconds", labels...)
	data.SendSumOfGaugesPerUserWithLabels(out, m.GroupRules, "prometheus_rule_group_rules", labels...)
	data.SendSumOfGaugesPerUserWithLabels(out, m.GroupLastEvalSamples, "prometheus_rule_group_last_evaluation_samples", labels...)

	data.SendSumOfSummariesPerUser(out, m.NotificationLatency, "prometheus_notifications_latency_seconds")
	data.SendSumOfCountersPerUserWithLabels(out, m.NotificationErrors, "prometheus_notifications_errors_total", "alertmanager")
	data.SendSumOfCountersPerUserWithLabels(out, m.NotificationSent, "prometheus_notifications_sent_total", "alertmanager")
	data.SendSumOfCountersPerUser(out, m.NotificationDropped, "prometheus_notifications_dropped_total")
	data.SendSumOfGaugesPerUser(out, m.NotificationQueueLength, "prometheus_notifications_queue_length")
	data.SendSumOfGaugesPerUser(out, m.NotificationQueueCapacity, "prometheus_notifications_queue_capacity")
	data.SendSumOfGaugesPerUser(out, m.AlertmanagersDiscovered, "prometheus_notifications_alertmanagers_discovered")
}
