package alertmanager

import (
	"bytes"
	"testing"

	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

var integrations = []string{
	"email",
	"pagerduty",
	"wechat",
	"pushover",
	"slack",
	"opsgenie",
	"webhook",
	"victorops",
}

func TestAlertmanagerMetricsStore(t *testing.T) {
	mainReg := prometheus.NewPedanticRegistry()

	alertmanangerMetrics := newAlertmanagerMetrics()
	mainReg.MustRegister(alertmanangerMetrics)
	alertmanangerMetrics.addUserRegistry("user1", populateAlertmanager(1))
	alertmanangerMetrics.addUserRegistry("user2", populateAlertmanager(10))
	alertmanangerMetrics.addUserRegistry("user3", populateAlertmanager(100))

	//noinspection ALL
	err := testutil.GatherAndCompare(mainReg, bytes.NewBufferString(`
		# HELP cortex_alertmanager_alerts How many alerts by state.
		# TYPE cortex_alertmanager_alerts gauge
		cortex_alertmanager_alerts{state="active",user="user1"} 1
		cortex_alertmanager_alerts{state="active",user="user2"} 10
		cortex_alertmanager_alerts{state="active",user="user3"} 100
		cortex_alertmanager_alerts{state="suppressed",user="user1"} 2
		cortex_alertmanager_alerts{state="suppressed",user="user2"} 20
		cortex_alertmanager_alerts{state="suppressed",user="user3"} 200
		# HELP cortex_alertmanager_alerts_invalid_total The total number of received alerts that were invalid.
		# TYPE cortex_alertmanager_alerts_invalid_total counter
		cortex_alertmanager_alerts_invalid_total{user="user1"} 2
		cortex_alertmanager_alerts_invalid_total{user="user2"} 20
		cortex_alertmanager_alerts_invalid_total{user="user3"} 200
		# HELP cortex_alertmanager_alerts_received_total The total number of received alerts.
		# TYPE cortex_alertmanager_alerts_received_total counter
		cortex_alertmanager_alerts_received_total{user="user1"} 10
		cortex_alertmanager_alerts_received_total{user="user2"} 100
		cortex_alertmanager_alerts_received_total{user="user3"} 1000
		# HELP cortex_alertmanager_config_hash Hash of the currently loaded alertmanager configuration.
		# TYPE cortex_alertmanager_config_hash gauge
		cortex_alertmanager_config_hash{user="user1"} 0
		cortex_alertmanager_config_hash{user="user2"} 0
		cortex_alertmanager_config_hash{user="user3"} 0
		# HELP cortex_alertmanager_nflog_gc_duration_seconds Duration of the last notification log garbage collection cycle.
		# TYPE cortex_alertmanager_nflog_gc_duration_seconds summary
		cortex_alertmanager_nflog_gc_duration_seconds_sum 111
		cortex_alertmanager_nflog_gc_duration_seconds_count 3
		# HELP cortex_alertmanager_nflog_gossip_messages_propagated_total Number of received gossip messages that have been further gossiped.
		# TYPE cortex_alertmanager_nflog_gossip_messages_propagated_total counter
		cortex_alertmanager_nflog_gossip_messages_propagated_total 111
		# HELP cortex_alertmanager_nflog_queries_total Number of notification log queries were received.
		# TYPE cortex_alertmanager_nflog_queries_total counter
		cortex_alertmanager_nflog_queries_total 111
		# HELP cortex_alertmanager_nflog_query_duration_seconds Duration of notification log query evaluation.
		# TYPE cortex_alertmanager_nflog_query_duration_seconds histogram
		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.005"} 0
		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.01"} 0
		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.025"} 0
		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.05"} 0
		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.1"} 0
		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.25"} 0
		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.5"} 0
		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="1"} 1
		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="2.5"} 1
		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="5"} 1
		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="10"} 2
		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="+Inf"} 3
		cortex_alertmanager_nflog_query_duration_seconds_sum 111
		cortex_alertmanager_nflog_query_duration_seconds_count 3
		# HELP cortex_alertmanager_nflog_query_errors_total Number notification log received queries that failed.
		# TYPE cortex_alertmanager_nflog_query_errors_total counter
		cortex_alertmanager_nflog_query_errors_total 111
		# HELP cortex_alertmanager_nflog_snapshot_duration_seconds Duration of the last notification log snapshot.
		# TYPE cortex_alertmanager_nflog_snapshot_duration_seconds summary
		cortex_alertmanager_nflog_snapshot_duration_seconds_sum 111
		cortex_alertmanager_nflog_snapshot_duration_seconds_count 3
		# HELP cortex_alertmanager_nflog_snapshot_size_bytes Size of the last notification log snapshot in bytes.
		# TYPE cortex_alertmanager_nflog_snapshot_size_bytes gauge
		cortex_alertmanager_nflog_snapshot_size_bytes 111
		# HELP cortex_alertmanager_notification_latency_seconds The latency of notifications in seconds.
		# TYPE cortex_alertmanager_notification_latency_seconds histogram
		cortex_alertmanager_notification_latency_seconds_bucket{le="1"} 14
		cortex_alertmanager_notification_latency_seconds_bucket{le="5"} 19
		cortex_alertmanager_notification_latency_seconds_bucket{le="10"} 21
		cortex_alertmanager_notification_latency_seconds_bucket{le="15"} 23
		cortex_alertmanager_notification_latency_seconds_bucket{le="20"} 24
		cortex_alertmanager_notification_latency_seconds_bucket{le="+Inf"} 24
		cortex_alertmanager_notification_latency_seconds_sum 77.7
		cortex_alertmanager_notification_latency_seconds_count 24
		# HELP cortex_alertmanager_notifications_failed_total The total number of failed notifications.
		# TYPE cortex_alertmanager_notifications_failed_total counter
		cortex_alertmanager_notifications_failed_total{integration="email",user="user1"} 0
		cortex_alertmanager_notifications_failed_total{integration="email",user="user2"} 0
		cortex_alertmanager_notifications_failed_total{integration="email",user="user3"} 0
		cortex_alertmanager_notifications_failed_total{integration="opsgenie",user="user1"} 5
		cortex_alertmanager_notifications_failed_total{integration="opsgenie",user="user2"} 50
		cortex_alertmanager_notifications_failed_total{integration="opsgenie",user="user3"} 500
		cortex_alertmanager_notifications_failed_total{integration="pagerduty",user="user1"} 1
		cortex_alertmanager_notifications_failed_total{integration="pagerduty",user="user2"} 10
		cortex_alertmanager_notifications_failed_total{integration="pagerduty",user="user3"} 100
		cortex_alertmanager_notifications_failed_total{integration="pushover",user="user1"} 3
		cortex_alertmanager_notifications_failed_total{integration="pushover",user="user2"} 30
		cortex_alertmanager_notifications_failed_total{integration="pushover",user="user3"} 300
		cortex_alertmanager_notifications_failed_total{integration="slack",user="user1"} 4
		cortex_alertmanager_notifications_failed_total{integration="slack",user="user2"} 40
		cortex_alertmanager_notifications_failed_total{integration="slack",user="user3"} 400
		cortex_alertmanager_notifications_failed_total{integration="victorops",user="user1"} 7
		cortex_alertmanager_notifications_failed_total{integration="victorops",user="user2"} 70
		cortex_alertmanager_notifications_failed_total{integration="victorops",user="user3"} 700
		cortex_alertmanager_notifications_failed_total{integration="webhook",user="user1"} 6
		cortex_alertmanager_notifications_failed_total{integration="webhook",user="user2"} 60
		cortex_alertmanager_notifications_failed_total{integration="webhook",user="user3"} 600
		cortex_alertmanager_notifications_failed_total{integration="wechat",user="user1"} 2
		cortex_alertmanager_notifications_failed_total{integration="wechat",user="user2"} 20
		cortex_alertmanager_notifications_failed_total{integration="wechat",user="user3"} 200
		# HELP cortex_alertmanager_notification_requests_total The total number of attempted notification requests.
		# TYPE cortex_alertmanager_notification_requests_total counter
		cortex_alertmanager_notification_requests_total{integration="email",user="user1"} 0
		cortex_alertmanager_notification_requests_total{integration="email",user="user2"} 0
		cortex_alertmanager_notification_requests_total{integration="email",user="user3"} 0
		cortex_alertmanager_notification_requests_total{integration="opsgenie",user="user1"} 5
		cortex_alertmanager_notification_requests_total{integration="opsgenie",user="user2"} 50
		cortex_alertmanager_notification_requests_total{integration="opsgenie",user="user3"} 500
		cortex_alertmanager_notification_requests_total{integration="pagerduty",user="user1"} 1
		cortex_alertmanager_notification_requests_total{integration="pagerduty",user="user2"} 10
		cortex_alertmanager_notification_requests_total{integration="pagerduty",user="user3"} 100
		cortex_alertmanager_notification_requests_total{integration="pushover",user="user1"} 3
		cortex_alertmanager_notification_requests_total{integration="pushover",user="user2"} 30
		cortex_alertmanager_notification_requests_total{integration="pushover",user="user3"} 300
		cortex_alertmanager_notification_requests_total{integration="slack",user="user1"} 4
		cortex_alertmanager_notification_requests_total{integration="slack",user="user2"} 40
		cortex_alertmanager_notification_requests_total{integration="slack",user="user3"} 400
		cortex_alertmanager_notification_requests_total{integration="victorops",user="user1"} 7
		cortex_alertmanager_notification_requests_total{integration="victorops",user="user2"} 70
		cortex_alertmanager_notification_requests_total{integration="victorops",user="user3"} 700
		cortex_alertmanager_notification_requests_total{integration="webhook",user="user1"} 6
		cortex_alertmanager_notification_requests_total{integration="webhook",user="user2"} 60
		cortex_alertmanager_notification_requests_total{integration="webhook",user="user3"} 600
		cortex_alertmanager_notification_requests_total{integration="wechat",user="user1"} 2
		cortex_alertmanager_notification_requests_total{integration="wechat",user="user2"} 20
		cortex_alertmanager_notification_requests_total{integration="wechat",user="user3"} 200
		# HELP cortex_alertmanager_notification_requests_failed_total The total number of failed notification requests.
		# TYPE cortex_alertmanager_notification_requests_failed_total counter
		cortex_alertmanager_notification_requests_failed_total{integration="email",user="user1"} 0
		cortex_alertmanager_notification_requests_failed_total{integration="email",user="user2"} 0
		cortex_alertmanager_notification_requests_failed_total{integration="email",user="user3"} 0
		cortex_alertmanager_notification_requests_failed_total{integration="opsgenie",user="user1"} 5
		cortex_alertmanager_notification_requests_failed_total{integration="opsgenie",user="user2"} 50
		cortex_alertmanager_notification_requests_failed_total{integration="opsgenie",user="user3"} 500
		cortex_alertmanager_notification_requests_failed_total{integration="pagerduty",user="user1"} 1
		cortex_alertmanager_notification_requests_failed_total{integration="pagerduty",user="user2"} 10
		cortex_alertmanager_notification_requests_failed_total{integration="pagerduty",user="user3"} 100
		cortex_alertmanager_notification_requests_failed_total{integration="pushover",user="user1"} 3
		cortex_alertmanager_notification_requests_failed_total{integration="pushover",user="user2"} 30
		cortex_alertmanager_notification_requests_failed_total{integration="pushover",user="user3"} 300
		cortex_alertmanager_notification_requests_failed_total{integration="slack",user="user1"} 4
		cortex_alertmanager_notification_requests_failed_total{integration="slack",user="user2"} 40
		cortex_alertmanager_notification_requests_failed_total{integration="slack",user="user3"} 400
		cortex_alertmanager_notification_requests_failed_total{integration="victorops",user="user1"} 7
		cortex_alertmanager_notification_requests_failed_total{integration="victorops",user="user2"} 70
		cortex_alertmanager_notification_requests_failed_total{integration="victorops",user="user3"} 700
		cortex_alertmanager_notification_requests_failed_total{integration="webhook",user="user1"} 6
		cortex_alertmanager_notification_requests_failed_total{integration="webhook",user="user2"} 60
		cortex_alertmanager_notification_requests_failed_total{integration="webhook",user="user3"} 600
		cortex_alertmanager_notification_requests_failed_total{integration="wechat",user="user1"} 2
		cortex_alertmanager_notification_requests_failed_total{integration="wechat",user="user2"} 20
		cortex_alertmanager_notification_requests_failed_total{integration="wechat",user="user3"} 200
		# HELP cortex_alertmanager_notifications_total The total number of attempted notifications.
		# TYPE cortex_alertmanager_notifications_total counter
		cortex_alertmanager_notifications_total{integration="email",user="user1"} 0
		cortex_alertmanager_notifications_total{integration="email",user="user2"} 0
		cortex_alertmanager_notifications_total{integration="email",user="user3"} 0
		cortex_alertmanager_notifications_total{integration="opsgenie",user="user1"} 5
		cortex_alertmanager_notifications_total{integration="opsgenie",user="user2"} 50
		cortex_alertmanager_notifications_total{integration="opsgenie",user="user3"} 500
		cortex_alertmanager_notifications_total{integration="pagerduty",user="user1"} 1
		cortex_alertmanager_notifications_total{integration="pagerduty",user="user2"} 10
		cortex_alertmanager_notifications_total{integration="pagerduty",user="user3"} 100
		cortex_alertmanager_notifications_total{integration="pushover",user="user1"} 3
		cortex_alertmanager_notifications_total{integration="pushover",user="user2"} 30
		cortex_alertmanager_notifications_total{integration="pushover",user="user3"} 300
		cortex_alertmanager_notifications_total{integration="slack",user="user1"} 4
		cortex_alertmanager_notifications_total{integration="slack",user="user2"} 40
		cortex_alertmanager_notifications_total{integration="slack",user="user3"} 400
		cortex_alertmanager_notifications_total{integration="victorops",user="user1"} 7
		cortex_alertmanager_notifications_total{integration="victorops",user="user2"} 70
		cortex_alertmanager_notifications_total{integration="victorops",user="user3"} 700
		cortex_alertmanager_notifications_total{integration="webhook",user="user1"} 6
		cortex_alertmanager_notifications_total{integration="webhook",user="user2"} 60
		cortex_alertmanager_notifications_total{integration="webhook",user="user3"} 600
		cortex_alertmanager_notifications_total{integration="wechat",user="user1"} 2
		cortex_alertmanager_notifications_total{integration="wechat",user="user2"} 20
		cortex_alertmanager_notifications_total{integration="wechat",user="user3"} 200

		# HELP cortex_alertmanager_silences How many silences by state.
		# TYPE cortex_alertmanager_silences gauge
		cortex_alertmanager_silences{state="active",user="user1"} 1
		cortex_alertmanager_silences{state="active",user="user2"} 10
		cortex_alertmanager_silences{state="active",user="user3"} 100
		cortex_alertmanager_silences{state="expired",user="user1"} 2
		cortex_alertmanager_silences{state="expired",user="user2"} 20
		cortex_alertmanager_silences{state="expired",user="user3"} 200
		cortex_alertmanager_silences{state="pending",user="user1"} 3
		cortex_alertmanager_silences{state="pending",user="user2"} 30
		cortex_alertmanager_silences{state="pending",user="user3"} 300
		# HELP cortex_alertmanager_silences_gc_duration_seconds Duration of the last silence garbage collection cycle.
		# TYPE cortex_alertmanager_silences_gc_duration_seconds summary
		cortex_alertmanager_silences_gc_duration_seconds_sum 111
		cortex_alertmanager_silences_gc_duration_seconds_count 3
		# HELP cortex_alertmanager_silences_gossip_messages_propagated_total Number of received gossip messages that have been further gossiped.
		# TYPE cortex_alertmanager_silences_gossip_messages_propagated_total counter
		cortex_alertmanager_silences_gossip_messages_propagated_total 111
		# HELP cortex_alertmanager_silences_queries_total How many silence queries were received.
		# TYPE cortex_alertmanager_silences_queries_total counter
		cortex_alertmanager_silences_queries_total 111
		# HELP cortex_alertmanager_silences_query_duration_seconds Duration of silence query evaluation.
		# TYPE cortex_alertmanager_silences_query_duration_seconds histogram
		cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.005"} 0
		cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.01"} 0
		cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.025"} 0
		cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.05"} 0
		cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.1"} 0
		cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.25"} 0
		cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.5"} 0
		cortex_alertmanager_silences_query_duration_seconds_bucket{le="1"} 1
		cortex_alertmanager_silences_query_duration_seconds_bucket{le="2.5"} 1
		cortex_alertmanager_silences_query_duration_seconds_bucket{le="5"} 1
		cortex_alertmanager_silences_query_duration_seconds_bucket{le="10"} 2
		cortex_alertmanager_silences_query_duration_seconds_bucket{le="+Inf"} 3
		cortex_alertmanager_silences_query_duration_seconds_sum 111
		cortex_alertmanager_silences_query_duration_seconds_count 3
		# HELP cortex_alertmanager_silences_query_errors_total How many silence received queries did not succeed.
		# TYPE cortex_alertmanager_silences_query_errors_total counter
		cortex_alertmanager_silences_query_errors_total 111
		# HELP cortex_alertmanager_silences_snapshot_duration_seconds Duration of the last silence snapshot.
		# TYPE cortex_alertmanager_silences_snapshot_duration_seconds summary
		cortex_alertmanager_silences_snapshot_duration_seconds_sum 111
		cortex_alertmanager_silences_snapshot_duration_seconds_count 3
		# HELP cortex_alertmanager_silences_snapshot_size_bytes Size of the last silence snapshot in bytes.
		# TYPE cortex_alertmanager_silences_snapshot_size_bytes gauge
		cortex_alertmanager_silences_snapshot_size_bytes 111
		# HELP cortex_alertmanager_state_fetch_replica_state_failed_total Number of times we have failed to read and merge the full state from another replica.
		# TYPE cortex_alertmanager_state_fetch_replica_state_failed_total counter
		cortex_alertmanager_state_fetch_replica_state_failed_total 0
		# HELP cortex_alertmanager_state_fetch_replica_state_total Number of times we have tried to read and merge the full state from another replica.
		# TYPE cortex_alertmanager_state_fetch_replica_state_total counter
		cortex_alertmanager_state_fetch_replica_state_total 0
		# HELP cortex_alertmanager_state_initial_sync_duration_seconds Time spent syncing initial state from peers or storage.
		# TYPE cortex_alertmanager_state_initial_sync_duration_seconds histogram
		cortex_alertmanager_state_initial_sync_duration_seconds_bucket{le="+Inf"} 0
		cortex_alertmanager_state_initial_sync_duration_seconds_sum 0
		cortex_alertmanager_state_initial_sync_duration_seconds_count 0
		# HELP cortex_alertmanager_state_initial_sync_total Number of times we have tried to sync initial state from peers or storage.
		# TYPE cortex_alertmanager_state_initial_sync_total counter
		cortex_alertmanager_state_initial_sync_total 0
		# HELP cortex_alertmanager_state_persist_failed_total Number of times we have failed to persist the running state to storage.
		# TYPE cortex_alertmanager_state_persist_failed_total counter
		cortex_alertmanager_state_persist_failed_total 0
		# HELP cortex_alertmanager_state_persist_total Number of times we have tried to persist the running state to storage.
		# TYPE cortex_alertmanager_state_persist_total counter
		cortex_alertmanager_state_persist_total 0
`))
	require.NoError(t, err)
}

func TestAlertmanagerMetricsRemoval(t *testing.T) {
	mainReg := prometheus.NewPedanticRegistry()

	alertmanagerMetrics := newAlertmanagerMetrics()
	mainReg.MustRegister(alertmanagerMetrics)
	alertmanagerMetrics.addUserRegistry("user1", populateAlertmanager(1))
	alertmanagerMetrics.addUserRegistry("user2", populateAlertmanager(10))
	alertmanagerMetrics.addUserRegistry("user3", populateAlertmanager(100))

	// In this test, we assert that metrics are "soft deleted" per the registry removal.
	// In practice, this means several things:
	// a) counters of removed registries are not reset.
	// b) gauges of removed registries are removed.
	// c) histograms/summaries (as these are just counters) are not reset.
	// Instead of just asserting a few of these metrics we go for the whole payload to ensure our tests are sensitive to change and avoid regressions.
	err := testutil.GatherAndCompare(mainReg, bytes.NewBufferString(`
						# HELP cortex_alertmanager_alerts How many alerts by state.
        	            # TYPE cortex_alertmanager_alerts gauge
        	            cortex_alertmanager_alerts{state="active",user="user1"} 1
        	            cortex_alertmanager_alerts{state="active",user="user2"} 10
        	            cortex_alertmanager_alerts{state="active",user="user3"} 100
        	            cortex_alertmanager_alerts{state="suppressed",user="user1"} 2
        	            cortex_alertmanager_alerts{state="suppressed",user="user2"} 20
        	            cortex_alertmanager_alerts{state="suppressed",user="user3"} 200

        	            # HELP cortex_alertmanager_alerts_invalid_total The total number of received alerts that were invalid.
        	            # TYPE cortex_alertmanager_alerts_invalid_total counter
        	            cortex_alertmanager_alerts_invalid_total{user="user1"} 2
        	            cortex_alertmanager_alerts_invalid_total{user="user2"} 20
        	            cortex_alertmanager_alerts_invalid_total{user="user3"} 200

        	            # HELP cortex_alertmanager_alerts_received_total The total number of received alerts.
        	            # TYPE cortex_alertmanager_alerts_received_total counter
        	            cortex_alertmanager_alerts_received_total{user="user1"} 10
        	            cortex_alertmanager_alerts_received_total{user="user2"} 100
        	            cortex_alertmanager_alerts_received_total{user="user3"} 1000

        	            # HELP cortex_alertmanager_config_hash Hash of the currently loaded alertmanager configuration.
        	            # TYPE cortex_alertmanager_config_hash gauge
        	            cortex_alertmanager_config_hash{user="user1"} 0
        	            cortex_alertmanager_config_hash{user="user2"} 0
        	            cortex_alertmanager_config_hash{user="user3"} 0

        	            # HELP cortex_alertmanager_nflog_gc_duration_seconds Duration of the last notification log garbage collection cycle.
        	            # TYPE cortex_alertmanager_nflog_gc_duration_seconds summary
        	            cortex_alertmanager_nflog_gc_duration_seconds_sum 111
        	            cortex_alertmanager_nflog_gc_duration_seconds_count 3

        	            # HELP cortex_alertmanager_nflog_gossip_messages_propagated_total Number of received gossip messages that have been further gossiped.
        	            # TYPE cortex_alertmanager_nflog_gossip_messages_propagated_total counter
        	            cortex_alertmanager_nflog_gossip_messages_propagated_total 111

        	            # HELP cortex_alertmanager_nflog_queries_total Number of notification log queries were received.
        	            # TYPE cortex_alertmanager_nflog_queries_total counter
        	            cortex_alertmanager_nflog_queries_total 111

        	            # HELP cortex_alertmanager_nflog_query_duration_seconds Duration of notification log query evaluation.
        	            # TYPE cortex_alertmanager_nflog_query_duration_seconds histogram
        	            cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.005"} 0
        	            cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.01"} 0
        	            cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.025"} 0
        	            cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.05"} 0
        	            cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.1"} 0
        	            cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.25"} 0
        	            cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.5"} 0
        	            cortex_alertmanager_nflog_query_duration_seconds_bucket{le="1"} 1
        	            cortex_alertmanager_nflog_query_duration_seconds_bucket{le="2.5"} 1
        	            cortex_alertmanager_nflog_query_duration_seconds_bucket{le="5"} 1
        	            cortex_alertmanager_nflog_query_duration_seconds_bucket{le="10"} 2
        	            cortex_alertmanager_nflog_query_duration_seconds_bucket{le="+Inf"} 3
        	            cortex_alertmanager_nflog_query_duration_seconds_sum 111
        	            cortex_alertmanager_nflog_query_duration_seconds_count 3

        	            # HELP cortex_alertmanager_nflog_query_errors_total Number notification log received queries that failed.
        	            # TYPE cortex_alertmanager_nflog_query_errors_total counter
        	            cortex_alertmanager_nflog_query_errors_total 111

        	            # HELP cortex_alertmanager_nflog_snapshot_duration_seconds Duration of the last notification log snapshot.
        	            # TYPE cortex_alertmanager_nflog_snapshot_duration_seconds summary
        	            cortex_alertmanager_nflog_snapshot_duration_seconds_sum 111
        	            cortex_alertmanager_nflog_snapshot_duration_seconds_count 3

        	            # HELP cortex_alertmanager_nflog_snapshot_size_bytes Size of the last notification log snapshot in bytes.
        	            # TYPE cortex_alertmanager_nflog_snapshot_size_bytes gauge
        	            cortex_alertmanager_nflog_snapshot_size_bytes 111

						# HELP cortex_alertmanager_notification_latency_seconds The latency of notifications in seconds.
        	           	# TYPE cortex_alertmanager_notification_latency_seconds histogram
        	            cortex_alertmanager_notification_latency_seconds_bucket{le="1"} 14
        	            cortex_alertmanager_notification_latency_seconds_bucket{le="5"} 19
        	            cortex_alertmanager_notification_latency_seconds_bucket{le="10"} 21
        	            cortex_alertmanager_notification_latency_seconds_bucket{le="15"} 23
        	            cortex_alertmanager_notification_latency_seconds_bucket{le="20"} 24
        	            cortex_alertmanager_notification_latency_seconds_bucket{le="+Inf"} 24
        	            cortex_alertmanager_notification_latency_seconds_sum 77.7
        	            cortex_alertmanager_notification_latency_seconds_count 24

        	            # HELP cortex_alertmanager_notification_requests_failed_total The total number of failed notification requests.
        	            # TYPE cortex_alertmanager_notification_requests_failed_total counter
        	            cortex_alertmanager_notification_requests_failed_total{integration="email",user="user1"} 0
        	            cortex_alertmanager_notification_requests_failed_total{integration="email",user="user2"} 0
        	            cortex_alertmanager_notification_requests_failed_total{integration="email",user="user3"} 0
        	            cortex_alertmanager_notification_requests_failed_total{integration="opsgenie",user="user1"} 5
        	            cortex_alertmanager_notification_requests_failed_total{integration="opsgenie",user="user2"} 50
        	            cortex_alertmanager_notification_requests_failed_total{integration="opsgenie",user="user3"} 500
        	            cortex_alertmanager_notification_requests_failed_total{integration="pagerduty",user="user1"} 1
        	            cortex_alertmanager_notification_requests_failed_total{integration="pagerduty",user="user2"} 10
        	            cortex_alertmanager_notification_requests_failed_total{integration="pagerduty",user="user3"} 100
        	            cortex_alertmanager_notification_requests_failed_total{integration="pushover",user="user1"} 3
        	            cortex_alertmanager_notification_requests_failed_total{integration="pushover",user="user2"} 30
        	            cortex_alertmanager_notification_requests_failed_total{integration="pushover",user="user3"} 300
        	            cortex_alertmanager_notification_requests_failed_total{integration="slack",user="user1"} 4
        	            cortex_alertmanager_notification_requests_failed_total{integration="slack",user="user2"} 40
        	            cortex_alertmanager_notification_requests_failed_total{integration="slack",user="user3"} 400
        	            cortex_alertmanager_notification_requests_failed_total{integration="victorops",user="user1"} 7
        	            cortex_alertmanager_notification_requests_failed_total{integration="victorops",user="user2"} 70
        	            cortex_alertmanager_notification_requests_failed_total{integration="victorops",user="user3"} 700
        	            cortex_alertmanager_notification_requests_failed_total{integration="webhook",user="user1"} 6
        	            cortex_alertmanager_notification_requests_failed_total{integration="webhook",user="user2"} 60
        	            cortex_alertmanager_notification_requests_failed_total{integration="webhook",user="user3"} 600
        	            cortex_alertmanager_notification_requests_failed_total{integration="wechat",user="user1"} 2
        	            cortex_alertmanager_notification_requests_failed_total{integration="wechat",user="user2"} 20
        	            cortex_alertmanager_notification_requests_failed_total{integration="wechat",user="user3"} 200

        	            # HELP cortex_alertmanager_notification_requests_total The total number of attempted notification requests.
        	            # TYPE cortex_alertmanager_notification_requests_total counter
        	            cortex_alertmanager_notification_requests_total{integration="email",user="user1"} 0
        	            cortex_alertmanager_notification_requests_total{integration="email",user="user2"} 0
        	            cortex_alertmanager_notification_requests_total{integration="email",user="user3"} 0
        	            cortex_alertmanager_notification_requests_total{integration="opsgenie",user="user1"} 5
        	            cortex_alertmanager_notification_requests_total{integration="opsgenie",user="user2"} 50
        	            cortex_alertmanager_notification_requests_total{integration="opsgenie",user="user3"} 500
        	            cortex_alertmanager_notification_requests_total{integration="pagerduty",user="user1"} 1
        	            cortex_alertmanager_notification_requests_total{integration="pagerduty",user="user2"} 10
        	            cortex_alertmanager_notification_requests_total{integration="pagerduty",user="user3"} 100
        	            cortex_alertmanager_notification_requests_total{integration="pushover",user="user1"} 3
        	            cortex_alertmanager_notification_requests_total{integration="pushover",user="user2"} 30
        	            cortex_alertmanager_notification_requests_total{integration="pushover",user="user3"} 300
        	            cortex_alertmanager_notification_requests_total{integration="slack",user="user1"} 4
        	            cortex_alertmanager_notification_requests_total{integration="slack",user="user2"} 40
        	            cortex_alertmanager_notification_requests_total{integration="slack",user="user3"} 400
        	            cortex_alertmanager_notification_requests_total{integration="victorops",user="user1"} 7
        	            cortex_alertmanager_notification_requests_total{integration="victorops",user="user2"} 70
        	            cortex_alertmanager_notification_requests_total{integration="victorops",user="user3"} 700
        	            cortex_alertmanager_notification_requests_total{integration="webhook",user="user1"} 6
        	            cortex_alertmanager_notification_requests_total{integration="webhook",user="user2"} 60
        	            cortex_alertmanager_notification_requests_total{integration="webhook",user="user3"} 600
        	            cortex_alertmanager_notification_requests_total{integration="wechat",user="user1"} 2
        	            cortex_alertmanager_notification_requests_total{integration="wechat",user="user2"} 20
        	            cortex_alertmanager_notification_requests_total{integration="wechat",user="user3"} 200

        	            # HELP cortex_alertmanager_notifications_failed_total The total number of failed notifications.
        	            # TYPE cortex_alertmanager_notifications_failed_total counter
        	            cortex_alertmanager_notifications_failed_total{integration="email",user="user1"} 0
        	            cortex_alertmanager_notifications_failed_total{integration="email",user="user2"} 0
        	            cortex_alertmanager_notifications_failed_total{integration="email",user="user3"} 0
        	            cortex_alertmanager_notifications_failed_total{integration="opsgenie",user="user1"} 5
        	            cortex_alertmanager_notifications_failed_total{integration="opsgenie",user="user2"} 50
        	            cortex_alertmanager_notifications_failed_total{integration="opsgenie",user="user3"} 500
        	            cortex_alertmanager_notifications_failed_total{integration="pagerduty",user="user1"} 1
        	            cortex_alertmanager_notifications_failed_total{integration="pagerduty",user="user2"} 10
        	            cortex_alertmanager_notifications_failed_total{integration="pagerduty",user="user3"} 100
        	            cortex_alertmanager_notifications_failed_total{integration="pushover",user="user1"} 3
        	            cortex_alertmanager_notifications_failed_total{integration="pushover",user="user2"} 30
        	            cortex_alertmanager_notifications_failed_total{integration="pushover",user="user3"} 300
        	            cortex_alertmanager_notifications_failed_total{integration="slack",user="user1"} 4
        	            cortex_alertmanager_notifications_failed_total{integration="slack",user="user2"} 40
        	            cortex_alertmanager_notifications_failed_total{integration="slack",user="user3"} 400
        	            cortex_alertmanager_notifications_failed_total{integration="victorops",user="user1"} 7
        	            cortex_alertmanager_notifications_failed_total{integration="victorops",user="user2"} 70
        	            cortex_alertmanager_notifications_failed_total{integration="victorops",user="user3"} 700
        	            cortex_alertmanager_notifications_failed_total{integration="webhook",user="user1"} 6
        	            cortex_alertmanager_notifications_failed_total{integration="webhook",user="user2"} 60
        	            cortex_alertmanager_notifications_failed_total{integration="webhook",user="user3"} 600
        	            cortex_alertmanager_notifications_failed_total{integration="wechat",user="user1"} 2
        	            cortex_alertmanager_notifications_failed_total{integration="wechat",user="user2"} 20
        	            cortex_alertmanager_notifications_failed_total{integration="wechat",user="user3"} 200

        	            # HELP cortex_alertmanager_notifications_total The total number of attempted notifications.
        	            # TYPE cortex_alertmanager_notifications_total counter
        	            cortex_alertmanager_notifications_total{integration="email",user="user1"} 0
        	            cortex_alertmanager_notifications_total{integration="email",user="user2"} 0
        	            cortex_alertmanager_notifications_total{integration="email",user="user3"} 0
        	            cortex_alertmanager_notifications_total{integration="opsgenie",user="user1"} 5
        	            cortex_alertmanager_notifications_total{integration="opsgenie",user="user2"} 50
        	            cortex_alertmanager_notifications_total{integration="opsgenie",user="user3"} 500
        	            cortex_alertmanager_notifications_total{integration="pagerduty",user="user1"} 1
        	            cortex_alertmanager_notifications_total{integration="pagerduty",user="user2"} 10
        	            cortex_alertmanager_notifications_total{integration="pagerduty",user="user3"} 100
        	            cortex_alertmanager_notifications_total{integration="pushover",user="user1"} 3
        	            cortex_alertmanager_notifications_total{integration="pushover",user="user2"} 30
        	            cortex_alertmanager_notifications_total{integration="pushover",user="user3"} 300
        	            cortex_alertmanager_notifications_total{integration="slack",user="user1"} 4
        	            cortex_alertmanager_notifications_total{integration="slack",user="user2"} 40
        	            cortex_alertmanager_notifications_total{integration="slack",user="user3"} 400
        	            cortex_alertmanager_notifications_total{integration="victorops",user="user1"} 7
        	            cortex_alertmanager_notifications_total{integration="victorops",user="user2"} 70
        	            cortex_alertmanager_notifications_total{integration="victorops",user="user3"} 700
        	            cortex_alertmanager_notifications_total{integration="webhook",user="user1"} 6
        	            cortex_alertmanager_notifications_total{integration="webhook",user="user2"} 60
        	            cortex_alertmanager_notifications_total{integration="webhook",user="user3"} 600
        	            cortex_alertmanager_notifications_total{integration="wechat",user="user1"} 2
        	            cortex_alertmanager_notifications_total{integration="wechat",user="user2"} 20
        	            cortex_alertmanager_notifications_total{integration="wechat",user="user3"} 200

        	            # HELP cortex_alertmanager_silences How many silences by state.
        	            # TYPE cortex_alertmanager_silences gauge
        	            cortex_alertmanager_silences{state="active",user="user1"} 1
        	            cortex_alertmanager_silences{state="active",user="user2"} 10
        	            cortex_alertmanager_silences{state="active",user="user3"} 100
        	            cortex_alertmanager_silences{state="expired",user="user1"} 2
        	            cortex_alertmanager_silences{state="expired",user="user2"} 20
        	            cortex_alertmanager_silences{state="expired",user="user3"} 200
        	            cortex_alertmanager_silences{state="pending",user="user1"} 3
        	            cortex_alertmanager_silences{state="pending",user="user2"} 30
        	            cortex_alertmanager_silences{state="pending",user="user3"} 300

        	            # HELP cortex_alertmanager_silences_gc_duration_seconds Duration of the last silence garbage collection cycle.
        	            # TYPE cortex_alertmanager_silences_gc_duration_seconds summary
        	            cortex_alertmanager_silences_gc_duration_seconds_sum 111
        	            cortex_alertmanager_silences_gc_duration_seconds_count 3

        	            # HELP cortex_alertmanager_silences_gossip_messages_propagated_total Number of received gossip messages that have been further gossiped.
        	            # TYPE cortex_alertmanager_silences_gossip_messages_propagated_total counter
        	            cortex_alertmanager_silences_gossip_messages_propagated_total 111

        	            # HELP cortex_alertmanager_silences_queries_total How many silence queries were received.
        	            # TYPE cortex_alertmanager_silences_queries_total counter
        	            cortex_alertmanager_silences_queries_total 111

                        # HELP cortex_alertmanager_silences_query_duration_seconds Duration of silence query evaluation.
						# TYPE cortex_alertmanager_silences_query_duration_seconds histogram
        	            cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.005"} 0
        	            cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.01"} 0
        	            cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.025"} 0
        	            cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.05"} 0
        	            cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.1"} 0
        	            cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.25"} 0
        	            cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.5"} 0
        	            cortex_alertmanager_silences_query_duration_seconds_bucket{le="1"} 1
        	            cortex_alertmanager_silences_query_duration_seconds_bucket{le="2.5"} 1
        	            cortex_alertmanager_silences_query_duration_seconds_bucket{le="5"} 1
        	            cortex_alertmanager_silences_query_duration_seconds_bucket{le="10"} 2
        	            cortex_alertmanager_silences_query_duration_seconds_bucket{le="+Inf"} 3
        	            cortex_alertmanager_silences_query_duration_seconds_sum 111
        	            cortex_alertmanager_silences_query_duration_seconds_count 3

        	            # HELP cortex_alertmanager_silences_query_errors_total How many silence received queries did not succeed.
        	            # TYPE cortex_alertmanager_silences_query_errors_total counter
        	            cortex_alertmanager_silences_query_errors_total 111

        	            # HELP cortex_alertmanager_silences_snapshot_duration_seconds Duration of the last silence snapshot.
        	            # TYPE cortex_alertmanager_silences_snapshot_duration_seconds summary
        	            cortex_alertmanager_silences_snapshot_duration_seconds_sum 111
        	            cortex_alertmanager_silences_snapshot_duration_seconds_count 3

						# HELP cortex_alertmanager_silences_snapshot_size_bytes Size of the last silence snapshot in bytes.
						# TYPE cortex_alertmanager_silences_snapshot_size_bytes gauge
						cortex_alertmanager_silences_snapshot_size_bytes 111
						# HELP cortex_alertmanager_state_fetch_replica_state_failed_total Number of times we have failed to read and merge the full state from another replica.
						# TYPE cortex_alertmanager_state_fetch_replica_state_failed_total counter
						cortex_alertmanager_state_fetch_replica_state_failed_total 0
						# HELP cortex_alertmanager_state_fetch_replica_state_total Number of times we have tried to read and merge the full state from another replica.
						# TYPE cortex_alertmanager_state_fetch_replica_state_total counter
						cortex_alertmanager_state_fetch_replica_state_total 0
						# HELP cortex_alertmanager_state_initial_sync_duration_seconds Time spent syncing initial state from peers or storage.
						# TYPE cortex_alertmanager_state_initial_sync_duration_seconds histogram
						cortex_alertmanager_state_initial_sync_duration_seconds_bucket{le="+Inf"} 0
						cortex_alertmanager_state_initial_sync_duration_seconds_sum 0
						cortex_alertmanager_state_initial_sync_duration_seconds_count 0
						# HELP cortex_alertmanager_state_initial_sync_total Number of times we have tried to sync initial state from peers or storage.
						# TYPE cortex_alertmanager_state_initial_sync_total counter
						cortex_alertmanager_state_initial_sync_total 0
						# HELP cortex_alertmanager_state_persist_failed_total Number of times we have failed to persist the running state to storage.
						# TYPE cortex_alertmanager_state_persist_failed_total counter
						cortex_alertmanager_state_persist_failed_total 0
						# HELP cortex_alertmanager_state_persist_total Number of times we have tried to persist the running state to storage.
						# TYPE cortex_alertmanager_state_persist_total counter
						cortex_alertmanager_state_persist_total 0
`))
	require.NoError(t, err)

	alertmanagerMetrics.removeUserRegistry("user3")

	// After removal counters shouldn't reset.
	err = testutil.GatherAndCompare(mainReg, bytes.NewBufferString(`
    		# HELP cortex_alertmanager_alerts How many alerts by state.
    		# TYPE cortex_alertmanager_alerts gauge
    		cortex_alertmanager_alerts{state="active",user="user1"} 1
    		cortex_alertmanager_alerts{state="active",user="user2"} 10
    		cortex_alertmanager_alerts{state="suppressed",user="user1"} 2
    		cortex_alertmanager_alerts{state="suppressed",user="user2"} 20

    		# HELP cortex_alertmanager_alerts_invalid_total The total number of received alerts that were invalid.
    		# TYPE cortex_alertmanager_alerts_invalid_total counter
    		cortex_alertmanager_alerts_invalid_total{user="user1"} 2
    		cortex_alertmanager_alerts_invalid_total{user="user2"} 20

    		# HELP cortex_alertmanager_alerts_received_total The total number of received alerts.
    		# TYPE cortex_alertmanager_alerts_received_total counter
    		cortex_alertmanager_alerts_received_total{user="user1"} 10
    		cortex_alertmanager_alerts_received_total{user="user2"} 100

    		# HELP cortex_alertmanager_config_hash Hash of the currently loaded alertmanager configuration.
    		# TYPE cortex_alertmanager_config_hash gauge
    		cortex_alertmanager_config_hash{user="user1"} 0
    		cortex_alertmanager_config_hash{user="user2"} 0

    		# HELP cortex_alertmanager_nflog_gc_duration_seconds Duration of the last notification log garbage collection cycle.
    		# TYPE cortex_alertmanager_nflog_gc_duration_seconds summary
    		cortex_alertmanager_nflog_gc_duration_seconds_sum 111
    		cortex_alertmanager_nflog_gc_duration_seconds_count 3

    		# HELP cortex_alertmanager_nflog_gossip_messages_propagated_total Number of received gossip messages that have been further gossiped.
    		# TYPE cortex_alertmanager_nflog_gossip_messages_propagated_total counter
    		cortex_alertmanager_nflog_gossip_messages_propagated_total 111

    		# HELP cortex_alertmanager_nflog_queries_total Number of notification log queries were received.
    		# TYPE cortex_alertmanager_nflog_queries_total counter
    		cortex_alertmanager_nflog_queries_total 111

    		# HELP cortex_alertmanager_nflog_query_duration_seconds Duration of notification log query evaluation.
    		# TYPE cortex_alertmanager_nflog_query_duration_seconds histogram
    		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.005"} 0
    		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.01"} 0
    		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.025"} 0
    		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.05"} 0
    		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.1"} 0
    		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.25"} 0
    		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="0.5"} 0
    		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="1"} 1
    		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="2.5"} 1
    		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="5"} 1
    		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="10"} 2
    		cortex_alertmanager_nflog_query_duration_seconds_bucket{le="+Inf"} 3
    		cortex_alertmanager_nflog_query_duration_seconds_sum 111
    		cortex_alertmanager_nflog_query_duration_seconds_count 3

    		# HELP cortex_alertmanager_nflog_query_errors_total Number notification log received queries that failed.
    		# TYPE cortex_alertmanager_nflog_query_errors_total counter
    		cortex_alertmanager_nflog_query_errors_total 111

    		# HELP cortex_alertmanager_nflog_snapshot_duration_seconds Duration of the last notification log snapshot.
    		# TYPE cortex_alertmanager_nflog_snapshot_duration_seconds summary
    		cortex_alertmanager_nflog_snapshot_duration_seconds_sum 111
    		cortex_alertmanager_nflog_snapshot_duration_seconds_count 3

    		# HELP cortex_alertmanager_nflog_snapshot_size_bytes Size of the last notification log snapshot in bytes.
    		# TYPE cortex_alertmanager_nflog_snapshot_size_bytes gauge
    		cortex_alertmanager_nflog_snapshot_size_bytes 11

    		# HELP cortex_alertmanager_notification_latency_seconds The latency of notifications in seconds.
    		# TYPE cortex_alertmanager_notification_latency_seconds histogram
    		cortex_alertmanager_notification_latency_seconds_bucket{le="1"} 14
    		cortex_alertmanager_notification_latency_seconds_bucket{le="5"} 19
    		cortex_alertmanager_notification_latency_seconds_bucket{le="10"} 21
    		cortex_alertmanager_notification_latency_seconds_bucket{le="15"} 23
    		cortex_alertmanager_notification_latency_seconds_bucket{le="20"} 24
    		cortex_alertmanager_notification_latency_seconds_bucket{le="+Inf"} 24
    		cortex_alertmanager_notification_latency_seconds_sum 77.7
    		cortex_alertmanager_notification_latency_seconds_count 24

    		# HELP cortex_alertmanager_notification_requests_failed_total The total number of failed notification requests.
    		# TYPE cortex_alertmanager_notification_requests_failed_total counter
    		cortex_alertmanager_notification_requests_failed_total{integration="email",user="user1"} 0
    		cortex_alertmanager_notification_requests_failed_total{integration="email",user="user2"} 0
    		cortex_alertmanager_notification_requests_failed_total{integration="opsgenie",user="user1"} 5
    		cortex_alertmanager_notification_requests_failed_total{integration="opsgenie",user="user2"} 50
    		cortex_alertmanager_notification_requests_failed_total{integration="pagerduty",user="user1"} 1
    		cortex_alertmanager_notification_requests_failed_total{integration="pagerduty",user="user2"} 10
    		cortex_alertmanager_notification_requests_failed_total{integration="pushover",user="user1"} 3
    		cortex_alertmanager_notification_requests_failed_total{integration="pushover",user="user2"} 30
    		cortex_alertmanager_notification_requests_failed_total{integration="slack",user="user1"} 4
    		cortex_alertmanager_notification_requests_failed_total{integration="slack",user="user2"} 40
    		cortex_alertmanager_notification_requests_failed_total{integration="victorops",user="user1"} 7
    		cortex_alertmanager_notification_requests_failed_total{integration="victorops",user="user2"} 70
    		cortex_alertmanager_notification_requests_failed_total{integration="webhook",user="user1"} 6
    		cortex_alertmanager_notification_requests_failed_total{integration="webhook",user="user2"} 60
    		cortex_alertmanager_notification_requests_failed_total{integration="wechat",user="user1"} 2
    		cortex_alertmanager_notification_requests_failed_total{integration="wechat",user="user2"} 20

    		# HELP cortex_alertmanager_notification_requests_total The total number of attempted notification requests.
    		# TYPE cortex_alertmanager_notification_requests_total counter
    		cortex_alertmanager_notification_requests_total{integration="email",user="user1"} 0
    		cortex_alertmanager_notification_requests_total{integration="email",user="user2"} 0
    		cortex_alertmanager_notification_requests_total{integration="opsgenie",user="user1"} 5
    		cortex_alertmanager_notification_requests_total{integration="opsgenie",user="user2"} 50
    		cortex_alertmanager_notification_requests_total{integration="pagerduty",user="user1"} 1
    		cortex_alertmanager_notification_requests_total{integration="pagerduty",user="user2"} 10
    		cortex_alertmanager_notification_requests_total{integration="pushover",user="user1"} 3
    		cortex_alertmanager_notification_requests_total{integration="pushover",user="user2"} 30
    		cortex_alertmanager_notification_requests_total{integration="slack",user="user1"} 4
    		cortex_alertmanager_notification_requests_total{integration="slack",user="user2"} 40
    		cortex_alertmanager_notification_requests_total{integration="victorops",user="user1"} 7
    		cortex_alertmanager_notification_requests_total{integration="victorops",user="user2"} 70
    		cortex_alertmanager_notification_requests_total{integration="webhook",user="user1"} 6
    		cortex_alertmanager_notification_requests_total{integration="webhook",user="user2"} 60
    		cortex_alertmanager_notification_requests_total{integration="wechat",user="user1"} 2
    		cortex_alertmanager_notification_requests_total{integration="wechat",user="user2"} 20

    		# HELP cortex_alertmanager_notifications_failed_total The total number of failed notifications.
    		# TYPE cortex_alertmanager_notifications_failed_total counter
    		cortex_alertmanager_notifications_failed_total{integration="email",user="user1"} 0
    		cortex_alertmanager_notifications_failed_total{integration="email",user="user2"} 0
    		cortex_alertmanager_notifications_failed_total{integration="opsgenie",user="user1"} 5
    		cortex_alertmanager_notifications_failed_total{integration="opsgenie",user="user2"} 50
    		cortex_alertmanager_notifications_failed_total{integration="pagerduty",user="user1"} 1
    		cortex_alertmanager_notifications_failed_total{integration="pagerduty",user="user2"} 10
    		cortex_alertmanager_notifications_failed_total{integration="pushover",user="user1"} 3
    		cortex_alertmanager_notifications_failed_total{integration="pushover",user="user2"} 30
    		cortex_alertmanager_notifications_failed_total{integration="slack",user="user1"} 4
    		cortex_alertmanager_notifications_failed_total{integration="slack",user="user2"} 40
    		cortex_alertmanager_notifications_failed_total{integration="victorops",user="user1"} 7
    		cortex_alertmanager_notifications_failed_total{integration="victorops",user="user2"} 70
    		cortex_alertmanager_notifications_failed_total{integration="webhook",user="user1"} 6
    		cortex_alertmanager_notifications_failed_total{integration="webhook",user="user2"} 60
    		cortex_alertmanager_notifications_failed_total{integration="wechat",user="user1"} 2
    		cortex_alertmanager_notifications_failed_total{integration="wechat",user="user2"} 20

    		# HELP cortex_alertmanager_notifications_total The total number of attempted notifications.
    		# TYPE cortex_alertmanager_notifications_total counter
    		cortex_alertmanager_notifications_total{integration="email",user="user1"} 0
    		cortex_alertmanager_notifications_total{integration="email",user="user2"} 0
    		cortex_alertmanager_notifications_total{integration="opsgenie",user="user1"} 5
    		cortex_alertmanager_notifications_total{integration="opsgenie",user="user2"} 50
    		cortex_alertmanager_notifications_total{integration="pagerduty",user="user1"} 1
    		cortex_alertmanager_notifications_total{integration="pagerduty",user="user2"} 10
    		cortex_alertmanager_notifications_total{integration="pushover",user="user1"} 3
    		cortex_alertmanager_notifications_total{integration="pushover",user="user2"} 30
    		cortex_alertmanager_notifications_total{integration="slack",user="user1"} 4
    		cortex_alertmanager_notifications_total{integration="slack",user="user2"} 40
    		cortex_alertmanager_notifications_total{integration="victorops",user="user1"} 7
    		cortex_alertmanager_notifications_total{integration="victorops",user="user2"} 70
    		cortex_alertmanager_notifications_total{integration="webhook",user="user1"} 6
    		cortex_alertmanager_notifications_total{integration="webhook",user="user2"} 60
    		cortex_alertmanager_notifications_total{integration="wechat",user="user1"} 2
    		cortex_alertmanager_notifications_total{integration="wechat",user="user2"} 20

    		# HELP cortex_alertmanager_silences How many silences by state.
    		# TYPE cortex_alertmanager_silences gauge
    		cortex_alertmanager_silences{state="active",user="user1"} 1
    		cortex_alertmanager_silences{state="active",user="user2"} 10
    		cortex_alertmanager_silences{state="expired",user="user1"} 2
    		cortex_alertmanager_silences{state="expired",user="user2"} 20
    		cortex_alertmanager_silences{state="pending",user="user1"} 3
    		cortex_alertmanager_silences{state="pending",user="user2"} 30

    		# HELP cortex_alertmanager_silences_gc_duration_seconds Duration of the last silence garbage collection cycle.
    		# TYPE cortex_alertmanager_silences_gc_duration_seconds summary
    		cortex_alertmanager_silences_gc_duration_seconds_sum 111
    		cortex_alertmanager_silences_gc_duration_seconds_count 3

    		# HELP cortex_alertmanager_silences_gossip_messages_propagated_total Number of received gossip messages that have been further gossiped.
    		# TYPE cortex_alertmanager_silences_gossip_messages_propagated_total counter
    		cortex_alertmanager_silences_gossip_messages_propagated_total 111

    		# HELP cortex_alertmanager_silences_queries_total How many silence queries were received.
    		# TYPE cortex_alertmanager_silences_queries_total counter
    		cortex_alertmanager_silences_queries_total 111

    		# HELP cortex_alertmanager_silences_query_duration_seconds Duration of silence query evaluation.
    		# TYPE cortex_alertmanager_silences_query_duration_seconds histogram
    		cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.005"} 0
    		cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.01"} 0
    		cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.025"} 0
    		cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.05"} 0
    		cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.1"} 0
    		cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.25"} 0
    		cortex_alertmanager_silences_query_duration_seconds_bucket{le="0.5"} 0
    		cortex_alertmanager_silences_query_duration_seconds_bucket{le="1"} 1
    		cortex_alertmanager_silences_query_duration_seconds_bucket{le="2.5"} 1
    		cortex_alertmanager_silences_query_duration_seconds_bucket{le="5"} 1
    		cortex_alertmanager_silences_query_duration_seconds_bucket{le="10"} 2
    		cortex_alertmanager_silences_query_duration_seconds_bucket{le="+Inf"} 3
    		cortex_alertmanager_silences_query_duration_seconds_sum 111
    		cortex_alertmanager_silences_query_duration_seconds_count 3

    		# HELP cortex_alertmanager_silences_query_errors_total How many silence received queries did not succeed.
    		# TYPE cortex_alertmanager_silences_query_errors_total counter
    		cortex_alertmanager_silences_query_errors_total 111

    		# HELP cortex_alertmanager_silences_snapshot_duration_seconds Duration of the last silence snapshot.
    		# TYPE cortex_alertmanager_silences_snapshot_duration_seconds summary
    		cortex_alertmanager_silences_snapshot_duration_seconds_sum 111
    		cortex_alertmanager_silences_snapshot_duration_seconds_count 3

			# HELP cortex_alertmanager_silences_snapshot_size_bytes Size of the last silence snapshot in bytes.
			# TYPE cortex_alertmanager_silences_snapshot_size_bytes gauge
			cortex_alertmanager_silences_snapshot_size_bytes 11

			# HELP cortex_alertmanager_state_fetch_replica_state_failed_total Number of times we have failed to read and merge the full state from another replica.
			# TYPE cortex_alertmanager_state_fetch_replica_state_failed_total counter
			cortex_alertmanager_state_fetch_replica_state_failed_total 0
			# HELP cortex_alertmanager_state_fetch_replica_state_total Number of times we have tried to read and merge the full state from another replica.
			# TYPE cortex_alertmanager_state_fetch_replica_state_total counter
			cortex_alertmanager_state_fetch_replica_state_total 0
			# HELP cortex_alertmanager_state_initial_sync_duration_seconds Time spent syncing initial state from peers or storage.
			# TYPE cortex_alertmanager_state_initial_sync_duration_seconds histogram
			cortex_alertmanager_state_initial_sync_duration_seconds_bucket{le="+Inf"} 0
			cortex_alertmanager_state_initial_sync_duration_seconds_sum 0
			cortex_alertmanager_state_initial_sync_duration_seconds_count 0
			# HELP cortex_alertmanager_state_initial_sync_total Number of times we have tried to sync initial state from peers or storage.
			# TYPE cortex_alertmanager_state_initial_sync_total counter
			cortex_alertmanager_state_initial_sync_total 0
			# HELP cortex_alertmanager_state_persist_failed_total Number of times we have failed to persist the running state to storage.
			# TYPE cortex_alertmanager_state_persist_failed_total counter
			cortex_alertmanager_state_persist_failed_total 0
			# HELP cortex_alertmanager_state_persist_total Number of times we have tried to persist the running state to storage.
			# TYPE cortex_alertmanager_state_persist_total counter
			cortex_alertmanager_state_persist_total 0
`))
	require.NoError(t, err)
}

func populateAlertmanager(base float64) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	s := newSilenceMetrics(reg)
	s.gcDuration.Observe(base)
	s.snapshotDuration.Observe(base)
	s.snapshotSize.Add(base)
	s.queriesTotal.Add(base)
	s.queryErrorsTotal.Add(base)
	s.queryDuration.Observe(base)
	s.propagatedMessagesTotal.Add(base)
	s.silencesActive.Set(base)
	s.silencesExpired.Set(base * 2)
	s.silencesPending.Set(base * 3)

	n := newNflogMetrics(reg)
	n.gcDuration.Observe(base)
	n.snapshotDuration.Observe(base)
	n.snapshotSize.Add(base)
	n.queriesTotal.Add(base)
	n.queryErrorsTotal.Add(base)
	n.queryDuration.Observe(base)
	n.propagatedMessagesTotal.Add(base)

	nm := newNotifyMetrics(reg)
	for i, integration := range integrations {
		nm.numNotifications.WithLabelValues(integration).Add(base * float64(i))
		nm.numFailedNotifications.WithLabelValues(integration).Add(base * float64(i))
		nm.numNotificationRequestsTotal.WithLabelValues(integration).Add(base * float64(i))
		nm.numNotificationRequestsFailedTotal.WithLabelValues(integration).Add(base * float64(i))
		nm.notificationLatencySeconds.WithLabelValues(integration).Observe(base * float64(i) * 0.025)
	}

	m := newMarkerMetrics(reg)
	m.alerts.WithLabelValues(string(types.AlertStateActive)).Add(base)
	m.alerts.WithLabelValues(string(types.AlertStateSuppressed)).Add(base * 2)

	v1APIMetrics := newAPIMetrics("v1", reg)
	v1APIMetrics.firing.Add(base * 2)
	v1APIMetrics.invalid.Add(base)
	v1APIMetrics.resolved.Add(base * 3)

	v2APIMetrics := newAPIMetrics("v2", reg)
	v2APIMetrics.firing.Add(base * 2)
	v2APIMetrics.invalid.Add(base)
	v2APIMetrics.resolved.Add(base * 3)

	return reg
}

// Copied from github.com/alertmanager/nflog/nflog.go
type nflogMetrics struct {
	gcDuration              prometheus.Summary
	snapshotDuration        prometheus.Summary
	snapshotSize            prometheus.Gauge
	queriesTotal            prometheus.Counter
	queryErrorsTotal        prometheus.Counter
	queryDuration           prometheus.Histogram
	propagatedMessagesTotal prometheus.Counter
}

func newNflogMetrics(r prometheus.Registerer) *nflogMetrics {
	m := &nflogMetrics{}

	m.gcDuration = promauto.With(r).NewSummary(prometheus.SummaryOpts{
		Name:       "alertmanager_nflog_gc_duration_seconds",
		Help:       "Duration of the last notification log garbage collection cycle.",
		Objectives: map[float64]float64{},
	})
	m.snapshotDuration = promauto.With(r).NewSummary(prometheus.SummaryOpts{
		Name:       "alertmanager_nflog_snapshot_duration_seconds",
		Help:       "Duration of the last notification log snapshot.",
		Objectives: map[float64]float64{},
	})
	m.snapshotSize = promauto.With(r).NewGauge(prometheus.GaugeOpts{
		Name: "alertmanager_nflog_snapshot_size_bytes",
		Help: "Size of the last notification log snapshot in bytes.",
	})
	m.queriesTotal = promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "alertmanager_nflog_queries_total",
		Help: "Number of notification log queries were received.",
	})
	m.queryErrorsTotal = promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "alertmanager_nflog_query_errors_total",
		Help: "Number notification log received queries that failed.",
	})
	m.queryDuration = promauto.With(r).NewHistogram(prometheus.HistogramOpts{
		Name: "alertmanager_nflog_query_duration_seconds",
		Help: "Duration of notification log query evaluation.",
	})
	m.propagatedMessagesTotal = promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "alertmanager_nflog_gossip_messages_propagated_total",
		Help: "Number of received gossip messages that have been further gossiped.",
	})

	return m
}

// Copied from github.com/alertmanager/silence/silence.go
type silenceMetrics struct {
	gcDuration              prometheus.Summary
	snapshotDuration        prometheus.Summary
	snapshotSize            prometheus.Gauge
	queriesTotal            prometheus.Counter
	queryErrorsTotal        prometheus.Counter
	queryDuration           prometheus.Histogram
	silencesActive          prometheus.Gauge
	silencesPending         prometheus.Gauge
	silencesExpired         prometheus.Gauge
	propagatedMessagesTotal prometheus.Counter
}

func newSilenceMetrics(r prometheus.Registerer) *silenceMetrics {
	m := &silenceMetrics{}

	m.gcDuration = promauto.With(r).NewSummary(prometheus.SummaryOpts{
		Name:       "alertmanager_silences_gc_duration_seconds",
		Help:       "Duration of the last silence garbage collection cycle.",
		Objectives: map[float64]float64{},
	})
	m.snapshotDuration = promauto.With(r).NewSummary(prometheus.SummaryOpts{
		Name:       "alertmanager_silences_snapshot_duration_seconds",
		Help:       "Duration of the last silence snapshot.",
		Objectives: map[float64]float64{},
	})
	m.snapshotSize = promauto.With(r).NewGauge(prometheus.GaugeOpts{
		Name: "alertmanager_silences_snapshot_size_bytes",
		Help: "Size of the last silence snapshot in bytes.",
	})
	m.queriesTotal = promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "alertmanager_silences_queries_total",
		Help: "How many silence queries were received.",
	})
	m.queryErrorsTotal = promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "alertmanager_silences_query_errors_total",
		Help: "How many silence received queries did not succeed.",
	})
	m.queryDuration = promauto.With(r).NewHistogram(prometheus.HistogramOpts{
		Name: "alertmanager_silences_query_duration_seconds",
		Help: "Duration of silence query evaluation.",
	})
	m.propagatedMessagesTotal = promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "alertmanager_silences_gossip_messages_propagated_total",
		Help: "Number of received gossip messages that have been further gossiped.",
	})
	m.silencesActive = promauto.With(r).NewGauge(prometheus.GaugeOpts{
		Name:        "alertmanager_silences",
		Help:        "How many silences by state.",
		ConstLabels: prometheus.Labels{"state": string(types.SilenceStateActive)},
	})
	m.silencesPending = promauto.With(r).NewGauge(prometheus.GaugeOpts{
		Name:        "alertmanager_silences",
		Help:        "How many silences by state.",
		ConstLabels: prometheus.Labels{"state": string(types.SilenceStatePending)},
	})
	m.silencesExpired = promauto.With(r).NewGauge(prometheus.GaugeOpts{
		Name:        "alertmanager_silences",
		Help:        "How many silences by state.",
		ConstLabels: prometheus.Labels{"state": string(types.SilenceStateExpired)},
	})

	return m
}

// Copied from github.com/alertmanager/notify/notify.go
type notifyMetrics struct {
	numNotifications                   *prometheus.CounterVec
	numFailedNotifications             *prometheus.CounterVec
	numNotificationRequestsTotal       *prometheus.CounterVec
	numNotificationRequestsFailedTotal *prometheus.CounterVec
	notificationLatencySeconds         *prometheus.HistogramVec
}

func newNotifyMetrics(r prometheus.Registerer) *notifyMetrics {
	m := &notifyMetrics{
		numNotifications: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Namespace: "alertmanager",
			Name:      "notifications_total",
			Help:      "The total number of attempted notifications.",
		}, []string{"integration"}),
		numFailedNotifications: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Namespace: "alertmanager",
			Name:      "notifications_failed_total",
			Help:      "The total number of failed notifications.",
		}, []string{"integration"}),
		numNotificationRequestsTotal: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Namespace: "alertmanager",
			Name:      "notification_requests_total",
			Help:      "The total number of attempted notification requests.",
		}, []string{"integration"}),
		numNotificationRequestsFailedTotal: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Namespace: "alertmanager",
			Name:      "notification_requests_failed_total",
			Help:      "The total number of failed notification requests.",
		}, []string{"integration"}),
		notificationLatencySeconds: promauto.With(r).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "alertmanager",
			Name:      "notification_latency_seconds",
			Help:      "The latency of notifications in seconds.",
			Buckets:   []float64{1, 5, 10, 15, 20},
		}, []string{"integration"}),
	}
	for _, integration := range integrations {
		m.numNotifications.WithLabelValues(integration)
		m.numFailedNotifications.WithLabelValues(integration)
		m.numNotificationRequestsTotal.WithLabelValues(integration)
		m.numNotificationRequestsFailedTotal.WithLabelValues(integration)
		m.notificationLatencySeconds.WithLabelValues(integration)
	}
	return m
}

type markerMetrics struct {
	alerts *prometheus.GaugeVec
}

func newMarkerMetrics(r prometheus.Registerer) *markerMetrics {
	return &markerMetrics{
		alerts: promauto.With(r).NewGaugeVec(prometheus.GaugeOpts{
			Name: "alertmanager_alerts",
			Help: "How many alerts by state.",
		}, []string{"state"}),
	}
}

// Copied from github.com/alertmanager/api/metrics/metrics.go
type apiMetrics struct {
	firing   prometheus.Counter
	resolved prometheus.Counter
	invalid  prometheus.Counter
}

func newAPIMetrics(version string, r prometheus.Registerer) *apiMetrics {
	numReceivedAlerts := promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Name:        "alertmanager_alerts_received_total",
		Help:        "The total number of received alerts.",
		ConstLabels: prometheus.Labels{"version": version},
	}, []string{"status"})
	numInvalidAlerts := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name:        "alertmanager_alerts_invalid_total",
		Help:        "The total number of received alerts that were invalid.",
		ConstLabels: prometheus.Labels{"version": version},
	})

	return &apiMetrics{
		firing:   numReceivedAlerts.WithLabelValues("firing"),
		resolved: numReceivedAlerts.WithLabelValues("resolved"),
		invalid:  numInvalidAlerts,
	}
}
