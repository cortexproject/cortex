local g = import "grafana.libsonnet";
local db = import "bigtable.libsonnet";

g.dashboard(
    title="Cortex - Write",
    panelWidth=12
)
.addPanel(
    g.row(
        title='Distributor',
    )
)
.addPanel(
    g.graphPanel(
        title='WPS',
        type='stack',
        targets=[{
            query: 'sum(rate(cortex_request_duration_seconds_count{route="api_prom_push", status_code=~"2.*"}[1m]))',
            legendFormat: '2xx',
        }, {
            query: 'sum(rate(cortex_request_duration_seconds_count{route="api_prom_push", status_code=~"4.*"}[1m]))',
            legendFormat: '4xx',
        }, {
            query: 'sum(rate(cortex_request_duration_seconds_count{route="api_prom_push", status_code=~"5.*"}[1m]))',
            legendFormat: '5xx',
        }],
    ) + {
        aliasColors: {
            '2xx': "#7EB26D",
            '4xx': "#F2C96D",
            '5xx': "#BF1B00",
        },
    }
)
.addPanel(
    g.graphPanel(
        title='Latency',
        targets=[{
            query: 'histogram_quantile(0.5, sum(rate(cortex_request_duration_seconds_bucket{route="api_prom_push"}[1m])) by (le))',
            legendFormat: 'p50',
        }, {
            query: 'histogram_quantile(0.95, sum(rate(cortex_request_duration_seconds_bucket{route="api_prom_push"}[1m])) by (le))',
            legendFormat: 'p95',
        }, {
            query: 'histogram_quantile(0.99, sum(rate(cortex_request_duration_seconds_bucket{route="api_prom_push"}[1m])) by (le))',
            legendFormat: 'p99',
        }],
    )
)
.addPanel(
    g.row(
        title='Ingester',
    )
)
.addPanel(
    g.graphPanel(
        title='WPS',
        type='stack',
        targets=[{
            query: 'sum(rate(cortex_request_duration_seconds_count{status_code="success", route=~".*Push"}[1m]))',
            legendFormat: 'success',
        }, {
            query: 'sum(rate(cortex_request_duration_seconds_count{status_code!="success", route=~".*Push"}[1m]))',
            legendFormat: 'failure',
        }],
    ) + {
        aliasColors: {
            success: "#7EB26D",
            failure: "#BF1B00",
        },
    }
)
.addPanel(
    g.graphPanel(
        title='Latency',
        targets=[{
            query: 'histogram_quantile(0.5, sum(rate(cortex_request_duration_seconds_bucket{route=~".*Push"}[1m])) by (le))',
            legendFormat: 'p50',
        }, {
            query: 'histogram_quantile(0.95, sum(rate(cortex_request_duration_seconds_bucket{route=~".*Push"}[1m])) by (le))',
            legendFormat: 'p95',
        }, {
            query: 'histogram_quantile(0.99, sum(rate(cortex_request_duration_seconds_bucket{route=~".*Push"}[1m])) by (le))',
            legendFormat: 'p99',
        }],
    )
)
.addPanel(
    g.row(
        title='Ruler',
    )
)
.addPanel(
    g.graphPanel(
        title='Rules Processed',
        targets=[{
            query: 'rate(cortex_rules_processed_total[1m])',
            legendFormat: 'pod_name',
        }],
    )
)
.addPanel(
    g.graphPanel(
        title='Group Eval Durations',
        targets=[{
            query: 'histogram_quantile(0.5, sum(rate(cortex_group_evaluation_duration_seconds_bucket[1m])) by (le))',
            legendFormat: 'p50',
        }, {
            query: 'histogram_quantile(0.95, sum(rate(cortex_group_evaluation_duration_seconds_bucket[1m])) by (le))',
            legendFormat: 'p95',
        }, {
            query: 'histogram_quantile(0.99, sum(rate(cortex_group_evaluation_duration_seconds_bucket[1m])) by (le))',
            legendFormat: 'p99',
        }],
    )
)
.addPanel(
    g.row(
        title='Memcached',
    )
)
.addPanel(
    g.graphPanel(
        title='WPS',
        type='stack',
        targets=[{
            query: 'sum(rate(cortex_cache_request_duration_seconds_count{method="memcache.store", status_code=~"2.*"}[1m]))',
            legendFormat: '2xx',
        }, {
            query: 'sum(rate(cortex_cache_request_duration_seconds_count{method="memcache.store", status_code=~"5.*"}[1m]))',
            legendFormat: '5xx',
        }],
    ) + {
        aliasColors: {
            "2xx": "#7EB26D",
            "5xx": "#BF1B00",
        },
    }
)
.addPanel(
    g.graphPanel(
        title='Latency',
        targets=[{
            query: 'histogram_quantile(0.5, sum(rate(cortex_cache_request_duration_seconds_bucket{method="memcache.store"}[1m])) by (le))',
            legendFormat: 'p50',
        }, {
            query: 'histogram_quantile(0.95, sum(rate(cortex_cache_request_duration_seconds_bucket{method="memcache.store"}[1m])) by (le))',
            legendFormat: 'p95',
        }, {
            query: 'histogram_quantile(0.99, sum(rate(cortex_cache_request_duration_seconds_bucket{method="memcache.store"}[1m])) by (le))',
            legendFormat: 'p99',
        }],
    )
)
.addPanel(
    g.row(
        title=db.name,
    )
)
.addPanel(
    g.graphPanel(
        title='WPS',
        type='stack',
        targets=db.writeRateTargets,
    ) + db.writeRateMixin
)
.addPanel(
    g.graphPanel(
        title='Latency',
        targets=db.writeLatencyTargets,
    ) + db.writeLatencyMixin
)
