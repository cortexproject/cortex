local g = import "grafana.libsonnet";
local db = import "bigtable.libsonnet";

g.dashboard(
    title="Cortex - Read",
    panelWidth=12
)
.addPanel(
    g.row(
        title='Querier',
    )
)
.addPanel(
    g.graphPanel(
        title='QPS',
        type='stack',
        targets=[{
            query: 'sum(rate(cortex_request_duration_seconds_count{status_code=~"2.*", route="api_prom_api_v1"}[1m]))',
            legendFormat: '2xx',
        }, {
            query: 'sum(rate(cortex_request_duration_seconds_count{status_code=~"4.*", route="api_prom_api_v1"}[1m]))',
            legendFormat: '4xx',
        }, {
            query: 'sum(rate(cortex_request_duration_seconds_count{status_code=~"5.*", route="api_prom_api_v1"}[1m]))',
            legendFormat: '5xx',
        }],
    ) + {
        aliasColors: {
            "2xx": "#7EB26D",
            "4xx": "#F2C96D",
            "5xx": "#BF1B00",
        },
    }
)
.addPanel(
    g.graphPanel(
        title='Latency',
        targets=[{
            query: 'histogram_quantile(0.5, sum(rate(cortex_request_duration_seconds_bucket{route="api_prom_api_v1"}[1m])) by (le))',
            legendFormat: 'p50',
        }, {
            query: 'histogram_quantile(0.95, sum(rate(cortex_request_duration_seconds_bucket{route="api_prom_api_v1"}[1m])) by (le))',
            legendFormat: 'p95',
        }, {
            query: 'histogram_quantile(0.99, sum(rate(cortex_request_duration_seconds_bucket{route="api_prom_api_v1"}[1m])) by (le))',
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
        title='QPS',
        type='stack',
        targets=[{
            query: 'sum(rate(cortex_request_duration_seconds_count{status_code="success", route=~".*Query|.*LabelValues|.*MetricsForLabelMatchers"}[1m]))',
            legendFormat: 'success',
        }, {
            query: 'sum(rate(cortex_request_duration_seconds_count{status_code!="success", route=~".*Query|.*LabelValues|.*MetricsForLabelMatchers"}[1m]))',
            legendFormat: 'failuer',
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
            query: 'histogram_quantile(0.5, sum(rate(cortex_request_duration_seconds_bucket{route=~".*Query|.*LabelValues|.*MetricsForLabelMatchers"}[1m])) by (le, route))',
            legendFormat: 'p50 - {{route}}',
        }, {
            query: 'histogram_quantile(0.99, sum(rate(cortex_request_duration_seconds_bucket{route=~".*Query|.*LabelValues|.*MetricsForLabelMatchers"}[1m])) by (le, route))',
            legendFormat: 'p99 - {{route}}',
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
        title='QPS',
        type='stack',
        targets=[{
            query: 'sum(rate(cortex_cache_request_duration_seconds_count{method="memcache.fetch", status_code=~"2.*"}[1m]))',
            legendFormat: '2xx',
        }, {
            query: 'sum(rate(cortex_cache_request_duration_seconds_count{method="memcache.fetch", status_code=~"5.*"}[1m]))',
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
            query: 'histogram_quantile(0.5, sum(rate(cortex_cache_request_duration_seconds_bucket{method="memcache.fetch"}[1m])) by (le))',
            legendFormat: 'p50',
        }, {
            query: 'histogram_quantile(0.95, sum(rate(cortex_cache_request_duration_seconds_bucket{method="memcache.fetch"}[1m])) by (le))',
            legendFormat: 'p95',
        }, {
            query: 'histogram_quantile(0.99, sum(rate(cortex_cache_request_duration_seconds_bucket{method="memcache.fetch"}[1m])) by (le))',
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
        title='QPS',
        type='stack',
        targets=db.readRateTargets,
    ) + db.readRateMixin
)
.addPanel(
    g.graphPanel(
        title='Latency',
        targets=db.readLatencyTargets,
    ) + db.readLatencyMixin
)
