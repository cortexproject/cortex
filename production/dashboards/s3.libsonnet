{
    name: 's3',

    readRateTargets: [{
        query: 'sum(rate(cortex_s3_request_duration_seconds_count{operation=~"S3.GetObject"}[1m])) by (status_code)',
        legendFormat: '{{status_code}}',
    }],
    readRateMixin: {
        aliasColors: {
            "200": "#7EB26D",
            "500": "#BF1B00",
        },
    },

    readLatencyTargets: [{
        query: 'histogram_quantile(0.5, sum(rate(cortex_s3_request_duration_seconds_bucket[1m])) by (le))',
        legendFormat: 'p50',
    }, {
        query: 'histogram_quantile(0.95, sum(rate(cortex_s3_request_duration_seconds_bucket[1m])) by (le))',
        legendFormat: 'p95',
    }, {
        query: 'histogram_quantile(0.99, sum(rate(cortex_s3_request_duration_seconds_bucket[1m])) by (le))',
        legendFormat: 'p99',
    }],
    readLatencyMixin: {},

    writeRateTargets: [{
        query: 'sum(rate(cortex_s3_request_duration_seconds_count{operation="S3.PutObject"}[1m])) by (status_code)',
        legendFormat: '{{status_code}}',
    }],
    writeRateMixin: {
        aliasColors: {
            "200": "#7EB26D",
            "500": "#BF1B00",
        },
    },

    writeLatencyTargets: [{
        query: 'histogram_quantile(0.5, sum(rate(cortex_s3_request_duration_seconds_bucket{operation="S3.PutObject"}[1m])) by (le))',
        legendFormat: 'p50',
    }, {
        query: 'histogram_quantile(0.95, sum(rate(cortex_s3_request_duration_seconds_bucket{operation="S3.PutObject"}[1m])) by (le))',
        legendFormat: 'p95',
    }, {
        query: 'histogram_quantile(0.99, sum(rate(cortex_s3_request_duration_seconds_bucket{operation="S3.PutObject"}[1m])) by (le))',
        legendFormat: 'p99',
    }],
    writeLatencyMixin: {},

}
