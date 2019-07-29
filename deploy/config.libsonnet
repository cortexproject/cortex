// Cortex Configuration
//
// sane defaults for deploying cortex
// this file should not be edited, but you should copy and override parameters provided in here
// this should contain most/all of the knobs you need to turn to deploy cortex. 
// If it is not provided here, you can use mixins on top of the final generated manifests.
// 
local default_fifocache_size = 102400;

{
    _config+:: {
        namespace: 'cortex',
        image:: "quay.io/cortexproject/cortex:master-be013707",
        schemaConfig_:: [{
            from: '2019-01-01',
            store: 'aws-dynamo',
            schema: 'v9',
            index: {
                prefix: 'cortex_',
                period: '168h0m0s',
            },
            chunks: {
                prefix: 'cortex_chunks_',
                period: '168h0m0s',
            },
        }],
        schemaConfig: std.manifestYamlDoc({'configs': $._config.schemaConfig_}),
        alertmanager:: {
            name: 'alertmanager',
            image: $._config.image,
            labels: { app: $._config.alertmanager.name },
            extraArgs: [],
            resources: {},
        },
        configs:: {
            name: 'configs',
            image: $._config.image,
            labels: { app: $._config.configs.name },
            extraArgs: [],
            resources: {},
            # For connecting to Postgres
            postgresUser: 'postgres',
            postgresPassword: 'postgres',
            postgresDb: 'configs',
        },
        consul:: {
            name: 'consul',
            image: 'consul:1.4.2',
            labels: { app: $._config.consul.name },
            replicas: 1,
            resources: {},
        },
        distributor:: {
            name: 'distributor',
            image: $._config.image,
            labels: { app: $._config.distributor.name },
            extraArgs: [],
            resources: {},
        },
        ingester:: {
            name: 'ingester',
            image: $._config.image,
            labels: { app: $._config.ingester.name },
            replicas: 4,
            extraArgs: [],
            envKVMixin:: {},
            env: [],
            resources: {},
        },
        memcached:: {
            name: 'memcached',
            image: 'memcached:1.5',
            labels: { app: $._config.memcached.name },
            exporterImage: 'quay.io/prometheus/memcached-exporter:v0.5.0',
            metrics: false,
            maxItemMemory: 64, #megabytes
            extraArgs: [],
            resources: {},
        },
        postgres:: {
            name: 'postgres',
            image: 'postgres:9.6',
            labels: { app: $._config.postgres.name },
            resources: {},
            # Authentication
            user: 'postgres',
            password: 'postgres',
            db: 'configs',
            # Storage/Persistence
            storage: {
                persistent: true,
                storageClass: 'default',
                volumeSize: '1Gi',
            },
        },
        querier:: {
            name: 'querier',
            image: $._config.image,
            labels: { app: $._config.querier.name }, 
            replicas: 1,
            extraArgs: [],
            envKVMixin:: {},
            env: [],
            serviceType: 'ClusterIP',
            resources: {},
            caching: {
                chunk_fifocache: {
                    enable: false,
                    size: default_fifocache_size,
                },
                index_fifocache: {
                    enable: false,
                    size: default_fifocache_size,
                },
            },
        },
        queryFrontend:: {
            name: 'query-frontend',
            image: $._config.image,
            labels: { app: $._config.queryFrontend.name },
            replicas: 1,
            extraArgs: [],
            resources: {},
        },
        ruler:: {
            name: 'ruler',
            image: $._config.image,
            labels: { app: $._config.ruler.name },
            extraArgs: [],
            envKVMixin:: {},
            env: [],
            resources: {},
            enacaching: {
                chunk_fifocache: {
                    enable: false,
                    size: default_fifocache_size,
                },
                index_fifocache: {
                    enable: false,
                    size: default_fifocache_size,
                },
            },
        },
        tableManager:: {
            name: 'table-manager',
            image: $._config.image,
            labels: { app: $._config.tableManager.name },
            extraArgs: [],
            envKVMixin:: {},
            env: [],
            resources: {},
        },
    }
}
