// Cortex Configuration
//
// sane defaults for deploying cortex
// this file should not be edited, but you should copy and override parameters provided in here
// this should contain most/all of the knobs you need to turn to deploy cortex. 
// If it is not provided here, you can use mixins on top of the final generated manifests.
// 
{
    _config+:: {
        namespace: 'cortex',
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
            labels: { app: $._config.alertmanager.name },
            extraArgs: [],
            resources: {},
        },
        configs:: {
            name: 'configs',
            labels: { app: $._config.configs.name },
            extraArgs: [],
            resources: {},
        },
        consul:: {
            name: 'consul',
            replicas: 1,
            labels: { app: $._config.consul.name },
            resources: {},
        },
        distributor:: {
            name: 'distributor',
            labels: { app: $._config.distributor.name },
            extraArgs: [],
            resources: {},
        },
        ingester:: {
            name: 'ingester',
            replicas: 4,
            labels: { app: $._config.ingester.name },
            extraArgs: [],
            envKVMixin:: {},
            env: [],
            resources: {},
        },
        postgres:: {
            name: 'postgres',
            labels: { app: $._config.postgres.name },
            resources: {},
        },
        querier:: {
            name: 'querier',
            replicas: 1,
            extraArgs: [],
            envKVMixin:: {},
            env: [],
            serviceType: 'ClusterIP',
            labels: { app: $._config.querier.name }, 
            resources: {},
        },
        queryFrontend:: {
            name: 'query-frontend',
            replicas: 1,
            labels: { app: $._config.queryFrontend.name },
            extraArgs: [],
            resources: {},
        },
        ruler:: {
            name: 'ruler',
            labels: { app: $._config.ruler.name },
            extraArgs: [],
            envKVMixin:: {},
            env: [],
            resources: {},
        },
        tableManager:: {
            name: 'table-manager',
            labels: { app: $._config.tableManager.name },
            extraArgs: [],
            envKVMixin:: {},
            env: [],
            resources: {},
        },
    }
}
