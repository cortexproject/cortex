local kube = import 'kube-libsonnet/kube.libsonnet';

(import 'cortex.jsonnet') +
(import 'lib/dynamodb.libsonnet') +
(import 'lib/prometheus.libsonnet') +
(import 'lib/nginx.libsonnet') + 
{
    _config+:: {
        local dynamodb_uri = 'dynamodb://user:pass@' + $._config.dynamodb.name + '.' + $._config.namespace + '.svc.cluster.local:8000',
        dynamodb+:: {
            name: 'dynamodb',
            image: 'amazon/dynamodb-local:latest',
            labels: { app: $._config.dynamodb.name },
            resources: {},
        },
        ingester+:: {
            extraArgs+: [
                '-dynamodb.url=' + dynamodb_uri,
            ],
        },
        memcached+:: {
            metrics: true,
            extraArgs+: [
                "-I 5m",
            ],
        },
        nginx+:: {
            name: 'nginx',
            image: 'nginx:1.15',
            labels: { app: $._config.nginx.name },
            configuration: (importstr 'lib/nginx.conf'),
            resources: {},
        },
        prometheusConfig+:: (import 'lib/prometheusConfig.jsonnet'),
        prometheus+:: {
            name: 'prometheus',
            image: 'quay.io/prometheus/prometheus:v2.9.2',
            labels: { app: $._config.prometheus.name },
            configuration: std.manifestYamlDoc($._config.prometheusConfig),
            resources: {},
        },
        querier+:: {
            extraArgs+: [
                '-dynamodb.url=' + dynamodb_uri,
            ],
        },
        ruler+:: {
            extraArgs+: [
                '-dynamodb.url=' + dynamodb_uri,
            ],
        },
        tableManager+:: {
            extraArgs+: [
                '-dynamodb.url=' + dynamodb_uri,
            ],
        },
    },
    namespace:
        kube.Namespace($._config.namespace)
}
