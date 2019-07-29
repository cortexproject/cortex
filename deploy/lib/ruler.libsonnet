local kube = import 'kube-libsonnet/kube.libsonnet';

{
    ruler_deployment:
        local name = $._config.ruler.name;
        local labels = $._config.ruler.labels;

        # Arguments
        local extraArgs = $._config.ruler.extraArgs;
        local alertmanager_uri = 'http://' + $._config.alertmanager.name + '.' + $._config.namespace + '.svc.cluster.local/api/prom/alertmanager/';
        local configs_uri = 'http://' + $._config.configs.name + '.' + $._config.namespace + '.svc.cluster.local';
        local consul_uri = $._config.consul.name + '.' + $._config.namespace + '.svc.cluster.local:8500';
        local memcached_uri = $._config.memcached.name + '.' + $._config.namespace + '.svc.cluster.local';
        local index_fifocache_args = if $._config.ruler.caching.index_fifocache.enable
                                     then ['-store.index-cache-read.cache.enable-fifocache=true',
                                           '-store.index-cache-read.fifocache.size=' + $._config.ruler.caching.index_fifocache.size]
                                     else [];
        local chunk_fifocache_args = if $._config.ruler.caching.chunk_fifocache.enable
                                     then ['-cache.enable-fifocache=true',
                                           '-fifocache.size=' + $._config.ruler.caching.chunk_fifocache.size]
                                     else [];
        local args = [
            '-target=ruler',
            '-server.http-listen-port=80',
            '-config-yaml=/etc/cortex/schemaConfig.yaml',
            '-consul.hostname=' + consul_uri,
            '-ruler.alertmanager-url=' + alertmanager_uri,
            '-ruler.configs.url=' + configs_uri,
            '-memcached.hostname=' + memcached_uri,
            '-store.index-cache-read.memcached.hostname=' + memcached_uri,
        ] + index_fifocache_args + chunk_fifocache_args;

        # Environment Variables
        local env = $._config.ruler.env;
        local envKVMixin = $._config.ruler.envKVMixin;
        local extraEnv = [{name: key, value: envKVMixin[key]} for key in std.objectFields(envKVMixin)];

        local rulerPorts = {
            http: {
                containerPort: 80,
            },
        };

        # SchemaConfig volume
        local schemaConfigVolume = kube.ConfigMapVolume($.schema_configmap);
        local schemaConfigVolumeMount = {
            config_volume: {
                mountPath: '/etc/cortex',
                readOnly: true,
            },
        };
        
        # Container
        local rulerContainer = kube.Container(name) + {
            image: $._config.ruler.image,
            args+: args + extraArgs,
            ports_: rulerPorts,
            volumeMounts_: schemaConfigVolumeMount,
            resources+: $._config.ruler.resources,
        };

        local rulerPod = kube.PodSpec + {
            containers_: {
                ruler: rulerContainer,
            },
            volumes_: {
                config_volume: schemaConfigVolume,
            },
        };

        kube.Deployment(name) + {
            metadata+: {
                labels: labels,
                namespace: $._config.namespace,
            },
            spec+: {
                template+: {
                    spec: rulerPod,
                    metadata+: {
                        labels: labels,
                    },
                },
            },
        },

    ruler_service:
        local name = $._config.ruler.name;
        local labels = $._config.ruler.labels;

        kube.Service(name) + {
            target_pod: $.ruler_deployment.spec.template,
            metadata+: {
                labels: labels,
                namespace: $._config.namespace,
            },
        },
}
