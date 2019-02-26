local kube = import 'kube-libsonnet/kube.libsonnet';

{
    ruler_deployment:
        local name = $._config.ruler.name;
        local labels = $._config.ruler.labels;

        # Arguments
        local extraArgs = $._config.ruler.extraArgs;
        local alertmanager_uri = $._config.alertmanager.name + '.' + $._config.namespace + '.svc.cluster.local/api/prom/alertmanager/';
        local configs_uri = $._config.configs.name + '.' + $._config.namespace + '.svc.cluster.local';
        local consul_uri = $._config.consul.name + '.' + $._config.namespace + '.svc.cluster.local:8500';
        local args = [
            '-server.http-listen-port=80',
            '-config-yaml=/etc/cortex/schemaConfig.yaml',
            '-consul.hostname=' + consul_uri,
            '-ruler.alertmanager-url=' + alertmanager_uri,
            '-ruler.configs.url=' + configs_uri,
        ];

        # Environment Variables
        local env = $._config.ruler.env;
        local envKVMixin = $._config.ruler.envKVMixin;
        local extraEnv = [{name: key, value: extraEnv[key]} for key in std.objectFields(envKVMixin)];

        local rulerPorts = {
            http: {
                containerPort: 80
            },
        };

        # SchemaConfig volume
        local schemaConfigVolume = kube.ConfigMapVolume($.schema_configmap);
        local schemaConfigVolumeMount = {
            config_volume: {
                mountPath: '/etc/cortex',
                readOnly: true
            },
        };
        
        # Container
        local image = $._images.ruler;
        local rulerContainer = kube.Container(name) + {
            image: image,
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
                config_volume: schemaConfigVolume
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
                        labels: labels
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
