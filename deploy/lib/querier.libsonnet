local kube = import 'kube-libsonnet/kube.libsonnet';

{
    querier_deployment:
        local name = $._config.querier.name;
        local labels = $._config.querier.labels;

        # Arguments
        local extraArgs = $._config.querier.extraArgs;
        local consul_uri = $._config.consul.name + '.' + $._config.namespace + '.svc.cluster.local:8500';
        local args = [
            '-server.http-listen-port=80',
            '-querier.frontend-address=query-frontend.' + $._config.namespace + '.svc.cluster.local:9095',
            '-querier.batch-iterators=true',
            '-querier.ingester-streaming=true',
            '-config-yaml=/etc/cortex/schemaConfig.yaml',
            '-consul.hostname=' + consul_uri,
        ];
        
        # Environment Variables
        local env = $._config.ingester.env;
        local envKVMixin = $._config.querier.envKVMixin;
        local extraEnv = [{name: key, value: extraEnv[key]} for key in std.objectFields(envKVMixin)];

        # SchemaConfig volume
        local schemaConfigVolume = kube.ConfigMapVolume($.schema_configmap);
        local schemaConfigVolumeMount = {
            config_volume: {
                mountPath: '/etc/cortex',
                readOnly: true
            },
        };

        local querierPorts = {
            http: {
                containerPort: 80
            },
        };

        # Container
        local image = $._images.querier;
        local querierContainer = kube.Container(name) + {
            image: image,
            env: env + extraEnv,
            args+: args + extraArgs,
            ports_: querierPorts,
            volumeMounts_: schemaConfigVolumeMount,
            resources+: $._config.querier.resources,
        };

        # Pod
        local querierPod = kube.PodSpec + {
            containers_: {
                querier: querierContainer,
            },
            volumes_: {
                config_volume: schemaConfigVolume
            },
        };

        # Deployment
        kube.Deployment(name) + {
            metadata+: {
                labels: labels,
                namespace: $._config.namespace,
            },
            spec+: {
                replicas: $._config.querier.replicas,
                template+: {
                    spec: querierPod,
                    metadata+: {
                        labels: labels
                    },
                },
            },
        },

    querier_service:
        local name = $._config.querier.name;
        local labels = $._config.querier.labels;

        kube.Service(name) + {
            target_pod: $.querier_deployment.spec.template,
            metadata+: {
                labels: labels,
                namespace: $._config.namespace,
            },
        },
}
