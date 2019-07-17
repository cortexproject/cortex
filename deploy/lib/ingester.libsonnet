local kube = import 'kube-libsonnet/kube.libsonnet';

{
    ingester_deployment:
        local name = $._config.ingester.name;

        # Arguments
        local extraArgs = $._config.ingester.extraArgs;
        local consul_uri = $._config.consul.name + '.' + $._config.namespace + '.svc.cluster.local:8500';
        local memcached_uri = $._config.memcached.name + '.' + $._config.namespace + '.svc.cluster.local';
        local args = [
            '-target=ingester',
            '-ingester.join-after=30s',
            '-ingester.claim-on-rollout=true',
            '-ingester.normalise-tokens=true',
            '-config-yaml=/etc/cortex/schemaConfig.yaml',
            '-consul.hostname=' + consul_uri,
            '-memcached.hostname=' + memcached_uri,
        ];
        
        # Environment Variables
        local env = $._config.ingester.env;
        local envKVMixin = $._config.ingester.envKVMixin;
        local extraEnv = [{name: key, value: envKVMixin[key]} for key in std.objectFields(envKVMixin)];

        local readinessProbe = {
            httpGet: {
                path: '/ready',
                port: 80,
            },
            initialDelaySeconds: 15,
        };

        # SchemaConfig volume
        local schemaConfigVolume = kube.ConfigMapVolume($.schema_configmap);
        local schemaConfigVolumeMount = {
            config_volume: {
                mountPath: '/etc/cortex',
                readOnly: true,
            },
        };

        # Ports
        local ingesterPorts = {
            http: {
                containerPort: 80,
            },
        };

        # Container
        local ingesterContainer = kube.Container(name) + {
            image: $._config.ingester.image,
            ports_: ingesterPorts,
            args+: args + extraArgs,
            env: env + extraEnv,
            volumeMounts_: schemaConfigVolumeMount,
            resources+: $._config.ingester.resources,
            readinessProbe: readinessProbe,
        };

        # Pod
        local ingesterPod = kube.PodSpec + {
            containers_: {
                ingester: ingesterContainer,
            },
            # Give ingesters 40 minutes to flush chunks and exit cleanly
            # They will exit early when completely flushed
            terminationGracePeriodSeconds: 2400,
            volumes_: {
                config_volume: schemaConfigVolume,
            },
        };

        kube.Deployment(name) + {
            metadata+: {
                labels: $._config.ingester.labels,
                namespace: $._config.namespace,
            },
            spec+: {
                replicas: $._config.ingester.replicas,
                minReadySeconds: 60,
                template+: {
                    spec: ingesterPod,
                    metadata+: {
                        labels: $._config.ingester.labels,
                    },
                },
                # Modify rolling update strategy to halt a bad upgrade
                # If the new ingester doesn't come up cleanly, the upgrade will not proceed
                strategy: {
                    type: "RollingUpdate",
                    rollingUpdate: {
                        maxSurge: 0,
                        maxUnavailable: 1,
                    },
                },
            },
        },
}
