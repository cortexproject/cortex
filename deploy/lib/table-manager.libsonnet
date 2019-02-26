local kube = import 'kube-libsonnet/kube.libsonnet';

{

    table_manager_deployment:
        local name = $._config.tableManager.name;
        local labels = $._config.tableManager.labels;

        # SchemaConfig volume
        local schemaConfigVolume = kube.ConfigMapVolume($.schema_configmap);
        local schemaConfigVolumeMount = {
            config_volume: {
                mountPath: '/etc/cortex',
                readOnly: true
            },
        };

        # Ports
        local tableManagerPorts = {
            http: {
                containerPort: 80
            },
        };

        # Arguments
        local extraArgs = $._config.tableManager.extraArgs;
        local args = [
            '-config-yaml=/etc/cortex/schemaConfig.yaml',
        ];

        # Environment Variables
        local env = $._config.tableManager.env;
        local envKVMixin = $._config.tableManager.envKVMixin;
        local extraEnv = [{name: key, value: extraEnv[key]} for key in std.objectFields(envKVMixin)];
        
        # Container
        local image = $._images.tableManager;
        local resources = $._config.tableManager.resources;
        local tableManagerContainer = kube.Container(name) + {
            image: image,
            args+: args + extraArgs,
            env: env + extraEnv,
            ports_: tableManagerPorts,
            volumeMounts_: schemaConfigVolumeMount,
            resources: resources
        };

        # Pod
        local tableManagerPod = kube.PodSpec + {
            containers_: {
                ['table-manager']: tableManagerContainer
            },
            volumes_: {
                config_volume: schemaConfigVolume
            }
        };

        kube.Deployment(name) + {
            metadata+: {
                namespace: $._config.namespace,
                labels: labels,
            },
            spec+: {
                template+: {
                    spec: tableManagerPod,
                    metadata+: {
                        labels: labels
                    }
                },
            }
        },

}
