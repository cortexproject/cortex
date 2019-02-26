local kube = import 'kube-libsonnet/kube.libsonnet';

{
    configs_deployment:
        local name = $._config.configs.name;
        local labels = $._config.configs.labels;

        # Arguments
        local extraArgs = $._config.configs.extraArgs;
        local postgres_path = '/configs?sslmode=disable';
        local postgres_uri = $._config.postgres.name + '.' + $._config.namespace + '.svc.cluster.local';
        local args = [
            '-server.http-listen-port=80',
            '-database.uri=postgres://postgres@' + postgres_uri + postgres_path,
            '-database.migrations=/migrations',
        ];

        local configsPorts = {
            http: {
                containerPort: 80
            },
        };

        # Container
        local image = $._images.configs;
        local configsContainer = kube.Container(name) + {
            image: image,
            args+: args + extraArgs,
            ports_: configsPorts,
            resources+: $._config.configs.resources,
        };

        local configsPod = kube.PodSpec + {
            containers_: {
                configs: configsContainer
            },
        };

        kube.Deployment(name) + {
            metadata+: {
                labels: labels,
                namespace: $._config.namespace,
            },
            spec+: {
                template+: {
                    spec: configsPod,
                    metadata+:{ 
                        labels: labels
                    },
                },
            },
        },

    configs_service:
        local name = $._config.configs.name;
        local labels = $._config.configs.labels;

        kube.Service(name) + {
            target_pod: $.configs_deployment.spec.template,
            metadata+: {
                labels: labels,
                namespace: $._config.namespace
            },
        },
}
