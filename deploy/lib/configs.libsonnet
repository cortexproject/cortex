local kube = import 'kube-libsonnet/kube.libsonnet';

{
    configs_deployment:
        local name = $._config.configs.name;
        local labels = $._config.configs.labels;

        # Arguments
        local extraArgs = $._config.configs.extraArgs;
        local postgres_user = $._config.configs.postgresUser;
        local postgres_password = $._config.configs.postgresPassword;
        local postgres_db = $._config.configs.postgresDb;
        local postgres_path = '/' + postgres_db + '?sslmode=disable';
        local postgres_uri = $._config.postgres.name + '.' + $._config.namespace + '.svc.cluster.local';
        local args = [
            '-target=configs',
            '-server.http-listen-port=80',
            '-database.uri=postgres://' + postgres_user + ':' + postgres_password + '@' + postgres_uri + postgres_path,
            '-database.migrations=/migrations',
        ];

        local configsPorts = {
            http: {
                containerPort: 80,
            },
        };

        # Container
        local configsContainer = kube.Container(name) + {
            image: $._config.configs.image,
            args+: args + extraArgs,
            ports_: configsPorts,
            resources+: $._config.configs.resources,
        };

        local configsPod = kube.PodSpec + {
            containers_: {
                configs: configsContainer,
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
                        labels: labels,
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
                namespace: $._config.namespace,
            },
        },
}
