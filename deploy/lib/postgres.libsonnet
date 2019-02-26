local kube = import 'kube-libsonnet/kube.libsonnet';

{
    postgres_deployment:
        local name = $._config.postgres.name;
        local labels = $._config.postgres.labels;
        local image = $._images.postgres;

        local postgresPorts = {
            postgresql: {
                containerPort: 5432
            },
        };

        local env = {
            POSTGRES_DB: 'configs',
        };

        local postgresContainer = kube.Container(name) + {
            image: image,
            env_+: env,
            ports_+: postgresPorts,
            resources+: $._config.postgres.resources,
        };

        local postgresPod = kube.PodSpec + {
            containers_: {
                postgres: postgresContainer,
            },
        };

        kube.Deployment(name) + {
            metadata+: {
                labels: labels,
                namespace: $._config.namespace,
            },
            spec+: {
                template+: {
                    spec: postgresPod,
                    metadata+: {
                        labels: labels
                    },
                },
            },
        },

    postgres_service:
        local name = $._config.postgres.name;
        local labels = $._config.postgres.labels;

        kube.Service(name) + {
            target_pod: $.postgres_deployment.spec.template,
            metadata+: {
                labels: labels,
                namespace: $._config.namespace,
            },
        },
}
