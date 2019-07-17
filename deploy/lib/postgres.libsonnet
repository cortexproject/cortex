local kube = import 'kube-libsonnet/kube.libsonnet';

{
    _postgres_pvc::
        local name = $._config.postgres.name + '-data';
        if $._config.postgres.storage.persistent
        then kube.PersistentVolumeClaim(name) + {
            [if $._config.postgres.storage.storageClass != 'default' then 'storageClass']::
                $._config.postgres.storage.storageClass,
            metadata+: {
                namespace: $._config.namespace,
            },
            storage: $._config.postgres.storage.volumeSize,
        } else {},

    postgres_secret:
        local postgres_user = $._config.postgres.user;
        local postgres_password = $._config.postgres.password;
        kube.Secret($._config.postgres.name) {
            metadata+: {
                namespace: $._config.namespace,
            },
            data_+: {
                database_user: postgres_user,
                database_password: postgres_password,
            },
        },

    postgres_deployment:
        local name = $._config.postgres.name;
        local labels = $._config.postgres.labels;
        local initial_db = $._config.postgres.db;

        local postgresPorts = {
            postgresql: {
                containerPort: 5432,
            },
        };

        local env = {
            PGDATA: '/data/postgres',
            POSTGRES_DB: initial_db,
            POSTGRES_USER: kube.SecretKeyRef($.postgres_secret,
                                             "database_user"),
            POSTGRES_PASSWORD: kube.SecretKeyRef($.postgres_secret,
                                                 "database_password"),
        };

        local postgresContainer = kube.Container(name) + {
            image: $._config.postgres.image,
            env_+: env,
            ports_+: postgresPorts,
            volumeMounts_+: {
                'postgres-data': {
                    mountPath: '/data',
                    subPath: 'postgres',
                },
            },
            resources+: $._config.postgres.resources,
        };

        local postgresPod = kube.PodSpec + {
            containers_: {
                postgres: postgresContainer,
            },
            volumes_: {
                'postgres-data': (
                    if $._config.postgres.storage.persistent
                    then kube.PersistentVolumeClaimVolume($._postgres_pvc)
                    else kube.EmptyDirVolume()
                ),
            },
        };

        kube.List() {
            items: [
                kube.Deployment(name) + {
                    metadata+: {
                        labels: labels,
                        namespace: $._config.namespace,
                    },
                    spec+: {
                        template+: {
                            spec: postgresPod,
                            metadata+: {
                                labels: labels,
                            },
                        },
                    },
                },
            ] + (if $._config.postgres.storage.persistent
                 then [$._postgres_pvc] else []),
        },

    postgres_service:
        local name = $._config.postgres.name;
        local labels = $._config.postgres.labels;

        kube.Service(name) + {
            target_pod: $.postgres_deployment.items[0].spec.template,
            metadata+: {
                labels: labels,
                namespace: $._config.namespace,
            },
        },
}
