local kube = import 'kube-libsonnet/kube.libsonnet';

{
    distributor_deployment:
        local name = $._config.distributor.name;
        local labels = $._config.distributor.labels;

        # Arguments
        local extraArgs = $._config.distributor.extraArgs;
        local consul_uri = $._config.consul.name + '.' + $._config.namespace + '.svc.cluster.local:8500';
        local args = [
            '-target=distributor',
            '-server.http-listen-port=80',
            '-distributor.shard-by-all-labels=true',
            '-consul.hostname=' + consul_uri,
        ];

        # Ports
        local distributorPorts = {
            http: {
                containerPort: 80,
            },
        };

        # Container
        local distributorContainer = kube.Container(name) + {
            args+: args + extraArgs,
            image: $._config.distributor.image,
            ports_: distributorPorts,
            resources+: $._config.distributor.resources,
        };

        # Pod
        local distributorPod = kube.PodSpec + {
            containers_: {
                distributor: distributorContainer,
            },
        };

        kube.Deployment(name) + {
            metadata+: {
                labels: labels,
                namespace: $._config.namespace,
            },
            spec+: {
                template+: {
                    spec: distributorPod,
                    metadata+: {
                        labels: labels,
                    },
                },
            },
        },

    distributor_service:
        local name = $._config.distributor.name;
        local labels = $._config.distributor.labels;

        kube.Service(name) + {
            target_pod: $.distributor_deployment.spec.template,
            metadata+: {
                labels: labels,
                namespace: $._config.namespace,
            },
        },
}
