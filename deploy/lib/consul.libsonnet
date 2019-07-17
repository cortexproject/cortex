local kube = import 'kube-libsonnet/kube.libsonnet';

{
    consul_deployment:
        local name = $._config.consul.name;
        local labels = $._config.consul.labels;

        # Arguments
        local args = [
            'agent',
            '-ui',
            '-server',
            '-client=0.0.0.0',
            '-bootstrap',
        ];

        # Environment Variables
        local env = {
            CHECKPOINT_DISABLE: '1',
        };

        # Ports
        local consulPorts = {
            http: { containerPort: 8500 },
            server: { containerPort: 8300 },
            serf: { containerPort: 8301 },
            client: { containerPort: 8400 },
        };

        # Container
        local consulContainer = kube.Container(name) + {
            image: $._config.consul.image,
            ports_: consulPorts,
            args+: args,
            env_+: env,
            resources+: $._config.consul.resources,
        };

        local consulPod = kube.PodSpec + {
            containers_: {
                consul: consulContainer,
            },
        };
        
        kube.Deployment(name) + {
            metadata+: {
                labels: labels,
                namespace: $._config.namespace,
            },
            spec+: {
                replicas: $._config.consul.replicas,
                template+: {
                    spec: consulPod,
                    metadata: {
                        labels: labels,
                    },
                },
            },
        },
    consul_service:
        local name = $._config.consul.name;
        local labels = $._config.consul.labels;

        kube.Service(name) + {
            target_pod: $.consul_deployment.spec.template,
            metadata+: {
                labels: labels,
                namespace: $._config.namespace,
            },
            // kube.libsonnet will use the first containerPort found
            // We want to ensure that the 'http' service is exposed
            spec+: {
                ports: [
                    {
                        port: 8500,
                        targetPort: 8500,
                    },
                ],
            },
        },
}
