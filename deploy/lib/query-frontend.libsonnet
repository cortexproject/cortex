local kube = import 'kube-libsonnet/kube.libsonnet';

{
    query_frontend_deployment:
        local name = $._config.queryFrontend.name;
        local labels = $._config.queryFrontend.labels;
        local extraArgs = $._config.queryFrontend.extraArgs;
        local memcached_uri = $._config.memcached.name + '.' + $._config.namespace + '.svc.cluster.local';

        # Arguments
        local args = [
            '-target=query-frontend',
            '-server.http-listen-port=80',
            '-querier.split-queries-by-day=true',
            '-querier.align-querier-with-step=true',
            '-memcached.hostname=' + memcached_uri,
        ];
        
        # Environment Variables
        local env = $._config.queryFrontend.env;
        local envKVMixin = $._config.queryFrontend.envKVMixin;
        local extraEnv = [{name: key, value: envKVMixin[key]} for key in std.objectFields(envKVMixin)];

        local queryFrontendPorts = {
            http: {
                containerPort: 80,
            },
            grpc: {
                containerPort: 9095,
            },
        };

        local queryFrontendContainer = kube.Container(name) + {
            image: $._config.queryFrontend.image,
            args+: args + extraArgs,
            env: env + extraEnv,
            ports_: queryFrontendPorts,
            resources+: $._config.queryFrontend.resources,
        };

        local queryFrontendPod = kube.PodSpec + {
            containers_: {
                ['query-frontend']: queryFrontendContainer
            },
        };

        kube.Deployment(name) + {
            metadata+: {
                namespace: $._config.namespace,
                labels: labels,
            },
            spec+: {
                replicas: $._config.queryFrontend.replicas,
                template+: {
                    spec: queryFrontendPod,
                    metadata+: {
                        labels: labels,
                    },
                },
            },
        },
    query_frontend_service:
        local name = $._config.queryFrontend.name;
        local labels = $._config.queryFrontend.labels;

        kube.Service(name) + {
            target_pod: $.query_frontend_deployment.spec.template,
            metadata+: {
                labels: labels,
                namespace: $._config.namespace,
            },            
            // kube.libsonnet will use the first containerPort found
            // We want to ensure that the 'http' service is exposed
            spec+: {
                # Use a headless service
                # This allows all queriers to connect to all query-frontends if using more than 1 query-frontend
                clusterIP: 'None',
                ports: [
                    {
                        port: 80,
                        targetPort: 80,
                        name: 'http',
                    },
                    {
                        port: 9095,
                        targetPort: 9095,
                        name: 'grpc',
                    },
                ],
            },
        },
}
