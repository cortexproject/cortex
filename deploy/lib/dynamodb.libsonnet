local kube = import 'kube-libsonnet/kube.libsonnet';

{
    dynamodb_deployment:
        local name = $._config.dynamodb.name;
        local labels = $._config.dynamodb.labels;

        # Ports
        local dynamodbPorts = {
            http: {
                containerPort: 8000,
            },
        };

        # Container
        local dynamodbContainer = kube.Container(name) + {
            image: $._config.dynamodb.image,
            resources+: $._config.dynamodb.resources,
            ports_: dynamodbPorts,
        };

        local dynamodbPod = kube.PodSpec + {
            containers_: {
                dynamodb: dynamodbContainer,
            },
        };

        kube.Deployment(name) + {
            metadata+: {
                namespace: $._config.namespace,
                labels: labels,
            },
            spec+: {
                template+: {
                    spec: dynamodbPod,
                    metadata+: {
                        labels: labels,
                    },
                },
            },
        },

    dynamodb_service:
        local name = $._config.dynamodb.name;
        local labels = $._config.dynamodb.labels;

        kube.Service(name) + {
            target_pod: $.dynamodb_deployment.spec.template,
            metadata+: {
                labels: labels,
                namespace: $._config.namespace,
            },
        },

}
