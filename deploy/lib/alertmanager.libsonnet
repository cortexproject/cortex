local kube = import 'kube-libsonnet/kube.libsonnet';

{
    alertmanager_deployment:
        local name = $._config.alertmanager.name;
        local labels = $._config.alertmanager.labels;

        # Arguments
        local extraArgs = $._config.alertmanager.extraArgs;
        local configs_uri = 'http://' + $._config.configs.name + '.' + $._config.namespace + '.svc.cluster.local:80';
        local args = [
            '-target=alertmanager',
            '-server.http-listen-port=80',
            '-alertmanager.configs.url=' + configs_uri,
            '-alertmanager.web.external-url=/api/prom/alertmanager',
        ];

        local alertmanagerPorts = {
            http: {
                containerPort: 80,
            },
        };

        # Container
        local alertmanagerContainer = kube.Container(name) + {
            image: $._config.alertmanager.image,
            args+: args + extraArgs,
            ports_: alertmanagerPorts,
            resources+: $._config.alertmanager.resources,
        };

        local alertmanagerPod = kube.PodSpec + {
            containers_: {
                alertmanager: alertmanagerContainer,
            },
        };

        kube.Deployment(name) + {
            metadata+: {
                namespace: $._config.namespace,
                labels: labels,
            },
            spec+: {
                template+: { 
                    spec: alertmanagerPod,
                    metadata+: {
                        labels: labels,
                    },
                },
            },
        },

    alertmanager_service:
        local name = $._config.alertmanager.name;
        local labels = $._config.alertmanager.labels;
    
        kube.Service(name) + {
            target_pod: $.alertmanager_deployment.spec.template,
            metadata+: {
                labels: labels,
                namespace: $._config.namespace,
            },
        },
}
