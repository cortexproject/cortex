local kube = import 'kube-libsonnet/kube.libsonnet';

{
    nginx_configmap:
        kube.ConfigMap('nginx-config') + {
            metadata+: {
                namespace: $._config.namespace,
            },
            data: {
                'nginx.conf': $._config.nginx.configuration,
            },
        },

    nginx_deployment:
        local name = $._config.nginx.name;
        local labels = $._config.nginx.labels;

        # Config Volume
        local nginxConfigVolume = kube.ConfigMapVolume($.nginx_configmap);
        local nginxConfigVolumeMount = {
            config_volume: {
                mountPath: '/etc/nginx',
                readOnly: true,
            },
        };

        local nginxPorts = {
            http: {
                containerPort: 80,
            },
        };

        local nginxContainer = kube.Container(name) + {
            image: $._config.nginx.image,
            ports_: nginxPorts,
            resources+: $._config.nginx.resources,
            volumeMounts_: nginxConfigVolumeMount,
        };

        local nginxPod = kube.PodSpec + {
            containers_: {
                nginx: nginxContainer,
            },
            volumes_: {
                config_volume: nginxConfigVolume,
            },
        };

        kube.Deployment(name) + {
            metadata+: {
                namespace: $._config.namespace,
                labels: labels,
            },
            spec+: {
                template+: {
                    spec: nginxPod,
                    metadata+: {
                        labels: labels,
                    },
                },
            },
        },

    nginx_service:
        local name = $._config.nginx.name;
        local labels = $._config.nginx.labels;

        kube.Service(name) + {
            target_pod: $.nginx_deployment.spec.template,
            metadata+: {
                labels: labels,
                namespace: $._config.namespace,
            },
            spec+: {
                ports: [
                    {
                        nodePort: 30080,
                        port: 80,
                        name: 'http',
                    },
                ],
                type: 'NodePort',
            },
        },
}
