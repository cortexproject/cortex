local kube = import 'kube-libsonnet/kube.libsonnet';

{
    prometheus_configmap: 
        kube.ConfigMap('prometheus-config') + {
            metadata+: {
                namespace: $._config.namespace,
            },
            data: {
                'prometheus.yaml': $._config.prometheus.configuration,
            },
        },

    prometheus_clusterrole:
        kube.ClusterRole($._config.prometheus.name) + {
            rules+: [
                {
                    apiGroups: [''],
                    resources: [
                        'pods'
                    ],
                    verbs: ['get', 'list', 'watch'],
                },
                {
                    apiGroups: [''],
                    resources: ['configmaps'],
                    verbs: ['get'],
                },
                {
                    nonResourceURLs: ['/metrics'],
                    verbs: ['get'],
                },
            ],
        },

    prometheus_serviceaccount:
        kube.ServiceAccount($._config.prometheus.name) + {
            metadata+: {
                namespace: $._config.namespace,
            },
        },

    prometheus_clusterrolebinding:
        kube.ClusterRoleBinding($._config.prometheus.name) + {
            subjects_: [$.prometheus_serviceaccount],
            roleRef_: $.prometheus_clusterrole,
        },

    prometheus_deployment:
        local name = $._config.prometheus.name;
        local labels = $._config.prometheus.labels;

        # Arguments
        local args = [
            '--config.file=/etc/prometheus/prometheus.yaml',
            '--storage.tsdb.path=/data',
        ];

        # Config Volume
        local prometheusConfigVolume = kube.ConfigMapVolume($.prometheus_configmap);
        local prometheusConfigVolumeMount = {
            mountPath: '/etc/prometheus',
            readOnly: true,
        };

        # Storage Volume
        local storageVolume = kube.EmptyDirVolume();
        local storageVolumeMount = {
            mountPath: '/data',
            readOnly: false,
        };

        # Ports
        local prometheusPorts = {
            http: {
                containerPort: 80,
            },
        };

        # Container
        local prometheusContainer = kube.Container(name) + {
            args+: args,
            image: $._config.prometheus.image,
            ports_: prometheusPorts,
            resources+: $._config.prometheus.resources,
            volumeMounts_: {
                config_volume: prometheusConfigVolumeMount + { readOnly: true },
                storage_volume: storageVolumeMount,
            },
        };

        local prometheusPod = kube.PodSpec + {
            containers_: {
                prometheus: prometheusContainer,
            },
            volumes_: {
                config_volume: prometheusConfigVolume,
                storage_volume: storageVolume,
            },
            serviceAccountName: name,
        };

        kube.Deployment(name) + {
            metadata+: {
                namespace: $._config.namespace,
                labels: labels,
            },
            spec+: {
                template+: {
                    spec: prometheusPod,
                    metadata+: {
                        labels: labels,
                    },
                },
            },
        },

    prometheus_service:
        local name = $._config.prometheus.name;
        local labels = $._config.prometheus.labels;

        kube.Service(name) + {
            target_pod: $.prometheus_deployment.spec.template,
            metadata+: {
                labels: labels,
                namespace: $._config.namespace,
            },
        },
}
