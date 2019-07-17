local kube = import 'kube-libsonnet/kube.libsonnet';

{
    memcached_statefulset:
        # memcached container
        local memcachedContainer = kube.Container('memcached') + {
            image: $._config.memcached.image,
            ports_: {
                memcache: { containerPort: 11211 },
            },
            args: ['-m ' + $._config.memcached.maxItemMemory] +
                $._config.memcached.extraArgs,
            livenessProbe: {
                initialDelaySeconds: 30,
                periodSeconds: 1,
                tcpSocket:{
                    port: 'memcache',
                },
            },
            readinessProbe: {
                initialDelaySeconds: 5,
                periodSeconds: 1,
                tcpSocket: {
                    port: 'memcache',
                },
            },
        };

        # memcached-exporter container
        local exporterContainer = kube.Container('memcached-exporter') + {
            image: $._config.memcached.exporterImage,
            ports_: {
                metrics: { containerPort: 9150 },
            },
        };

        local memcachedPod = kube.PodSpec + {
            default_container:: "memcached",
            containers_:: {
                memcached: memcachedContainer,
                [if $._config.memcached.metrics then 'memcachedExporter']: exporterContainer,
            },
        };

        kube.StatefulSet($._config.memcached.name) + {
            metadata+: {
                namespace: $._config.namespace,
                labels: $._config.memcached.labels,
            },
            spec+: {
                template+: {
                    spec+: memcachedPod,
                },
            },
        },

    memcached_service:
        local memcachedPort = {
            name: 'memcached',
            port: 11211,
            targetPort: 11211,
        };
        local exporterPort = {
            name: 'metrics',
            port: 9150,
            targetPort: 9150,
        };
        local servicePorts = [memcachedPort] + 
            if $._config.memcached.metrics then [exporterPort] else [];

        kube.Service($._config.memcached.name) + {
            target_pod:: $.memcached_statefulset.spec.template,
            metadata+: {
                labels: $._config.memcached.labels,
                namespace: $._config.namespace,
            },
            spec+: {
                clusterIP: "None",
                ports: servicePorts,
            },
        },
}