local kube = import 'kube-libsonnet/kube.libsonnet';

{
    schema_configmap:
        kube.ConfigMap('schema-config') + {
            metadata+: {
                namespace: $._config.namespace,
            },
            data: {
                'schemaConfig.yaml': $._config.schemaConfig,
            },
        },
}
