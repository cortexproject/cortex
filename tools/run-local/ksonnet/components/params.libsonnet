{
  global: {},
  components: {
    prometheus: {
      containerPort: 80,
      image: 'prom/prometheus:v2.2.1',
      name: 'prometheus',
      replicas: 1,
      servicePort: 80,
      type: 'ClusterIP',
    },
    'cortex-lite': {
      cortex_args: {},
      cortex_flags: [
        '-dynamodb.use-periodic-tables',
      ],
      containerPort: 80,
      image: 'quay.io/weaveworks/cortex-lite:latest',
      name: 'cortex-lite',
      replicas: 1,
      servicePort: 80,
      type: 'ClusterIP',
    },
  },
}
