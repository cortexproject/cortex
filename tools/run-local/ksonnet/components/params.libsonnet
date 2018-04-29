{
  global: {
    // User-defined global parameters; accessible to all component and environments, Ex:
    // replicas: 4,
  },
  components: {
    // Component-level parameters, defined initially from 'ks prototype use ...'
    // Each object below should correspond to a component in the components/ directory
    prometheus: {
      containerPort: 80,
      image: 'prom/prometheus:v2.2.1',
      name: 'prometheus',
      replicas: 1,
      servicePort: 80,
      type: 'ClusterIP',
    },
    'cortex-lite': {
      cortex_args: {
        '-chunk.storage-client': 'gcp',
        '-bigtable.project': 'genuine-cat-188717',
        '-bigtable.instance': 'cortex',
        '-dynamodb.periodic-table.prefix': 'cortex_hourly_',
        '-dynamodb.periodic-table.start': '2018-02-06',
        '-dynamodb.chunk-table.from': '2017-12-01',
        '-dynamodb.v4-schema-from': '2017-12-01',
        '-dynamodb.v5-schema-from': '2017-12-01',
        '-dynamodb.v6-schema-from': '2017-12-01',
        '-dynamodb.v7-schema-from': '2017-12-01',
        '-dynamodb.v8-schema-from': '2017-12-01',
      },
      cortex_flags: [
        '-dynamodb.use-periodic-tables',
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
