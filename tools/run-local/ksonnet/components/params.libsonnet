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
      image: "prom/prometheus:v2.2.1",
      name: "prometheus",
      replicas: 1,
      servicePort: 80,
      type: "ClusterIP",
    },
    "cortex-lite": {
      containerPort: 80,
      image: "quay.io/weaveworks/cortex-lite:latest",
      name: "cortex-lite",
      replicas: 1,
      servicePort: 80,
      type: "ClusterIP",
    },
  },
}
