{
  grafanaDashboardFolder: 'Cortex',
  grafanaDashboardShards: 4,

  _config+:: {
    storage_engine: ['blocks'],  // TODO: Remove this option, it's not needed

    // Tags for dashboards.
    tags: ['cortex'],

    // If Cortex is deployed as a single binary, set to true to
    // modify the job selectors in the dashboard queries.
    singleBinary: true,

    // These are used by the dashboards and allow for the simultaneous display of
    // microservice and single binary cortex clusters.
    job_names: {
      ingester: '(ingester.*|cortex$)',  // Match also custom and per-zone ingester deployments.
      distributor: '(distributor|cortex$)',
      querier: '(querier.*|cortex$)',  // Match also custom querier deployments.
      ruler: '(ruler|cortex$)',
      query_frontend: '(query-frontend.*|cortex$)',  // Match also custom query-frontend deployments.
      query_scheduler: 'query-scheduler.*',  // Not part of single-binary. Match also custom query-scheduler deployments.
      ring_members: ['compactor', 'distributor', 'ingester.*', 'querier.*', 'ruler', 'store-gateway', 'cortex'],
      store_gateway: '(store-gateway|cortex$)',
      gateway: '(gateway|cortex-gw|cortex-gw-internal)',
      compactor: 'compactor.*',  // Match also custom compactor deployments.
      alertmanager: 'alertmanager',
    },

    // Grouping labels, to uniquely identify and group by {jobs, clusters}
    job_labels: ['cluster', 'namespace', 'job'],
    cluster_labels: ['cluster', 'namespace'],

    cortex_p99_latency_threshold_seconds: 2.5,

    // Whether resources dashboards are enabled (based on cAdvisor metrics).
    resources_dashboards_enabled: false,

    // The label used to differentiate between different application instances (i.e. 'pod' in a kubernetes install).
    per_instance_label: 'pod',

    // Name selectors for different application instances, using the "per_instance_label".
    instance_names: {
      compactor: 'compactor.*',
      alertmanager: 'alertmanager.*',
    },

    // The label used to differentiate between different nodes (i.e. servers).
    per_node_label: 'instance',

    // Whether certain dashboard description headers should be shown
    show_dashboard_descriptions: {
      writes: true,
      reads: true,
    },

    // The routes to exclude from alerts.
    alert_excluded_routes: [],

    // Name of the datasource for which the dashboards should attach to
    dashboard_datasource: 'Cortex',
  },
}
