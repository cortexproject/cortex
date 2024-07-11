(import 'config.libsonnet') +
(import 'groups.libsonnet') +
(import 'dashboards.libsonnet') +
(import 'alerts.libsonnet') +
(import 'recording_rules.libsonnet') + {
  _config+:: {
    dashboard_datasource: 'Cortex',
    singleBinary: true,
  },
}
