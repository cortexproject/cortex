## Cortex Grafana Dashboards

This folder contains example dashboards for Grafana 5 that can be used to monitor the Cortex read and write paths. These are built using [Jsonnet](http://jsonnet.org/) and [Grafonnet](https://github.com/grafana/grafonnet-lib) for some of the base templates.

The backend is abstracted out into its own libsonnet library to allow for easy swapping of backend metrics when generating the dashboards. See `bigtable.libsonnet` for an example backend metric mixins.

To use, Grafonnet needs to be installed locally; see [their docs](https://github.com/grafana/grafonnet-lib#install-grafonnet) for help. Then run Jsonnet to build the dashboards into json which can be imported into grafana.

```
jsonnet -J <grafonnet-path> -o cortex-read.json cortex-read.jsonnet
jsonnet -J <grafonnet-path> -o cortex-write.json cortex-write.jsonnet
```
