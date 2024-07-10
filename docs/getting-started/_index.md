---
title: "Getting Started"
linkTitle: "Getting Started"
weight: 1
no_section_index_title: true
slug: "getting-started"
---

Cortex is a powerful platform software that can be run in two modes: as a single binary or as multiple
independent [microservices](../architecture.md).
This guide will help you get started with Cortex in single-binary mode using
[blocks storage](../blocks-storage/_index.md).

## Prerequisites

Cortex can be configured to use local storage or cloud storage (S3, GCS, and Azure). It can also utilize external
Memcached and Redis instances for caching. This guide will focus on running Cortex as a single process with no
dependencies.

* [Docker Compose](https://docs.docker.com/compose/install/)

## Running Cortex as a Single Instance

For simplicity, we'll start by running Cortex as a single process with no dependencies. This mode is not recommended or
intended for production environments or production use.

This example uses [Docker Compose](https://docs.docker.com/compose/) to set up:

1. An instance of [SeaweedFS](https://github.com/seaweedfs/seaweedfs/) for S3-compatible object storage
1. An instance of [Cortex](https://cortexmetrics.io/) to receive metrics
1. An instance of [Prometheus](https://prometheus.io/) to send metrics to Cortex
1. An instance of [Grafana](https://grafana.com/) to visualize the metrics

### Instructions

#### Start the services

```sh
$ cd docs/getting-started
$ docker-compose up -d --wait
```

We can now access the following services:

* [Cortex](http://localhost:9009)
* [Prometheus](http://localhost:9090)
* [Grafana](http://localhost:3000)
* [SeaweedFS](http://localhost:8333)

If everything is working correctly, Prometheus should be sending metrics that it is scraping to Cortex. Prometheus is
configured to send metrics to Cortex via `remote_write`. Check out the `prometheus-config.yaml` file to see
how this is configured.

#### Configure SeaweedFS (S3)

```sh
# Create buckets in SeaweedFS
$ curl --aws-sigv4 "aws:amz:local:seaweedfs" --user "any:any" -X PUT http://localhost:8333/cortex-blocks
$ curl --aws-sigv4 "aws:amz:local:seaweedfs" --user "any:any" -X PUT http://localhost:8333/cortex-ruler
$ curl --aws-sigv4 "aws:amz:local:seaweedfs" --user "any:any" -X PUT http://localhost:8333/cortex-alertmanager
```

#### Configure Cortex Recording Rules and Alerting Rules

We can configure Cortex with [cortextool](https://github.com/cortexproject/cortex-tools/) to load [recording rules](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/) and [alerting rules](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/). This is optional, but it is helpful to see how Cortex can be configured to manage rules and alerts.

```sh
# Configure recording rules for the cortex tenant (optional)
$ docker run --network host -v $(pwd):/workspace -w /workspace quay.io/cortexproject/cortex-tools:v0.17.0 rules sync rules.yaml alerts.yaml --id cortex --address http://localhost:9009
```

#### Configure Cortex Alertmanager

Cortex also comes with a multi-tenant Alertmanager. Let's load configuration for it to be able to view them in Grafana.

```sh
# Configure alertmanager for the cortex tenant
$ docker run --network host -v $(pwd):/workspace -w /workspace quay.io/cortexproject/cortex-tools:v0.17.0 alertmanager load alertmanager-config.yaml --id cortex --address http://localhost:9009
```

You can configure Alertmanager in [Grafana as well](http://localhost:3000/alerting/notifications?search=&alertmanager=Cortex%20Alertmanager).

There's a list of recording rules and alerts that should be visible in Grafana [here](http://localhost:3000/alerting/list?view=list&search=datasource:Cortex).

#### Explore

Grafana is configured to use Cortex as a data source. Grafana is also configured with [Cortex Dashboards](http://localhost:3000/dashboards?tag=cortex) to understand the state of the Cortex instance. The dashboards are generated from the cortex-jsonnet repository. There is a Makefile in the repository that can be used to update the dashboards.

```sh
# Update the dashboards (optional)
$ make
```

If everything is working correctly, then the metrics seen in Grafana were successfully sent from Prometheus to Cortex
via `remote_write`!

Other things to explore:

- [Cortex](http://localhost:9009) - Administrative interface for Cortex
   - Try shutting down the ingester, and see how it affects metric ingestion.
   - Restart Cortex to bring the ingester back online, and see how Prometheus catches up.
   - Does it affect the querying of metrics in Grafana?
- [Prometheus](http://localhost:9090) - Prometheus instance that is sending metrics to Cortex
   - Try querying the metrics in Prometheus.
   - Are they the same as what you see in Cortex?
- [Grafana](http://localhost:3000) - Grafana instance that is visualizing the metrics.
   - Try creating a new dashboard and adding a new panel with a query to Cortex.

### Clean up

```sh
$ docker-compose down
```

## Running Cortex in microservice mode

Now that you have Cortex running as a single instance, let's explore how to run Cortex in microservice mode.

### Prerequisites

* [Kind](https://kind.sigs.k8s.io)
* [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
* [Helm](https://helm.sh/docs/intro/install/)

This example uses [Kind](https://kind.sigs.k8s.io) to set up:

1. A Kubernetes cluster
1. An instance of [SeaweedFS](https://github.com/seaweedfs/seaweedfs/) for S3-compatible object storage
1. An instance of [Cortex](https://cortexmetrics.io/) to receive metrics
1. An instance of [Prometheus](https://prometheus.io/) to send metrics to Cortex
1. An instance of [Grafana](https://grafana.com/) to visualize the metrics

### Setup Kind

```sh
$ kind create cluster
```

### Configure Helm

```sh
$ helm repo add cortex-helm https://cortexproject.github.io/cortex-helm-chart
$ helm repo add grafana https://grafana.github.io/helm-charts
$ helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
```

### Instructions

```sh
$ cd docs/getting-started
```

#### Configure SeaweedFS (S3)

```sh
# Create a namespace
$ kubectl create namespace cortex
```

```sh
# We can emulate S3 with SeaweedFS
$ kubectl -n cortex apply -f seaweedfs.yaml
```

```sh
# Port-forward to SeaweedFS to create a bucket
$ kubectl -n cortex port-forward svc/seaweedfs 8333
```

```shell
# Create a bucket
$ curl --aws-sigv4 "aws:amz:local:seaweedfs" --user "any:any" -X PUT http://localhost:8333/cortex-bucket
```

#### Setup Cortex

```sh
# Deploy Cortex using the provided values file which configures
# - blocks storage to use the seaweedfs service
$ helm upgrade --install --version=2.3.0  --namespace cortex cortex cortex-helm/cortex -f cortex-values.yaml
```

#### Setup Prometheus

```sh
# Deploy Prometheus to scrape metrics in the cluster and send them, via remote_write, to Cortex.
$ helm upgrade --install --version=25.20.1 --namespace cortex prometheus prometheus-community/prometheus -f prometheus-values.yaml
```

#### Setup Grafana

```sh
# Deploy Grafana to visualize the metrics that were sent to Cortex.
$ helm upgrade --install --version=7.3.9 --namespace cortex grafana grafana/grafana -f grafana-values.yaml
```

#### Explore

```sh
# Port-forward to Grafana to visualize
kubectl --namespace cortex port-forward deploy/grafana 3000
```

Grafana is configured to use Cortex as a data source. You can explore the data source in Grafana and query metrics. For example, this [explore](http://localhost:3000/explore?schemaVersion=1&panes=%7B%22au0%22:%7B%22datasource%22:%22P6693426190CB2316%22,%22queries%22:%5B%7B%22refId%22:%22A%22,%22expr%22:%22rate%28prometheus_remote_storage_samples_total%5B$__rate_interval%5D%29%22,%22range%22:true,%22instant%22:true,%22datasource%22:%7B%22type%22:%22prometheus%22,%22uid%22:%22P6693426190CB2316%22%7D,%22editorMode%22:%22builder%22,%22legendFormat%22:%22__auto%22,%22useBackend%22:false,%22disableTextWrap%22:false,%22fullMetaSearch%22:false,%22includeNullMetadata%22:false%7D%5D,%22range%22:%7B%22from%22:%22now-1h%22,%22to%22:%22now%22%7D%7D%7D&orgId=1) page is showing the rate of samples being sent to Cortex.


If everything is working correctly, then the metrics seen in Grafana were successfully sent from Prometheus to Cortex
via remote_write!

Other things to explore:

```sh
# Port forward to the ingester to see the administrative interface for Cortex:
$ kubectl --namespace cortex port-forward deploy/cortex-ingester 8080
```

- [Cortex Ingester](http://localhost:8080)
  - Try shutting down the [ingester](http://localhost:8080/ingester/shutdown) and see how it affects metric ingestion.
  - Restart ingester pod to bring the ingester back online, and see if Prometheus affected.
  - Does it affect the querying of metrics in Grafana? How many ingesters must be offline before it affects querying?


```sh
# Port forward to Prometheus to see the metrics that are being scraped:
$ kubectl --namespace cortex port-forward deploy/prometheus-server 9090
```

- [Prometheus](http://localhost:9090) - Prometheus instance that is sending metrics to Cortex
  - Try querying the metrics in Prometheus.
  - Are they the same as what you see in Cortex?

```sh
# Port forward to Prometheus to see the metrics that are being scraped:
$ kubectl --namespace cortex port-forward deploy/grafana 3000
```

- [Grafana](http://localhost:3000) - Grafana instance that is visualizing the metrics.
  - Try creating a new dashboard and adding a new panel with a query to Cortex.

### Clean up

```sh
$ kind delete cluster
```
