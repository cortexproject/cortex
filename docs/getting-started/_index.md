---
title: "Getting Started"
linkTitle: "Getting Started"
weight: 1
no_section_index_title: true
slug: "getting-started"
---

Cortex is a powerful platform software that can be run in two modes: as a single binary or as multiple
independent [microservices](../architecture.md).

There are two guides in this section:

1. [Single Binary Mode with Docker Compose](#single-binary-mode)
2. [Microservice Mode with KIND](#microservice-mode)

The single binary mode is useful for testing and development, while the microservice mode is useful for production.

Both guides will help you get started with Cortex using [blocks storage](../blocks-storage/_index.md).

## Single Binary Mode

This guide will help you get started with Cortex in single-binary mode using
[blocks storage](../blocks-storage/_index.md).

### Prerequisites

Cortex can be configured to use local storage or cloud storage (S3, GCS, and Azure). It can also utilize external
Memcached and Redis instances for caching. This guide will focus on running Cortex as a single process with no
dependencies.

* [Docker Compose](https://docs.docker.com/compose/install/)

### Running Cortex as a Single Instance

For simplicity, we'll start by running Cortex as a single process with no dependencies. This mode is not recommended or
intended for production environments or production use.

This example uses [Docker Compose](https://docs.docker.com/compose/) to set up:

1. An instance of [SeaweedFS](https://github.com/seaweedfs/seaweedfs/) for S3-compatible object storage
1. An instance of [Cortex](https://cortexmetrics.io/) to receive metrics.
1. An instance of [Prometheus](https://prometheus.io/) to send metrics to Cortex.
1. An instance of [Perses](https://perses.dev) for latest trend on dashboarding
1. An instance of [Grafana](https://grafana.com/) for legacy dashboarding

#### Instructions

```sh
$ git clone https://github.com/cortexproject/cortex.git
$ cd cortex/docs/getting-started
```

##### Start the services

```sh
$ docker compose up -d
```

We can now access the following services:

* [Cortex](http://localhost:9009)
* [Prometheus](http://localhost:9090)
* [Grafana](http://localhost:3000)
* [SeaweedFS](http://localhost:8333)

If everything is working correctly, Prometheus should be sending metrics that it is scraping to Cortex. Prometheus is
configured to send metrics to Cortex via `remote_write`. Check out the `prometheus-config.yaml` file to see
how this is configured.

#### Configure Cortex Recording Rules and Alerting Rules

We can configure Cortex with [cortextool](https://github.com/cortexproject/cortex-tools/) to load [recording rules](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/) and [alerting rules](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/). This is optional, but it is helpful to see how Cortex can be configured to manage rules and alerts.

```sh
# Configure recording rules for the Cortex tenant (optional)
$ docker run --network host -v "$(pwd):/workspace" -w /workspace quay.io/cortexproject/cortex-tools:v0.17.0 rules sync rules.yaml alerts.yaml --id cortex --address http://localhost:9009
```

#### Configure Cortex Alertmanager

Cortex also comes with a multi-tenant Alertmanager. Let's load configuration for it to be able to view them in Grafana.

```sh
# Configure alertmanager for the Cortex tenant
$ docker run --network host -v "$(pwd):/workspace" -w /workspace quay.io/cortexproject/cortex-tools:v0.17.0 alertmanager load alertmanager-config.yaml --id cortex --address http://localhost:9009
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
$ docker compose down -v
```

## Microservice Mode

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
$ git clone https://github.com/cortexproject/cortex.git
$ cd cortex/docs/getting-started
```

#### Configure SeaweedFS (S3)

```sh
# Create a namespace
$ kubectl create namespace cortex
```

```sh
# We can emulate S3 with SeaweedFS
$ kubectl --namespace cortex apply -f seaweedfs.yaml --wait --timeout=5m
```

```sh
# Wait for SeaweedFS to be ready
$ sleep 5
$ kubectl --namespace cortex wait --for=condition=ready pod -l app=seaweedfs --timeout=5m
```

```sh
# Port-forward to SeaweedFS to create a bucket
$ kubectl --namespace cortex port-forward svc/seaweedfs 8333 &
```

```sh
# Create buckets in SeaweedFS
$ for bucket in cortex-blocks cortex-ruler cortex-alertmanager; do
  curl --aws-sigv4 "aws:amz:local:seaweedfs" --user "any:any" -X PUT http://localhost:8333/$bucket
done
```

#### Setup Cortex

```sh
# Deploy Cortex using the provided values file which configures
# - blocks storage to use the seaweedfs service
$ helm upgrade --install --version=2.4.0  --namespace cortex cortex cortex-helm/cortex -f cortex-values.yaml --wait
```

#### Setup Prometheus

```sh
# Deploy Prometheus to scrape metrics in the cluster and send them, via remote_write, to Cortex.
$ helm upgrade --install --version=25.20.1 --namespace cortex prometheus prometheus-community/prometheus -f prometheus-values.yaml --wait
```

If everything is working correctly, Prometheus should be sending metrics that it is scraping to Cortex. Prometheus is
configured to send metrics to Cortex via `remote_write`. Check out the `prometheus-config.yaml` file to see
how this is configured.

#### Setup Grafana

```sh
# Deploy Grafana to visualize the metrics that were sent to Cortex.
$ helm upgrade --install --version=7.3.9 --namespace cortex grafana grafana/grafana -f grafana-values.yaml --wait
```

```sh
# Create dashboards for Cortex
$ for dashboard in $(ls dashboards); do
  basename=$(basename -s .json $dashboard)
  cmname=grafana-dashboard-$basename
  kubectl create --namespace cortex configmap $cmname --from-file=$dashboard=dashboards/$dashboard --save-config=true -o yaml --dry-run=client | kubectl apply -f -
  kubectl patch --namespace cortex configmap $cmname -p '{"metadata":{"labels":{"grafana_dashboard":""}}}'
done

```

```sh
# Port-forward to Grafana to visualize
$ kubectl --namespace cortex port-forward deploy/grafana 3000 &
```

View the dashboards in [Grafana](http://localhost:3000/dashboards?tag=cortex).

#### Configure Cortex Recording Rules and Alerting Rules (Optional)

We can configure Cortex with [cortextool](https://github.com/cortexproject/cortex-tools/) to load [recording rules](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/) and [alerting rules](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/). This is optional, but it is helpful to see how Cortex can be configured to manage rules and alerts.

```sh
# Port forward to the alertmanager to configure recording rules and alerts
$ kubectl --namespace cortex port-forward svc/cortex-nginx 8080:80 &
```

```sh
# Configure recording rules for the cortex tenant
$ cortextool rules sync rules.yaml alerts.yaml --id cortex --address http://localhost:8080
```

#### Configure Cortex Alertmanager (Optional)

Cortex also comes with a multi-tenant Alertmanager. Let's load configuration for it to be able to view them in Grafana.

```sh
# Configure alertmanager for the cortex tenant
$ cortextool alertmanager load alertmanager-config.yaml --id cortex --address http://localhost:8080
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

[Cortex](http://localhost:9009) - Administrative interface for Cortex

```sh
# Port forward to the ingester to see the administrative interface for Cortex
$ kubectl --namespace cortex port-forward deploy/cortex-ingester 9009:8080 &
```

- Try shutting down the ingester, and see how it affects metric ingestion.
- Restart Cortex to bring the ingester back online, and see how Prometheus catches up.
- Does it affect the querying of metrics in Grafana?

[Prometheus](http://localhost:9090) - Prometheus instance that is sending metrics to Cortex

```sh
# Port forward to Prometheus to see the metrics that are being scraped
$ kubectl --namespace cortex port-forward deploy/prometheus-server 9090 &
```
- Try querying the metrics in Prometheus.
- Are they the same as what you see in Cortex?

[Grafana](http://localhost:3000) - Grafana instance that is visualizing the metrics.

```sh
# Port forward to Grafana to visualize
$ kubectl --namespace cortex port-forward deploy/grafana 3000 &
```

- Try creating a new dashboard and adding a new panel with a query to Cortex.

### Clean up

```sh
# Remove the port-forwards
$ killall kubectl
```

```sh
$ kind delete cluster
```

