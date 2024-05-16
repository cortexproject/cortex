---
title: "Getting Started"
linkTitle: "Getting Started"
weight: 1
no_section_index_title: true
slug: "getting-started"
---

# Getting Started with Cortex

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
# Create a bucket in SeaweedFS
curl -X PUT http://localhost:8333/cortex-bucket
```

#### Configure Grafana

1. Log into the Grafana instance at [http://localhost:3000](http://localhost:3000)
    * login credentials are `username: admin` and `password: admin`
    * There may be an additional screen on setting a new password. This can be skipped and is optional
1. Navigate to the `Data Sources` page
    * Look for a gear icon on the left sidebar and select `Data Sources`
1. Add a new Prometheus Data Source
    * Use `http://cortex:9009/api/prom` as the URL
    * Click `Save & Test`
1. Go to `Metrics Explore` to query metrics
    * Look for a compass icon on the left sidebar
    * Click `Metrics` for a dropdown list of all the available metrics

If everything is working correctly, then the metrics seen in Grafana were successfully sent from Prometheus to Cortex
via remote_write!

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
$ curl -X PUT http://localhost:8333/cortex-bucket
```

#### Setup Cortex

```sh
# Deploy Cortex using the provided values file which configures
# - blocks storage to use the seaweedfs service
$ helm install --version=2.3.0  --namespace cortex cortex cortex-helm/cortex -f cortex-values.yaml
```

#### Setup Prometheus

```sh
# Deploy Prometheus to scrape metrics in the cluster and send them, via remote_write, to Cortex.
$ helm install --version=25.20.1 --namespace cortex prometheus prometheus-community/prometheus -f prometheus-values.yaml
```

#### Setup Grafana

```sh
# Deploy Grafana to visualize the metrics that were sent to Cortex.
$ helm install --version=7.3.9 --namespace cortex grafana grafana/grafana
```

#### Configure Grafana

```sh
# Get your 'admin' user password
kubectl get secret --namespace cortex grafana -o jsonpath="{.data.admin-password}" | base64 --decode ; echo
```

```sh
# Port-forward to Grafana to visualize
kubectl --namespace cortex port-forward deploy/cortex 3000
```

1. Log into the Grafana instance at [http://localhost:3000](http://localhost:3000)
1. Use the username `admin` and the password from the Kubernetes secret
1. Navigate to the [Data Sources](http://localhost:3000/connections/datasources) page
1. Add a new Prometheus Data Source
1. Use `http://cortex-nginx/api/prom` as the URL
1. Click `Save & Test`
1. Go to [Explore](http://localhost:3000/explore) to query metrics
1. Click `Metrics` for a dropdown list of all the available metrics

If everything is working correctly, then the metrics seen in Grafana were successfully sent from Prometheus to Cortex
via remote_write!

### Clean up

```sh
$ kind delete cluster
```
