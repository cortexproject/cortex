---
title: "Running Cortex chunks storage in Production"
linkTitle: "Running Cortex chunks storage in Production"
weight: 1
slug: running-chunks-storage-in-production
---

**Warning: the chunks storage is deprecated. You're encouraged to use the [blocks storage](../blocks-storage/_index.md).**

This document builds on the [getting started guide](../getting-started/_index.md) and specifies the steps needed to get Cortex [**chunks storage**](../chunks-storage/_index.md) into production.
Ensure you have completed all the steps in the [getting started guide](../getting-started/_index.md) and read about [the Cortex architecture](../architecture.md) before you start this one.

## 1. Pick a storage backend

The getting started guide uses local chunk storage.
Local chunk storage is experimental and shouldn’t be used in production.

Cortex requires a scalable storage back-end for production systems.
It is recommended you use chunk storage with one of the following back-ends:

* DynamoDB/S3 (see [AWS tips](../chunks-storage/aws-tips.md))
* BigTable/GCS
* Cassandra (see [Running chunks storage on Cassandra](./running-chunks-storage-with-cassandra.md))

Commercial cloud options are DynamoDB/S3 and Bigtable/GCS: the advantage is you don't have to know how to manage them, but the downside is they have specific costs.

Alternatively you can choose Apache Cassandra, which you will have to install and manage.
Cassandra support can also be used with commecial Cassandra-compatible services such as Azure Cosmos DB.

Cortex has an alternative to chunks storage: [blocks storage](../blocks-storage/_index.md). Blocks storage is ready for production use and does not require a separate index store.

## 2. Deploy Query Frontend

The **Query Frontend** is the Cortex component which parallelizes the execution of and caches the results of queries.
The **Query Frontend** is also responsible for retries and multi-tenant QoS.

For the multi-tenant QoS algorithms to work, you should not run more than two **Query Frontends**.
The **Query Frontend** should be deployed behind a load balancer, and should only be sent queries -- writes should go straight to the Distributor component, or to the single-process Cortex.

The **Querier** component (or single-process Cortex) “pulls” queries from the queues in the **Query Frontend**.
**Queriers** discover the **Query Frontend** via DNS.
The **Queriers** should not use the load balancer to access the **Query Frontend**.
In Kubernetes, you should use a separate headless service.

To configure the **Queries** to use the **Query Frontend**, set the following flag:

```sh
  -querier.frontend-address string
    Address of query frontend service.
```

There are other flag you can use to control the behaviour of the frontend - concurrency, retries, etc.
See [Query Frontend configuration](../configuration/arguments.md#query-frontend) for more information.

The **Query Frontend** can run using an in-process cache, but should be configured with an external Memcached for production workloads.
The next section has more details.

## 3. Setup Caching

Correctly configured caching is important for a production-ready Cortex cluster.
Cortex has many opportunities for using caching to accelerate queries and reduce cost.

For more information, see the [Caching in Cortex documentation.](../chunks-storage/caching.md)

## 4. Monitoring and Alerting

Cortex exports metrics in the Prometheus format.
We recommend you install and configure Prometheus server to monitor your Cortex cluster.

We publish a set of Prometheus alerts and Grafana dashboards as the [cortex-mixin](https://github.com/grafana/cortex-jsonnet).
We recommend you use these for any production Cortex cluster.

## 5. Authentication & Multitenancy

If you want to run Cortex as a multi-tenant system, you need to give each
tenant a unique ID - this can be any string.
Managing tenants and allocating IDs must be done outside of Cortex.
See [Authentication and Authorisation](authentication-and-authorisation.md) for more information.

## 6. Handling HA Prometheus Pairs

You should use a pair of Prometheus servers to monitor your targets and send metrics to Cortex.
This allows your monitoring system to survive the failure of one of these Prometheus instances.
Cortex support deduping the samples on ingestion.
For more information on how to configure Cortex and Prometheus to HA pairs, see [Config for sending HA Pairs data to Cortex](ha-pair-handling.md).
