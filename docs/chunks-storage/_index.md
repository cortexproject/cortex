---
title: "Chunks Storage (deprecated and pending removal)"
linkTitle: "Chunks Storage (deprecated and pending removal)"
weight: 4
menu:
---

## Deprecation Notice

The chunks storage is deprecated since v1.10.0. You're encouraged to use the [blocks storage](../blocks-storage/_index.md).

Chunks storage is scheduled to be removed in release 1.14.0


## Introduction

The chunks storage is a Cortex storage engine which stores each single time series into a separate object called _chunk_. Each chunk contains the samples for a given period (defaults to 12 hours). Chunks are then indexed by time range and labels, in order to provide a fast lookup across many (over millions) chunks. For this reason, the Cortex chunks storage requires two backend storages: a key-value store for the index and an object store for the chunks.

The supported backends for the **index store** are:

* [Amazon DynamoDB](https://aws.amazon.com/dynamodb)
* [Google Bigtable](https://cloud.google.com/bigtable)
* [Apache Cassandra](https://cassandra.apache.org)

The supported backends for the **chunks store** are:

* [Amazon DynamoDB](https://aws.amazon.com/dynamodb)
* [Google Bigtable](https://cloud.google.com/bigtable)
* [Apache Cassandra](https://cassandra.apache.org)
* [Amazon S3](https://aws.amazon.com/s3)
* [Google Cloud Storage](https://cloud.google.com/storage/)
* [Microsoft Azure Storage](https://azure.microsoft.com/en-us/services/storage/)

## Storage versioning

The chunks storage is based on a custom data format. The **chunks and index format are versioned**: this allows Cortex operators to upgrade the cluster to take advantage of new features and improvements. This strategy enables changes in the storage format without requiring any downtime or complex procedures to rewrite the stored data. A set of schemas are used to map the version while reading and writing time series belonging to a specific period of time.

The current schema recommendation is the **v9 schema** for most use cases and **v10 schema** if you expect to have very high cardinality metrics (v11 is still experimental). For more information about the schema, please check out the [schema configuration](schema-config.md).

## Guides

The following step-by-step guides can help you setting up Cortex running with the chunks storage:

- [Getting started with Cortex chunks storage](./chunks-storage-getting-started.md)
- [Running Cortex chunks storage in Production](./running-chunks-storage-in-production.md)
- [Running Cortex chunks storage with Cassandra](./running-chunks-storage-with-cassandra.md)
