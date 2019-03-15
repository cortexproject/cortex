# Setting up a Cortex cluster

## Prerequisites

1. You need somewhere to run processes.  In this document we will run
them within a Kubernetes cluster, which makes some aspects easier but
is not essential.

2. Pick a storage back-end. Primary choices are:
 - DynamoDB if on Amazon
 - Bigtable if on Google Cloud
 - Cassandra if on-prem

3. Decide your index table naming, period, and schema.
A simple test install can use a single table, but for production use
we recommend periodic tables.  The default period is weekly.
Use a recent schema, maybe one before the latest if the latest is very new.
At the time of writing the recommended schema is v9.

4. Pick an image tag. At the time of writing there are no stamped
releases, so just pick a recent git tag and use the images
corresponding to that. The official images are hosted at quay.io, and
you can see a list of tags, for instance at
https://quay.io/cortexproject/table-manager

## Sizing

TBD.

Sizing is a function of the number of timeseries that will be active
at any one time, and the rate at which timeseries come and go. These
factors are much more important than the total rate of input of
samples, because they get compressed down.

## Identification and authorization of clients

Cortex is a multi-tenant system, but how you ensure that each tenant
is securely identified so that others cannot view their data must be
solved separately. Generally people use a front-end proxy to forward
calls and attach the header that Cortex expects on each API call.

In our example config we use Nginx as a proxy, with one hard-coded tenant.

## Setting up the cluster

Run the table manager to create tables.
