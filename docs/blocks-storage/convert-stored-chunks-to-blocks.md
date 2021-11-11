---
title: "Convert long-term storage from chunks to blocks"
linkTitle: "Convert long-term storage from chunks to blocks"
weight: 6
slug: convert-long-term-storage-from-chunks-to-blocks
---

If you have [configured your cluster to write new data to blocks](./migrate-from-chunks-to-blocks.md), there is still a question about old data.
Cortex can query both chunks and the blocks at the same time, but converting old chunks to blocks still has some benefits, like being able to decommission the chunks storage backend and save costs.
This document presents set of tools for doing the conversion.

_[Original design document](https://docs.google.com/document/d/1VI0cgaJmHD0pcrRb3UV04f8szXXGmFKQyqUJnFOcf6Q/edit?usp=sharing) for `blocksconvert` is also available._

## Tools

Cortex provides a tool called `blocksconvert`, which is actually collection of three tools for converting chunks to blocks.

Tools are:

- [**Scanner**](#scanner)<br />
  Scans the chunks index database and produces so-called "plan files", each file being a set of series and chunks for each series. Plan files are uploaded to the same object store bucket where blocks live.
- [**Scheduler**](#scheduler)<br />
  Looks for plan files, and distributes them to builders. Scheduler has global view of overall conversion progress.
- [**Builder**](#builder)<br />
  Asks scheduler for next plan file to work on, fetches chunks, puts them into TSDB block, and uploads the block to the object store. It repeats this process until there are no more plans.
- [**Cleaner**](#cleaner)<br />
  Cleaner asks scheduler for next plan file to work on, but instead of building the block, it actually **REMOVES CHUNKS** and **INDEX ENTRIES** from the Index database.

All tools start HTTP server (see `-server.http*` options) exposing the `/metrics` endpoint.
All tools also start gRPC server (`-server.grpc*` options), but only Scheduler exposes services on it.

### Scanner

Scanner is started by running `blocksconvert -target=scanner`. Scanner requires configuration for accessing Cortex Index:

- `-schema-config-file` – this is standard Cortex schema file.
- `-bigtable.instance`, `-bigtable.project` – options for BigTable access.
- `-dynamodb.url` - for DynamoDB access.  Example `dynamodb://us-east-1/`
- `-blocks-storage.backend` and corresponding `-blocks-storage.*` options for storing plan files.
- `-scanner.output-dir` – specifies local directory for writing plan files to. Finished plan files are deleted after upload to the bucket. List of scanned tables is also kept in this directory, to avoid scanning the same tables multiple times when Scanner is restarted.
- `-scanner.allowed-users` – comma-separated list of Cortex tenants that should have plans generated. If empty, plans for all found users are generated.
- `-scanner.ignore-users-regex` - If plans for all users are generated (`-scanner.allowed-users` is not set), then users matching this non-empty regular expression will be skipped.
- `-scanner.tables-limit` – How many tables should be scanned? By default all tables are scanned, but when testing scanner it may be useful to start with small number of tables first.
- `-scanner.tables` – Comma-separated list of tables to be scanned. Can be used to scan specific tables only. Note that schema is still used to find all tables first, and then this list is consulted to select only specified tables.
- `-scanner.scan-period-start` & `-scanner.scan-period-end` - limit the scan to a particular date range (format like `2020-12-31`)

Scanner will read the Cortex schema file to discover Index tables, and then it will start scanning them from most-recent table first, going back.
For each table, it will fully read the table and generate a plan for each user and day stored in the table.
Plan files are then uploaded to the configured blocks-storage bucket (at the `-blocksconvert.bucket-prefix` location prefix), and local copies are deleted.
After that, scanner continues with the next table until it scans them all or `-scanner.tables-limit` is reached.

Note that even though `blocksconvert` has options for configuring different Index store backends, **it only supports BigTable and DynamoDB at the moment.**

It is expected that only single Scanner process is running.
Scanner does the scanning of multiple table subranges concurrently.

Scanner exposes metrics with `cortex_blocksconvert_scanner_` prefix, eg. total number of scanned index entries of different type, number of open files (scanner doesn't close currently plan files until entire table has been scanned), scanned rows and parsed index entries.

**Scanner only supports schema version v9 on DynamoDB; v9, v10 and v11 on BigTable. Earlier schema versions are currently not supported.**

### Scheduler

Scheduler is started by running `blocksconvert -target=scheduler`. It only needs to be configured with options to access the object store with blocks:

- `-blocks-storage.*` - Blocks storage object store configuration.
- `-scheduler.scan-interval` – How often to scan for plan files and their status.
- `-scheduler.allowed-users` – Comma-separated list of Cortex tenants. If set, only plans for these tenants will be offered to Builders.

It is expected that only single Scheduler process is running. Schedulers consume very little resources.

Scheduler's metrics have `cortex_blocksconvert_scheduler` prefix (number of plans in different states, oldest/newest plan).
Scheduler HTTP server also exposes  `/plans` page that shows currently queued plans, and all plans and their status for all users.

### Builder

Builder asks scheduler for next plan to work on, downloads the plan, builds the block and uploads the block to the blocks storage. It then repeats the process while there are still plans.

Builder is started by `blocksconvert -target=builder`. It needs to be configured with Scheduler endpoint, Cortex schema file, chunk-store specific options and blocks storage to upload blocks to.

- `-builder.scheduler-endpoint` - where to find scheduler, eg. "scheduler:9095"
- `-schema-config-file` - Cortex schema file, used to find out which chunks store to use for given plan
- `-gcs.bucketname` – when using GCS as chunks store (other chunks backend storages, like S3, are supported as well)
- `-blocks-storage.*` - blocks storage configuration
- `-builder.output-dir` - Local directory where Builder keeps the block while it is being built. Once block is uploaded to blocks storage, it is deleted from local directory.

Multiple builders may run at the same time, each builder will receive different plan to work on from scheduler.
Builders are CPU intensive (decoding and merging chunks), and require fast disk IO for writing blocks.

Builders's metrics have `cortex_blocksconvert_builder` prefix, and include total number of fetched chunks and their size, read position of the current plan and plan size, total number of written series and samples, number of chunks that couldn't be downloaded.

### Cleaner

Cleaner is similar to builder in that it asks scheduler for next plan to work on, but instead of building blocks, it actually **REMOVES CHUNKS and INDEX ENTRIES**. Use with caution.

Cleaner is started by using `blocksconvert -target=cleaner`. Like Builder, it needs Scheduler endpoint, Cortex schema file, index and chunk-store specific options. Note that Cleaner works with any index store supported by Cortex, not just BigTable.

- `-cleaner.scheduler-endpoint` – where to find scheduler
- `-blocks-storage.*` – blocks storage configuration, used for downloading plan files
- `-cleaner.plans-dir` – local directory to store plan file while it is being processed by Cleaner.
- `-schema-config-file` – Cortex schema file.

Cleaner doesn't **scan** for index entries, but uses existing plan files to find chunks and index entries. For each series, Cleaner needs to download at least one chunk. This is because plan file doesn't contain label names and values, but chunks do. Cleaner will then delete all index entries associated with the series, and also all chunks.

**WARNING:** If both Builder and Cleaner run at the same time and use use the same Scheduler, **some plans will be handled by builder, and some by cleaner!** This will result in a loss of data!

Cleaner should only be deployed if no other Builder is running. Running multiple Cleaners at once is not supported, and will result in leftover chunks and index entries. Reason for this is that chunks can span multiple days, and chunk is fully deleted only when processing plan (day) when chunk started. Since cleaner also needs to download some chunks to be able to clean up all index entries, when using multiple cleaners, it can happen that cleaner processing older plans will delete chunks required to properly clean up data in newer plans. When using single cleaner only, this is not a problem, since scheduler sends plans to cleaner in time-reversed order.

**Note:** Cleaner is designed for use in very special cases, eg. when deleting chunks and index entries for a specific customer. If `blocksconvert` was used to convert ALL chunks to blocks, it is simpler to just drop the index and chunks database afterwards. In such case, Cleaner is not needed.

### Limitations

The `blocksconvert` toolset currently has the following limitations:

- Scanner supports only BigTable and DynamoDB for chunks index backend, and cannot currently scan other databases.
- Supports only chunks schema versions v9 for DynamoDB; v9, v10 and v11 for Bigtable.
