---
title: "Binary index-header"
linkTitle: "Binary index-header"
weight: 5
slug: binary-index-header
---

In order to query series inside blocks from object storage, the [store-gateway](./store-gateway.md) has to know certain initial info from each block index. In order to achieve so, the store-gateway builds an index-header for each block and stores it on local disk; such index-header is built by downloading specific pieces of original block's index and storing them on local disk. Index header is then used by store-gateway at query time.

Store-gateways build the index-header with specific sections of the block's index downloaded using **GET byte range requests**. Since downloading specific sections of the original block's index is a computationally easy operation, the index-header is never uploaded back to the object storage and multiple store-gateway instances (or the same instance after a rolling update without a persistent disk) will re-build the index-header from original block's index each time, if not already existing on local disk.

## Format (version 1)

The index-header is a subset of the block index and contains:

- [Symbol Table](https://github.com/prometheus/prometheus/blob/master/tsdb/docs/format/index.md#symbol-table), used to unintern string values
- [Posting Offset Table](https://github.com/prometheus/prometheus/blob/master/tsdb/docs/format/index.md#postings-offset-table), used to lookup postings

The following describes the format of the index-header file found in each block store-gateway local directory. It is terminated by a table of contents which serves as an entry point into the index.

```
┌─────────────────────────────┬───────────────────────────────┐
│    magic(0xBAAAD792) <4b>   │      version(1) <1 byte>      │
├─────────────────────────────┬───────────────────────────────┤
│  index version(2) <1 byte>  │ index PostingOffsetTable <8b> │
├─────────────────────────────┴───────────────────────────────┤
│ ┌─────────────────────────────────────────────────────────┐ │
│ │      Symbol Table (exact copy from original index)      │ │
│ ├─────────────────────────────────────────────────────────┤ │
│ │      Posting Offset Table (exact copy from index)       │ │
│ ├─────────────────────────────────────────────────────────┤ │
│ │                          TOC                            │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```
