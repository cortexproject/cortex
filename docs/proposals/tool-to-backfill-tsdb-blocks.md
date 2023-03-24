---
title: "Tools to backfill TSDB blocks"
linkTitle:
weight: 1
slug:
---

- Author: [Siddharth Asthana](https://github.com/edith007)
- Date: March 2023
- Status: draft

## Overview

There might be a lot of cases where the users might already be using Prometheus and later decides to switch to Cortex. Cortex will take care of the metrics which will be generated after the switch, but at present there is no way to backfill the old Prometheus TSDB blocks to Cortex. This proposal is to add the ability in cortex to fetch the Prometheus TSDB blocks and backfill the data.

## Detailed Solution

We can create a new service called `Importer` which will be responsible for bringing in the old Prometheus TSDB blocks to Cortex.

### How Importer will work

The importer service will have one API endpoints: `import`.

`import`:

- It will take a link to where the old Prometheus TSDB blocks are stored
- Cortex will download this block from the link received and upload to the long term storage

### How will Cortex import the old TSDB blocks

Cortex will import the old TSDB blocks by using the `cortex-cli` tool, which will be used by
the user to initiate the import process. The `cortex-cli` will take care of the process of
uploading the blocks to cortex. The blocks can be store in either local storage or remote
storage (such as GCS or S3), depending on the user's choice.

When the user runs the `cortex-cli` import command, it will initiate the import process. The
CLI will read the block from the specified location and upload them to the Cortex cluster. The
`cortex-cli` will also provide feedback to the user, such as progress bars and status updates.

During the import process, Cortex will upload the old TSDB blocks in one request. This approach will simplify the import process by avoiding the complexity of chunking the blocks and managing affinity. Additionally, it will ensure that all the necessary files are present for validation and storage. Cortex will utilize parallel uploading to improve the performance of the import process.

Once the import process is complete, the user can verify that the blocks have been successfully
imported by querying the Cortex API.

### Architecture

┌───────────┐ 1. CLI sends compressed block bundle in a block-id.tgz file to /import endpoint
│   CLI     │──────────────────────────┬──────────┐
└───────────┘                          │          ▼
                                       │   ┌──────────────┐
                                       │   │   Cortex     │
                                       │   │  API Server  │
                                       ▼   └──────────────┘
                                2. Endpoint receives bundle and uncompresses it
                                       │          │
                                       │          │
                                       ▼          │
                                3. Bundle is validated, metadata is updated, and any other required modifications are made
                                       │          │
                                       │          │
                                       ▼          │
                                4. Bundle is uploaded to long term storage (S3)
                                       │          │
                                       │          │
                                       ▼          │
                                5. 200 OK response is sent back to the CLI