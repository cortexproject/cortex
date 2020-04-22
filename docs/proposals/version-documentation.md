---
title: "Documentation Versioning"
linkTitle: "Documentation Versioning"
weight: 1
slug: documentation-versioning
---

- Author: [Jay Batra](https://github.com/jaybatra26)
- Date: March 2020
- Status: proposal

## Problem
In Cortex, currently, we are missing versioning of documentation. The idea is to have version documentation just like Prometheus.[`Prometheus`](https://prometheus.io/docs/introduction/overview/). Documentation is the main source of information for current contributors and first-timers. A properly versioned documentation will help everyone to have a proper place to look for answers before flagging it in the community. 

In this proposal, we want to solve this. In particular, we want to:
1. Version Documentation.
2. Add or remove necessary and unnecessary data from it.
3. Organize the documentation according to the latest and stable developments.


## Proposed solution
Currently, the documentation is residing under the docs/ folder of cortexproject/cortex. It is built by Hugo using the theme [`docsy`](https://www.docsy.dev). It will have a proper [`drop-down menu`](https://www.docsy.dev/docs/adding-content/versioning/#adding-a-version-drop-down-menu) which will enable proper versioning. It has a section [`params.version`](https://www.docsy.dev/docs/adding-content/versioning/#adding-a-version-drop-down-menu) in config.toml which will allow us to map URLs with proper versions. We will have to change all the occurrences of older doc links with new links. The major versions would contain the releases, all other work should be a part of minor versions. The major version here referred is 1.0.0 and minor versions should be like 1.x

From the current doc, 
1. The chunk storage part(including getting started, architecture, and production implementation) should be a major release.
2. All other experimental storage should be a part of the minor release(Block and Gossip Ring)
3. Cortex API- part of the minor version as they are WIP.
4. Guides- major version
5. Operations- Block storage should be a part of the minor version. Query Auditor and Query Tee should be a part of the major version.
6. Contributing should be a part of the major version.
7. Governance, Changelog, Proposals, and Code of Conduct should be a part of the major version.

### Debatable Topics:
1. The above might need deduplication of information as the theme follows a folder structure. Everything under mysite.com/0.7/get_started is a copy of mysite.com/0.6/get_started.
