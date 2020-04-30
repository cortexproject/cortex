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

1. Version specific pages of the documentation
2. Include links to change version (the version must be in the URL)
3. Include the master version and last 3 minor releases. Documentation defaults to the last minor release.


## Proposed solution

Currently, the documentation is residing under the docs/ folder of cortexproject/cortex. It is built by Hugo using the theme [`docsy`](https://www.docsy.dev). It will have a proper [`drop-down menu`](https://www.docsy.dev/docs/adding-content/versioning/#adding-a-version-drop-down-menu) which will enable proper versioning. It has a section [`params.version`](https://www.docsy.dev/docs/adding-content/versioning/#adding-a-version-drop-down-menu) in config.toml which will allow us to map URLs with proper versions. We will have to change all the occurrences of older doc links with new links. We will keep `master` version with 3 latest `release` versions. Each release is a minor version expressed as `1.x`. The document would default to latest minor version.

From the current doc, the following paths (and all their subpages) should be versioned for now:

1. https://cortexmetrics.io/docs/apis/
2. https://cortexmetrics.io/docs/configuration/ (moving v1.x Guarantees outside of the tree, because these shouldn't be versioned)

The above should be versioned under a single URL path (`/docs/running-cortex/` in the following example, but final prefix is still to be decided).

### Example:

For `master` version we would be able to use the above links via the following path

```
/docs/running-cortex/master/configuration/
/docs/running-cortex/master/api/
```

And for a minor version like `1.x`:

```
/docs/running-cortex/1.0/configuration/
/docs/running-cortex/1.0/apis/
```

we'll have versioned documentation only under the /docs/running-cortex/ prefix and, as a starting point, all versioned pages should go there.

