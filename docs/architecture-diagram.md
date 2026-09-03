---
title: "Interactive Architecture Diagram"
linkTitle: "Interactive Diagram"
weight: 3
slug: architecture-diagram
---

The [interactive architecture diagram](../tools/diagram/cortex-architecture.html)
draws the same system [Architecture](./architecture.md) describes in prose, with the
details attached to the picture instead of scattered through the text. It covers
the write path, the read path, the blocks lifecycle and the optional services —
ruler, alertmanager, compactor, store-gateway, query-scheduler and the caches.
Hover a connector and it names the protocol and the endpoint that hop actually
uses; select a component and it gives you the role, whether it is stateful, which
hash ring it joins, the endpoints it serves, its `-target` value and the file in
the Cortex tree that implements it.

Three toggles cover the places where the topology genuinely forks, rather than
drawing one deployment and calling it typical: the query-frontend's own queue
versus a separate query-scheduler, the ruler evaluating rules in its own querier
stack versus delegating to the query-frontend with `-ruler.frontend-address`, and
the parquet queryable off versus on. There are also guided walkthroughs that step
through the write, read, rule-evaluation and blocks flows one hop at a time, a
table view of every component and flow, and a dark-mode toggle.

The diagram's metadata is hand-maintained against the Cortex source rather than
generated from it, so the `src` path shown in each component's panel is the
authority — if a ring key, prefix or endpoint disagrees with the code, the code is
right and the diagram needs fixing. It also deliberately shows a few things the
prose does not yet cover, such as the OTLP ingest endpoint and the
parquet-converter, which is marked experimental for that reason. Its source lives
in [`tools/diagram/`](https://github.com/cortexproject/cortex/tree/master/tools/diagram).

**[Open the interactive architecture diagram &rarr;](../tools/diagram/cortex-architecture.html)**
