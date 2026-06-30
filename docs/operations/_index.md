---
title: "Operating Cortex"
linkTitle: "Operations"
no_section_index_title: true
weight: 8
menu:
---

This section covers day-2 operation of a Cortex cluster. Start here if you are
running Cortex in production.

## Core operator guides

- [Monitoring Cortex]({{< relref "./monitoring-cortex.md" >}}) — install the
  bundled dashboards, alert rules, and recording rules.
- [Troubleshooting]({{< relref "./troubleshooting.md" >}}) — symptom-driven
  decision tree for the write path, read path, storage, and rings.
- [Upgrading]({{< relref "./upgrading.md" >}}) — version-to-version upgrade
  procedure, component ordering, and downgrade caveats.

## Specialized topics

- [Scaling the Query Frontend]({{< relref "./scalable-query-frontend.md" >}})
- [Query Auditor]({{< relref "./query-auditor.md" >}}) — detect query
  correctness regressions.
- [Query Tee]({{< relref "./query-tee.md" >}}) — compare two Cortex deployments
  side-by-side.
- [Requests Mirroring with Envoy]({{< relref
  "./requests-mirroring-to-secondary-cluster.md" >}})
- [CI Modernization]({{< relref "./ci-modernization.md" >}})

For component-level operational guidance (HA pairs, shuffle sharding, zone
replication, capacity planning, encryption), see the [Guides]({{< relref
"../guides/" >}}) section.