---
title: "Upgrading Cortex"
linkTitle: "Upgrading"
weight: 3
slug: upgrading
---

This page describes how to upgrade a running Cortex cluster safely. It covers
the general procedure, the component-by-component ordering, and the places to
look for version-specific breaking changes.

## Where breaking changes are recorded

Cortex follows semantic versioning for the [v1 surface]({{< relref
"../configuration/v1-guarantees.md" >}}). Anything outside that surface — and
anything inside it that requires explicit operator action — is recorded in
`CHANGELOG.md` in the repository root, in the section for the target release.

For each release, scan the CHANGELOG for these headings before upgrading:

- `[CHANGE]` — behavioural changes that affect operators.
- `[FEATURE]` / `[ENHANCEMENT]` — new options that may need configuration.
- `[BUGFIX]` — fixes; useful when an upgrade resolves a known issue.

`[CHANGE]` entries are the ones that bite. Always read every `[CHANGE]` in the
range between your current version and the target, not just the target's
entries.

## Recommended upgrade procedure

### 1. Pick a target version

Upgrade one minor version at a time when possible — for example, `1.16 → 1.17`
rather than `1.14 → 1.17` — so any `[CHANGE]` entries that require config
adjustments can be applied incrementally. Patch upgrades (`1.17.0 → 1.17.1`)
are safe to take in bulk.

### 2. Reconcile configuration

Compare your current config against `cortex -config.expand-env -modules` and
the [Config File Reference]({{< relref
"../configuration/config-file-reference.md" >}}) for the target version.

Pay particular attention to:

- Renamed or deprecated flags (the CHANGELOG `[CHANGE]` entries call these out
  explicitly).
- New required fields — usually called out in `[FEATURE]` entries.
- Runtime config (`overrides:`) — limits added in newer versions take effect
  even when the flag is absent, using the new default.

Use `cortex -config.file=cortex.yaml -modules=none -log.level=debug` against
the new binary to validate the config without starting the service.

### 3. Deploy in the canonical order

Cortex's components form a layered system. Upgrade in this order so each layer
is always serving a version that understands what the layer above sends it:

1. **Compactor, store-gateway** — read-side dependencies, downstream of
   ingester. Safe to roll first because writes are unaffected.
2. **Querier, query-frontend, query-scheduler** — depend on (1).
3. **Ingester** — careful, see below.
4. **Distributor** — depends on (3) speaking the new ingester wire format.
5. **Ruler, alertmanager** — top-of-stack, depend on (1)-(4).

Within a stateful set (ingester, store-gateway, compactor), use a rolling
update that respects the ring: drain each pod via the
`/ingester/shutdown` endpoint before terminating, so its in-memory series flush
to object storage cleanly. The bundled `CortexRolloutStuck` alert fires if the
rollout stalls — pair the rollout with the
[rollout-progress dashboard]({{< relref "./monitoring-cortex.md" >}}).

### 4. Validate after each layer

After rolling each layer, before moving to the next:

- The [bundled dashboards]({{< relref "./monitoring-cortex.md" >}}) should
  show steady error rates and latency.
- No new alerts should be firing.
- Sample a few queries from production tenants to confirm the read path
  returns the expected data.

If anything regresses, **stop and roll back the current layer** before the
next layer starts. Cortex tolerates running mixed versions across a single
minor-version boundary; running across two minors is not supported.

## Component-specific notes

### Ingester

Ingesters hold the active TSDB head in memory (and on the WAL) plus local
on-disk blocks that have been cut but may not yet have shipped. A rough
restart that loses the local disk loses up to one
`-blocks-storage.tsdb.block-ranges-period` (default `2h`) of head data plus
any on-disk blocks not yet uploaded.

- Drain via `POST /ingester/shutdown` so blocks are uploaded and series are
  handed off to a replacement replica before the pod terminates.
- Watch `cortex_ingester_shipper_uploads_total` climb on the draining pod;
  termination is safe once it stops increasing.
- The `CortexIngesterHasUnshippedBlocks` alert is a hard "don't terminate yet"
  signal.

### Compactor

The compactor owns the bucket index. After a compactor upgrade,
`CortexBucketIndexNotUpdated` should clear within one compaction interval. If
it does not, the new binary may be failing to read pre-existing index format —
check compactor logs for `cannot decode`-style errors.

### Alertmanager

Alertmanager persists state (silences, notification log) through the storage
backend. Read the relevant `[CHANGE]` entries closely for any state migration:
on rare upgrades the persisted format changes and silences/nflog from the
previous version must be replayed.

## Downgrade

Cortex supports rolling **back** within the same minor version (any patch
release of the same minor) at any time. Cross-minor downgrade is **not**
supported: persisted formats on disk and in object storage may have been
written in a way the older binary cannot read.

If you must downgrade across a minor, the safe path is:

1. Stop all writes (point Prometheus's `remote_write` at a buffer).
2. Wait for compactor to finish and ingesters to ship.
3. Restore the previous binary against object storage; ingester local state on
   disk should be discarded.
4. Resume writes.

This is invasive — prefer to roll forward to a fix instead.

## Related

- [v1 Guarantees]({{< relref "../configuration/v1-guarantees.md" >}}) — what is
  and isn't covered by the stability contract.
- [Ingesters: Rolling Updates]({{< relref
  "../guides/ingesters-rolling-updates.md" >}}) — the mechanics of draining an
  ingester safely.
- [Migrating the KV Store to Memberlist]({{< relref
  "../guides/migration-kv-store-to-memberlist.md" >}}) — the canonical example
  of a multi-step migration done online.
- `CHANGELOG.md` in the repository root — version-by-version change list.
