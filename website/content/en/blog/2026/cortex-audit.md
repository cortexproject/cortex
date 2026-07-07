---
date: 2026-07-07
title: "Cortex Security Audit Results"
linkTitle: Cortex Security Audit Results
tags: [ "blog", "cortex", "security" ]
categories: [ "blog" ]
projects: [ "cortex" ]
description: >
  Results of the Cortex third-party security audit targeting multi-tenant isolation and cluster operations, performed by Quarkslab and coordinated by OSTIF.
author: Daniel Blando ([@danielblando](https://github.com/danielblando))
---

## Introduction

The Cortex maintainer team is publishing the results of a third-party security audit of the Cortex codebase, completed in Q2 2026.

Cortex is a horizontally scalable, multi-tenant, long-term storage backend for Prometheus. Multi-tenancy is enforced at the application layer via the `X-Scope-OrgID` header — every read and write path uses this tenant identifier to scope access to time-series data, rules, and alertmanager configurations. The integrity of this boundary is a critical security property.

Given the project's role as the storage layer for multiple managed Prometheus offerings and large-scale self-hosted deployments, the maintainers requested a dedicated security engagement through the CNCF.

## Engagement Details

- **Auditor:** [Quarkslab](https://www.quarkslab.com/)
- **Coordinator:** [OSTIF](https://ostif.org/)
- **Assessed commit:** `b4f5cfc37d83719040de7ca997ec125304a6b766`
- **Methodology:** Whitebox code review, static analysis, dynamic testing

## Scope

The auditors operated from a threat model agreed upon with the maintainers prior to the engagement. The focus areas were:

1. **Tenant boundary integrity** — Can a tenant read, write, or delete another tenant's series, rules, or alertmanager state? Are there code paths where `X-Scope-OrgID` propagation is missing or bypassable?
2. **Cluster operations** — Can an internal component (ingester, compactor, store-gateway) be induced into serving cross-tenant data? Are gossip/memberlist communications authenticated and integrity-checked?
3. **Ingestion path** — Remote write, PushStream gRPC, distributor validation, ingester replication.
4. **Query path** — Query frontend, querier, store-gateway block loading and filtering.
5. **Configuration and secrets exposure** — Runtime config endpoints, debug endpoints.

## Findings

7 vulnerabilities identified — 6 Medium, 1 Low. No Critical or High severity issues.

| ID  | Title | Risk | Impact | Attack Vector |
|-----|-------|------|--------|---------------|
| V01 | Tenant impersonation via PushStream gRPC | Medium | High (cross-tenant write) | Authenticated tenant sending crafted gRPC stream with overridden org ID |
| V02 | Stored XSS | Medium | Marginal | Malicious metric label values rendered in UI contexts |
| V03 | Sensitive information leakage | Medium | Marginal | `/config` endpoint exposing storage credentials in plaintext |
| V04 | Unbound Gzip decompression | Medium | Marginal (DoS) | Crafted compressed payload causing excessive memory allocation on distributor |
| V05 | Uncontrolled memory allocation via protobuf histogram | Medium | Marginal (DoS) | Malformed histogram sample with extreme bucket count |
| V06 | Unbounded read on gossip connections | Medium | Marginal (DoS) | Oversized gossip packet causing unbounded memory read in memberlist |
| V07 | Gossip packet integrity check not enforced | Low | Negligible | Unauthenticated gossip messages accepted without HMAC validation |

## Fix Status

All 7 findings have verified fixes merged to `master` and shipped in [**Cortex v1.21.1**](https://github.com/cortexproject/cortex/releases/tag/v1.21.1) (released 2026-06-04).

| Finding | Fix | PR |
|---------|-----|----|
| V01 — Tenant impersonation via PushStream gRPC | Reject `PushStream` requests where per-message `TenantID` diverges from authenticated caller; add HMAC-SHA256 stream auth via `-distributor.sign-write-requests-keys` | [#7475](https://github.com/cortexproject/cortex/pull/7475) |
| V02 — Stored XSS | Replace `text/template` with `html/template` in Alertmanager and Store Gateway status pages | [#7512](https://github.com/cortexproject/cortex/pull/7512) |
| V03 — Sensitive information leakage | Mask Swift, etcd, Redis, and HTTP basic-auth credentials on `/config` endpoint | [#7473](https://github.com/cortexproject/cortex/pull/7473) |
| V04 — Unbound Gzip decompression | Cap decompressed body via `-distributor.otlp-max-recv-msg-size` in `ParseProtoReader` and OTLP path | [#7515](https://github.com/cortexproject/cortex/pull/7515) |
| V05 — Uncontrolled memory allocation via protobuf histogram | Add `WrappedHistogram` with configurable size limit (`-validation.max-native-histogram-size-bytes`, default 16 KB) | [#7570](https://github.com/cortexproject/cortex/pull/7570) |
| V06 — Unbounded read on gossip connections | Add `-memberlist.packet-read-timeout`, `-memberlist.max-packet-size`, `-memberlist.max-concurrent-connections` | [#7518](https://github.com/cortexproject/cortex/pull/7518) |
| V07 — Gossip packet integrity check not enforced | Drop incoming TCP transport packets when digest verification fails | [#7474](https://github.com/cortexproject/cortex/pull/7474) |

**Upgrade to [v1.21.1](https://github.com/cortexproject/cortex/releases/tag/v1.21.1) or later to include all security fixes.**

## Auditor Assessment

From the report:

> "Cortex benefits from a solid engineering foundation. The project shows good code quality, extensive test coverage, and strong observability features, all of which contribute to its maintainability and resilience."

The report also recommended:

- Tightening default configurations (e.g., enabling gossip encryption by default)
- Centralizing input validation at ingestion boundaries rather than relying on per-component checks
- Establishing a dependency update cadence for indirect transitive dependencies

## Operator Action Required

If you operate a multi-tenant Cortex cluster:

1. **Update to the [v1.21.1](https://github.com/cortexproject/cortex/releases/tag/v1.21.1) or later release.** All fixes are included.
2. **Review your `/config` endpoint exposure.** If you expose Cortex's HTTP API without authentication, V03 may have been exploitable in your environment.
3. **Consider enabling memberlist encryption** (`-memberlist.encryption-enabled=true`) if your gossip traffic traverses untrusted networks. V07 makes this particularly relevant.
4. **Audit PushStream gRPC access.** If you expose the distributor gRPC port to tenants directly (not behind an auth gateway), V01 was exploitable prior to the fix.

## Resources

- [OSTIF Announcement](https://ostif.org/cortex-audit-complete/)
- [Quarkslab Blog Post](https://blog.quarkslab.com/cortex-security-audit.html)

## Acknowledgments

- [Quarkslab](https://www.quarkslab.com/)
- [OSTIF](https://ostif.org/)
- [CNCF](https://cncf.io)
- Cortex maintainers and community
