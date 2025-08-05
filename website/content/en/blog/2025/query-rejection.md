---
date: 2025-08-04
title: "Block the Blast: How Query Rejection Protects Your Cortex Cluster"
linkTitle: Query Rejection in Cortex
tags: [ "blog", "cortex", "query", "rejection" ]
categories: [ "blog" ]
projects: [ "cortex" ]
description: >
  Query rejection gives Cortex operators a last-resort safety net against disruptive queries that bypass resource limits. Learn how it works, how to configure it, and best practices to protect your multi-tenant cluster.
author: Erlan Zholdubai uulu ([@erlan-z](https://github.com/erlan-z))
---

# Introduction

We had events where a set of seemingly **harmless-looking** dashboard queries kept slipping just under our limits yet repeatedly **OOM-killing the querier pods**. Our safeguard mechanisms weren’t enough, and the only hope was that the tenant would either stop those queries or that we’d have to throttle all traffic from that tenant. Usually it wasn’t all traffic causing trouble—it was a small set of queries coming from a specific dashboard or some query with specific characteristics. We wished there was a way to manually specify query characteristics and reject them without throttling everything. **This inspired us to build query rejection**, a last-resort safety net for operators running multi-tenant Cortex clusters.

## Why Limits Aren’t Enough

Cortex already includes resource limiting, throttling and other resource safeguards, but these protections can’t cover every edge case. Some limits are enforced too late in the query lifecycle to matter; others are broad and can’t target specific bad actors. As a result, a single heavy query can bypass all service limits and still:

- Cause OOM kills that disrupt other tenants.
- Degrade availability or spike latency for everyone.
- Require manual operator intervention to restore normal service.

We needed a more precise tool—one that lets operators proactively block specific query patterns without harming legitimate traffic.

## What Is Query Rejection?

Think of query rejection as an “emergency stop” in a factory. It sits in front of the query engine and checks each request against a set of operator-defined rules. If a request matches criteria, it’s rejected immediately. This allows you to target the handful of queries that cause trouble without imposing a blanket slowdown on everyone else.

**Key features:**

- **Per-tenant control:** It's defined in the tenant limit configuration, which only targets queries from specific tenant. 
- **Precise matching:** You can specify different query attributes to narrow down to specific queries. All fields within a rule set must match (AND logic). If needed, you can define multiple independent rule sets to target different types of queries.
- **Pre-processing enforcement:** Query rejection is applied before the query is executed, allowing known-bad patterns to be blocked before consuming any resources.

## Matching Criteria

Heavy queries often share identifiable traits. Query rejection lets you match on a variety of attributes and reject only those requests. You can combine as many of these as needed:

- **API type:** `query`, `query_range`, `series`, etc.
- **Query string (regex):** Match by pattern, e.g., any query containing “ALERT”.
- **Time range:** Match queries whose range falls between a configured **min** and **max**.
- **Time window:** Match queries based on how far their time window is from now by specifying relative **min** and **max** boundaries. This is often used to distinguish queries that hit hot storage versus cold storage.
- **Step value (resolution):** Block extremely fine resolutions (e.g., steps under 5s).
- **Headers:** Match by User-Agent, Grafana dashboard UID (`X-Dashboard-Uid`) or panel ID (`X-Panel-Id`).

By combining these fields, you can zero in on the exact query patterns causing problems without over-blocking.

## Configuring Query Rejection

You define query rejection rules per tenant in a runtime config file. Each rule specifies a set of attributes that must all match for the query to be rejected. The configuration supports multiple such rule sets.

Here’s an example configuration:

```yaml
# runtime_config.yaml
overrides:
  <tenant_id>:
    query_rejection:
      enabled: true
      query_attributes:
        - api_type: query_range
          regex: .*ALERT.*
          query_step_limit:
            min: 6s
            max: 20s
          dashboard_uid: "dash123"
```

**What this does:**

- `enabled` This allows you to temporarily turn off query rejection without removing the configuration. It can help verify whether previously blocked queries are still causing issues.
- `query_attributes` is a list of rejection rules. A query will only be rejected if it matches all attributes within one rejection rule.

In the example above, the single rejection rule requires the following:

- API type must be `query_range`.
- The query string must contain the word `ALERT`.
- The step must be between 6 and 20 seconds.
- The request must come from a dashboard with UID `dash123`.

If all of these conditions match, the request is rejected with a `422` response. If even one condition doesn’t match, the query is allowed to run. You can define additional rejection rules in the list to target different patterns.

## Practical Example

Imagine a dashboard panel that repeatedly hits your cluster with a query like this:

```bash
curl \
  'http://localhost:8005/prometheus/api/v1/query?query=customALERTquery&start=1718383304&end=1718386904&step=7s' \
  -H "User-Agent: other" \
  -H "X-Dashboard-Uid: dash123"
```

Because this request matches all the configured attributes, it will be blocked. But if even one field is different—such as a longer step duration or a different dashboard UID—the query will go through.

## Best Practices and Cautions

- **Start with narrow rules.** Use the most specific fields first—like panel ID or dashboard UID—to reduce risk of over-blocking.

- **Monitor rejections.** Use the `cortex_query_frontend_rejected_queries_total` metric to track rejected queries, and check logs for detailed query information.

- **Communicate with tenants.** Let affected tenants know if their queries are being blocked, and help them adjust their dashboards accordingly.

## Conclusion

When traditional safeguards fall short, query rejection gives operators precise control to block only what’s harmful—without slowing down everything else.

If you operate a shared Cortex environment, consider learning how to use query rejection effectively. It might just save you from the next incident—by preventing OOM kills, degraded performance, or disruption to other tenants.

