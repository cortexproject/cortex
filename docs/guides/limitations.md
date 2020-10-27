---
title: "Limitations"
linkTitle: "Limitations"
weight: 998
slug: limitations
---

## Tenant ID naming

The tenant ID (also called "user ID" or "org ID") shouldn't include the following characters:

- `/`
- ` ` (whitespace)

## Query without metric name

The Cortex chunks storage doesn't support queries without a metric name, like `count({__name__=~".+"})`. On the contrary, the Cortex [blocks storage](../blocks-storage/_index.md) supports it.
