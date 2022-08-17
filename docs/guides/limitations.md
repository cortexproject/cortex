---
title: "Limitations"
linkTitle: "Limitations"
weight: 998
slug: limitations
---

## Tenant ID naming

The tenant ID (also called "user ID" or "org ID") is the unique identifier of a tenant within a Cortex cluster. The tenant ID is an opaque information to Cortex, which doesn't make any assumption on its format/content, but its naming has two limitations:

1. Supported characters
2. Length

### Supported characters

The following character sets are generally **safe for use in the tenant ID**:

- Alphanumeric characters
  - `0-9`
  - `a-z`
  - `A-Z`
- Special characters
  - Exclamation point (`!`)
  - Hyphen (`-`)
  - Underscore (`_`)
  - Single Period (`.`), but the tenant IDs `.` and `..` is considered invalid
  - Asterisk (`*`)
  - Single quote (`'`)
  - Open parenthesis (`(`)
  - Close parenthesis (`)`)

All other characters are not safe to use. In particular, slashes `/` and whitespaces (` `) are **not supported**.

### Length

The tenant ID length should not exceed 150 bytes/characters.

## Query series and labels

When running queries to the `/api/v1/series`, `/api/v1/labels` and `/api/v1/label/{name}/values` endpoints, query's time range is ignored and the data is always fetched from ingesters. There is experimental support to query the long-term store with the *blocks* storage engine when `-querier.query-store-for-labels-enabled` is set.
