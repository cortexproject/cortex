---
title: "Authentication and Authorisation"
linkTitle: "Authentication and Authorisation"
weight: 10
slug: auth
---

All Cortex components take the tenant ID from a header `X-Scope-OrgID`
on each request. A tenant (also called "user" or "org") is the owner of
a set of series written to and queried from Cortex. All Cortex components
trust this value completely: if you need to protect your Cortex installation
from accidental or malicious calls then you must add an additional layer
of protection.

Typically this means you run Cortex behind a reverse proxy, and you must
ensure that all callers, both machines sending data over the `remote_write`
interface and humans sending queries from GUIs, supply credentials
which identify them and confirm they are authorised.

When configuring the `remote_write` API in Prometheus there is no way to
add extra headers. The user and password fields of http Basic auth, or
Bearer token, can be used to convey the tenant ID and/or credentials.
See the **Cortex-Tenant** section below for one way to solve this.

To disable the multi-tenant functionality, you can pass the argument
`-auth.enabled=false` to every Cortex component, which will set the OrgID
to the string `fake` for every request.

Note that the tenant ID that is used to write the series to the datastore
should be the same as the one you use to query the data. If they don't match
you won't see any data. As of now, you can't see series from other tenants.

For more information regarding the tenant ID limits, refer to: [Tenant ID limitations](./limitations.md#tenant-id-naming)

### Cortex-Tenant

One way to add `X-Scope-OrgID` to Prometheus requests is to use a [cortex-tenant](https://github.com/blind-oracle/cortex-tenant)
proxy which is able to extract the tenant ID from Prometheus labels.

It can be placed between Prometheus and Cortex and will search for a predefined
label and use its value as `X-Scope-OrgID` header when proxying the timeseries to Cortex.

This can help to run Cortex in a trusted environment where you want to separate your metrics
into distinct namespaces by some criteria (e.g. teams, applications, etc).

Be advised that **cortex-tenant** is a third-party community project and it's not maintained by Cortex team.
