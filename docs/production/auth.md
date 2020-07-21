---
title: "Authentication and Authorisation"
linkTitle: "Authentication and Authorisation"
weight: 6
slug: auth
---

All Cortex components take the tenant ID from a header `X-Scope-OrgID`
on each request. They trust this value completely: if you need to
protect your Cortex installation from accidental or malicious calls
then you must add an additional layer of protection.

Typically this means you run Cortex behind a reverse proxy, and you must
ensure that all callers, both machines sending data over the `remote_write`
interface and humans sending queries from GUIs, supply credentials
which identify them and confirm they are authorised.

When configuring the `remote_write` API in Prometheus there is no way to
add extra headers. The user and password fields of http Basic auth, or
Bearer token, can be used to convey the tenant ID and/or credentials.

To disable the multi-tenant functionality, you can pass the argument
`-auth.enabled=false` to every Cortex component, which will set the OrgID
to the string `fake` for every request.
