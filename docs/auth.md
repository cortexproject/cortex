# Authentication and Authorisation

All Cortex components take the tenant ID from a header `X-Scope-OrgID`
on each request. They trust this value completely: if you need to
protect your Cortex installation from accidental or malicious calls
then you must add an additional layer of protection.

Typically this means you run Cortex behind a reverse proxy, and ensure
that all callers, both machines sending data over the remote_write
interface and humans sending queries from GUIs, supply credentials
which identify them and confirm they are authorised.

When configuring the remote_write API in Prometheus there is no way to
add extra headers. The user and password fields of http Basic auth, or
Bearer token, can be used to convey tenant ID and/or credentials.
