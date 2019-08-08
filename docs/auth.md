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
add extra headers. The http user and password fields can be user to
convey tenant ID and/or authentication credentials.
