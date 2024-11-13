---
title: "Securing communication between Cortex components with TLS"
linkTitle: "Securing communication between Cortex components with TLS"
weight: 10
slug: tls
---

Cortex is a distributed system with significant traffic between its services.
To allow for secure communication, Cortex supports TLS between all its
components. This guide describes the process of setting up TLS.

### Generation of certs to configure TLS

The first step to securing inter-service communication in Cortex with TLS is
generating certificates. A Certifying Authority (CA) will be used for this
purpose, which should be private to the organization, as any certificates signed
by this CA will have permissions to communicate with the cluster.

We will use the following script to generate self-signed certs for the cluster:

```
# keys
openssl genrsa -out root.key
openssl genrsa -out client.key
openssl genrsa -out server.key

# root cert / certifying authority
openssl req -x509 -new -nodes -key root.key -subj "/C=US/ST=KY/O=Org/CN=root" -sha256 -days 100000 -out root.crt

# csrs - certificate signing requests
openssl req -new -sha256 -key client.key -subj "/C=US/ST=KY/O=Org/CN=client" -out client.csr
openssl req -new -sha256 -key server.key -subj "/C=US/ST=KY/O=Org/CN=localhost" -out server.csr

# certificates
openssl x509 -req -in client.csr -CA root.crt -CAkey root.key -CAcreateserial -out client.crt -days 100000 -sha256
openssl x509 -req -in server.csr -CA root.crt -CAkey root.key -CAcreateserial -out server.crt -days 100000 -sha256
```

Note that the above script generates certificates that are valid for 100,000 days.
This can be changed by adjusting the `-days` option in the above commands.
It is recommended that the certs be replaced at least once every 2 years.

The above script generates keys `client.key, server.key` and certs
`client.crt, server.crt` for both the client and server. The CA cert is
generated as `root.crt`.

### Load certs into the HTTP/GRPC server/client

Every HTTP/GRPC link between Cortex components supports TLS configuration
through the following config parameters:

#### Server flags

```
    # Path to the TLS Cert for the HTTP Server
    -server.http-tls-cert-path=/path/to/server.crt

    # Path to the TLS Key for the HTTP Server
    -server.http-tls-key-path=/path/to/server.key

    # Type of Client Auth for the HTTP Server
    -server.http-tls-client-auth="RequireAndVerifyClientCert"

    # Path to the Client CA Cert for the HTTP Server
    -server.http-tls-ca-path="/path/to/root.crt"

    # Path to the TLS Cert for the GRPC Server
    -server.grpc-tls-cert-path=/path/to/server.crt

    # Path to the TLS Key for the GRPC Server
    -server.grpc-tls-key-path=/path/to/server.key

    # Type of Client Auth for the GRPC Server
    -server.grpc-tls-client-auth="RequireAndVerifyClientCert"

    # Path to the Client CA Cert for the GRPC Server
    -server.grpc-tls-ca-path=/path/to/root.crt
```

#### Client flags

Client flags are component-specific.

For an HTTP client in the Alertmanager:
```
    # Path to the TLS Cert for the HTTP Client
    -alertmanager.configs.tls-cert-path=/path/to/client.crt

    # Path to the TLS Key for the HTTP Client
    -alertmanager.configs.tls-key-path=/path/to/client.key

    # Path to the TLS CA for the HTTP Client
    -alertmanager.configs.tls-ca-path=/path/to/root.crt
```

For a GRPC client in the Querier:
```
    # Path to the TLS Cert for the GRPC Client
    -querier.frontend-client.tls-cert-path=/path/to/client.crt

    # Path to the TLS Key for the GRPC Client
    -querier.frontend-client.tls-key-path=/path/to/client.key

    # Path to the TLS CA for the GRPC Client
    -querier.frontend-client.tls-ca-path=/path/to/root.crt
```

Similarly, for the GRPC Ingester Client:
```
    # Path to the TLS Cert for the GRPC Client
    -ingester.client.tls-cert-path=/path/to/client.crt

    # Path to the TLS Key for the GRPC Client
    -ingester.client.tls-key-path=/path/to/client.key

    # Path to the TLS CA for the GRPC Client
    -ingester.client.tls-ca-path=/path/to/root.crt
```

TLS can be configured in a similar fashion for other HTTP/GRPC clients in Cortex.
