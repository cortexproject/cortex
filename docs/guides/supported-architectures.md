---
title: "Supported architectures"
linkTitle: "Supported architectures"
weight: 16
---

Cortex release artifacts are built for the operating systems and architectures
listed below. Use these targets when selecting container images, binary
artifacts, or OS packages for production deployments.

## Supported release targets

| Artifact type | Operating system | Architectures |
|---------------|------------------|---------------|
| Container images | Linux | `amd64`, `arm64` |
| Cortex binary | Linux | `amd64`, `arm64` |
| Cortex binary | darwin (macOS) | `amd64`, `arm64` |
| `query-tee` binary | Linux | `amd64`, `arm64` |
| `query-tee` binary | darwin (macOS) | `amd64`, `arm64` |
| Debian package | Linux | `amd64`, `arm64` |
| RPM package | Linux | `amd64`, `arm64` |

The CI and release pipelines build Cortex with `GOOS` and `GOARCH` targets
matching these rows. Automated tests run against Linux `amd64` and `arm64`
Cortex images.

## Unsupported targets

Other operating systems or architectures may work when built from source, but
they are not part of the regular Cortex release artifacts or CI matrix. Treat
those builds as unsupported unless you validate them in your own environment.

Cortex does not require architecture-specific CPU extensions such as AVX in its
release build configuration. If you use external services or custom base images
alongside Cortex, verify their architecture and CPU requirements separately.
