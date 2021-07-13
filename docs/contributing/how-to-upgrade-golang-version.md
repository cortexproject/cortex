---
title: "How to upgrade Golang version"
linkTitle: "How to upgrade Golang version"
weight: 4
slug: how-to-upgrade-golang-version
---

To upgrade the Golang version:

1. Upgrade build image version
   - Upgrade Golang version in `build-image/Dockerfile`
   - Build new image `make build-image/.uptodate`
   - Publish the new image to `quay.io` (requires a maintainer)
   - Update the Docker image tag in `.github/workflows/*`
2. Upgrade integration tests version
   - Update the Golang version installed in the `integration` job in `.github/workflows/*`
3. Upgrade the reference to the latest build image called `LATEST_BUILD_IMAGE_TAG` in `Makefile`

If the minimum support Golang version should be upgraded as well:

1. Upgrade `go` version in `go.mod`
