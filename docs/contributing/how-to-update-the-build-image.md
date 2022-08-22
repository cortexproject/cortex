---
title: "How to update the build image"
linkTitle: "How to update the build image"
weight: 5
slug: how-to-update-the-build-image
---

The build image currently can only be updated by a Cortex maintainer. If you're not a maintainer you can still open a PR with the changes, asking a maintainer to assist you publishing the updated image. The procedure is:

1. Update `build-image/Docker`.
1. Run `go env` and make sure `GOPROXY=https://proxy.golang.org,direct` (Go's default). Some environment may required `GOPROXY=direct`, and if you push a build image with this, build workflow on GitHub will take a lot longer to download modules.
1. Build the and publish the image by using `make push-multiarch-build-image`. This will build and push multiplatform docker image (for linux/amd64 and linux/arm64). Pushing to `quay.io/cortexproject/build-image` repository can only be done by a maintainer. Running this step successfully requires [Docker Buildx](https://docs.docker.com/buildx/working-with-buildx/), but does not require a specific platform.
1. Replace the image tag in `.github/workflows/*` (_there may be multiple references_) and Makefile (variable `LATEST_BUILD_IMAGE_TAG`).
1. Open a PR and make sure the CI with new build-image passes


