---
title: "How to update the build image"
linkTitle: "How to update the build image"
weight: 5
slug: how-to-update-the-build-image
---

The procedure is:

1. Update `build-image/Dockerfile`
1. Create a PR to master with that changed, after the PR is merged to master, the new build image is available in the quay.io repository. Check github action logs [here](https://github.com/cortexproject/cortex/actions/workflows/build-image.yml) for to find the image tag.
1. Create another PR to replace the image tag in `.github/workflows/*` (_there may be multiple references_) and Makefile (variable `LATEST_BUILD_IMAGE_TAG`).
1. If you are updating Go's runtime version be sure to change `actions/setup-go`'s `go-version` in ``.github/workflows/*`.
1. Open a PR and make sure the CI with new build-image passes
