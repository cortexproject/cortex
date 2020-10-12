---
title: "How to update the build image"
linkTitle: "How to update the build image"
weight: 5
slug: how-to-update-the-build-image
---

The build image currently can only be updated by a Cortex maintainer. If you're not a maintainer you can still open a PR with the changes, asking a maintainer to assist you publishing the updated image. The procedure is:

1. Update `build-image/Docker`
2. Build the image running `make build-image/.uptodate`
3. Publish the image to the repository running `docker push quay.io/cortexproject/build-image:TAG` (this can only be done by a maintainer)
4. Replace the image tag in `.circleci/config.yml` (_there may be multiple references_)
5. Replace the image tag in `.github/workflows/*` (_there may be multiple references_)
6. Open a PR and make sure the CI with new build-image passes
