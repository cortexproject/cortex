# Alertmanager UI Assets

This directory contains pre-built Alertmanager web UI assets for the version
vendored in this repository.

These files are **not included in the Go module proxy zip** for the alertmanager
module, so they must be maintained separately from the normal vendor workflow.

## Why this exists

`go mod vendor` copies files matching `//go:embed` patterns from the module cache.
Since the module proxy zip doesn't include `app/dist/`, we keep a canonical copy
here and populate the module cache before running `go mod vendor` (see `mod-check`
target in the Makefile).

## Updating

When upgrading `github.com/prometheus/alertmanager`, update these files:

1. Download the new UI assets tarball from the GitHub release:
   wget https://github.com/prometheus/alertmanager/releases/download/vX.Y.Z/alertmanager-web-ui-X.Y.Z.tar.gz

2. Extract into this directory:
   ```
   tar -xzvf alertmanager-web-ui-X.Y.Z.tar.gz
   ```

3. Commit the updated files along with the version bump.

