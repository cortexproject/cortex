#!/bin/bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RELEASE_TAG="v$(cat "${REPO_ROOT}/VERSION" | tr -d '[:space:]')"
CORTEX_REPO="${1:-$REPO_ROOT}"

# Scan a clean checkout of the release tag, not the working tree: bom includes
# every file under the directory, including untracked and gitignored ones.
# The checkout directory is named "cortex" because bom names the top-level
# package after it.
WORKTREE_PARENT="$(mktemp -d)"
trap 'git -C "$CORTEX_REPO" worktree remove --force "$WORKTREE_PARENT/cortex" >/dev/null 2>&1 || true; rm -rf "$WORKTREE_PARENT"' EXIT
git -C "$CORTEX_REPO" worktree add --detach "$WORKTREE_PARENT/cortex" "$RELEASE_TAG"

mkdir -p sbom

echo "Generating go-mod SBOM..."
bom generate -o sbom/go-mod.spdx \
  -n https://github.com/cortexproject/cortex \
  -d "$WORKTREE_PARENT/cortex"

echo "Generating cortex container image SBOM..."
bom generate -o sbom/cortex-container-image.spdx \
  -n https://github.com/cortexproject/cortex \
  -i "quay.io/cortexproject/cortex:${RELEASE_TAG}"

echo "Generating query-tee container image SBOM..."
bom generate -o sbom/query-tee-container-image.spdx \
  -n https://github.com/cortexproject/cortex \
  -i "quay.io/cortexproject/query-tee:${RELEASE_TAG}"

echo "Generating test-exporter container image SBOM..."
bom generate -o sbom/test-exporter-container-image.spdx \
  -n https://github.com/cortexproject/cortex \
  -i "quay.io/cortexproject/test-exporter:${RELEASE_TAG}"

echo "Generating thanosconvert container image SBOM..."
bom generate -o sbom/thanosconvert-container-image.spdx \
  -n https://github.com/cortexproject/cortex \
  -i "quay.io/cortexproject/thanosconvert:${RELEASE_TAG}"

echo "Creating sbom.tar.gz..."
tar -zcvf "${REPO_ROOT}/dist/sbom.tar.gz" sbom

echo "Done. sbom.tar.gz is at ${REPO_ROOT}/dist/sbom.tar.gz"
