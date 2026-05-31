#!/usr/bin/env bash
# Usage: ./update-am-ui.sh [version]
# Example: ./update-am-ui.sh 0.32.1

set -euo pipefail

VERSION="${1:-0.32.1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="${SCRIPT_DIR}/app/dist"
TARBALL="alertmanager-web-ui-${VERSION}.tar.gz"
DOWNLOAD_URL="https://github.com/prometheus/alertmanager/releases/download/v${VERSION}/${TARBALL}"
TMP_DIR="$(mktemp -d)"

trap 'rm -rf "${TMP_DIR}"' EXIT

echo "==> Downloading Alertmanager UI v${VERSION}..."
curl -fsSL "${DOWNLOAD_URL}" -o "${TMP_DIR}/${TARBALL}"

echo "==> Extracting..."
tar -xzf "${TMP_DIR}/${TARBALL}" -C "${TMP_DIR}"

EXTRACTED_DIST="$(find "${TMP_DIR}" -type d -name "dist" | head -1)"
if [ -z "${EXTRACTED_DIST}" ]; then
  echo "ERROR: Could not find 'dist' directory inside tarball. Contents:"
  find "${TMP_DIR}" | head -30
  exit 1
fi

echo "==> Found dist at: ${EXTRACTED_DIST}"

echo "==> Installing to ${TARGET_DIR}..."
rm -rf "${TARGET_DIR}"
mkdir -p "${TARGET_DIR}"
cp -r "${EXTRACTED_DIST}/." "${TARGET_DIR}/"

echo "==> Done. Contents of ${TARGET_DIR}:"
find "${TARGET_DIR}" | head -20
