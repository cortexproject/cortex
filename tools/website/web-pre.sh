#!/usr/bin/env bash

if ! [[ "$0" =~ "tools/website/web-pre.sh" ]]; then
	echo "must be run from repository root"
	exit 255
fi

WEBSITE_DIR="website"
ORIGINAL_CONTENT_DIR="docs"
OUTPUT_CONTENT_DIR="${WEBSITE_DIR}/content/en/docs"

rm -rf ${OUTPUT_CONTENT_DIR} || true
mkdir -p ${OUTPUT_CONTENT_DIR}

# Copy original content.
cp -r ${ORIGINAL_CONTENT_DIR}/* ${OUTPUT_CONTENT_DIR}
cp -r code-of-conduct.md CHANGELOG.md ${OUTPUT_CONTENT_DIR}
cp GOVERNANCE.md ${OUTPUT_CONTENT_DIR}/governance/_index.md
cp images/* ${WEBSITE_DIR}/static/images

# Add headers to special CODE_OF_CONDUCT.md and CHANGELOG.md files.
echo "$(cat <<EOT
---
title: Code of Conduct
type: docs
originalpath: code-of-conduct.md
weight: 13
---
EOT
)" > ${OUTPUT_CONTENT_DIR}/code-of-conduct.md
tail -n +2 code-of-conduct.md >> ${OUTPUT_CONTENT_DIR}/code-of-conduct.md

echo "$(cat <<EOT
---
title: Changelog
type: docs
originalpath: CHANGELOG.md
weight: 12
---
EOT
)" > ${OUTPUT_CONTENT_DIR}/CHANGELOG.md
tail -n +2 CHANGELOG.md >> ${OUTPUT_CONTENT_DIR}/CHANGELOG.md

echo "$(cat <<EOT
---
title: Governance
type: docs
originalpath: GOVERNANCE.md
weight: 11
---
EOT
)" > ${OUTPUT_CONTENT_DIR}/governance/_index.md
tail -n +2 GOVERNANCE.md >> ${OUTPUT_CONTENT_DIR}/governance/_index.md

ALL_DOC_CONTENT_FILES=`echo "${OUTPUT_CONTENT_DIR}/**/*.md ${OUTPUT_CONTENT_DIR}/*.md"`
for file in $(find ${OUTPUT_CONTENT_DIR} -name '*.md')
do
	go run ./tools/website/website.go $file
done
