#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="${SCRIPT_DIR}/../.."

source ${SCRIPT_DIR}/func.sh

echo "Deleting ksonnet bits from ${KS_ENV} environment..."
pushd ksonnet > /dev/null
kubectl config use-context cortex-lite
sed -ie 's/cortex_default/'${KS_ENV}'/g' app.yaml
${KS} delete --insecure-skip-tls-verify ${KS_ENV}
git checkout app.yaml
popd > /dev/null
