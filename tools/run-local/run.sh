#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="${SCRIPT_DIR}/../.."

source ${SCRIPT_DIR}/func.sh

function getCortexLiteImage() {
  echo $(${DOCKER} images --filter=reference='quay.io/weaveworks/cortex-lite*:latest' --format="{{ .Repository }}:{{ .Tag }}" | sort | uniq)
}

function buildLite() {
  pushd ${PROJECT_DIR} > /dev/null
  eval "$(${MK} docker-env -p cortex-lite)"
  make cmd/lite/.uptodate
  popd > /dev/null
}

echo "Checking for cortex-lite minikube profile..."
${MK} status -p cortex-lite
if [ $? -ne 0 ]; then
  echo "cortex-lite profile not found, creating ..."
  ${MK} start --cpus 4 --memory 8192 --kubernetes-version v1.9.4 --profile cortex-lite --log_dir ${SCRIPT_DIR}/logs
fi

eval "$(${MK} docker-env -p cortex-lite)"
echo "cortex-lite minikube profile setup, continuing..."

CORTEX_LITE_IMG=$(getCortexLiteImage)

echo "Checking for cortex-lite image..."

if [ -z "${CORTEX_LITE_IMG}" ]; then
  echo "Need to build cortex-lite image..."
  buildLite

  if [ -z "${CORTEX_LITE_IMG}" ]; then
    echo "Build failed, or the image went away. Exiting."
    exit 1
  fi
fi

echo "Found cortex-lite image in minikube docker: ${CORTEX_LITE_IMG}. Continuing..."

echo "Applying ksonnet bits to ${KS_ENV} environment..."
pushd ksonnet > /dev/null
kubectl config use-context cortex-lite
sed -ie 's/cortex_default/'${KS_ENV}'/g' app.yaml
${KS} apply --insecure-skip-tls-verify ${KS_ENV}
git checkout app.yaml
popd > /dev/null
