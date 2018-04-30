#!/bin/bash

if [ -n "${1}" ]; then
  KS_ENV=${1}
else
  echo "You need to specify a ksonnet environment. If you haven't already:"
  echo "# cd ksonnet/environments"
  echo "# cp -r default <YOUR_ENV_NAME>"
  echo "# vi <YOUR_ENV_NAME>/params.libsonnet"
  echo "Then modify the cortex_args object to use appropriate values."
  echo "Note that your new environment will not be accidentally added to source control, no worries."
  echo "----------------------------"
  echo "Usage: ${0} environment_name"
  exit 1
fi

KS_ENV=${1}
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="${SCRIPT_DIR}/../.."

DOCKER=$(which docker) > /dev/null 2>&1
if [ -z "${DOCKER}" ]; then
  echo "Sorry, you need to install Docker. Exiting."
  exit 1
fi

MK=$(which minikube) >/dev/null 2>&1
if [ -z ${MK} ]; then
  echo "Sorry, you need to install minikube. Exiting."
  exit 1
fi

KS=$(which ks) > /dev/null 2>&1
if [ -z "${KS}" ]; then
  echo "Sorry, you need to install ksonnet. Exiting."
  exit 1
fi

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

echo "Applying ksonnet bits to cortex-lite profile..."
pushd ksonnet > /dev/null
${KS} apply --insecure-skip-tls-verify ${KS_ENV}
popd > /dev/null
