#!/bin/bash

if [ -n "${1}" ]; then
  KS_ENV=${1}
else
  echo "You need to specify a ksonnet environment. If you haven't already:"
  echo "# cd ksonnet/environments"
  echo "# cp -r cortex_default <YOUR_ENV_NAME>"
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
