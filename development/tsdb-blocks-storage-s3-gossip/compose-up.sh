#!/bin/bash

set -e

SCRIPT_DIR=$(cd `dirname $0` && pwd)

# -gcflags "all=-N -l" disables optimizations that allow for better run with combination with Delve debugger.
CGO_ENABLED=0 GOOS=linux go build -mod=vendor -gcflags "all=-N -l" -o ${SCRIPT_DIR}/cortex ${SCRIPT_DIR}/../../cmd/cortex
docker-compose -f ${SCRIPT_DIR}/docker-compose.yml build distributor 
docker-compose -f ${SCRIPT_DIR}/docker-compose.yml up $@
