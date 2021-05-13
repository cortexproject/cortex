#!/bin/bash

SCRIPT_DIR=$(cd `dirname $0` && pwd)

CGO_ENABLED=0 GOOS=linux go build -o ${SCRIPT_DIR}/cortex ${SCRIPT_DIR}/../../cmd/cortex && \
docker-compose -f ${SCRIPT_DIR}/docker-compose.yml build cortex-1 && \
docker-compose -f ${SCRIPT_DIR}/docker-compose.yml up $@
