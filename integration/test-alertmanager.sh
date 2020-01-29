#! /bin/sh

set -e

. "$(dirname "$0")/common.sh"

docker network create cortex

ALERTMANAGER_ARGS="-target=alertmanager -log.level=debug -alertmanager.storage.local.path=/tests/fixtures/alertmanager_configs/ -alertmanager.storage.type=local -target=alertmanager -alertmanager.web.external-url=http://localhost:8080/api/prom"
echo Start alertmanager
docker run $RUN_ARGS -p 8080:80 -d --name=alertmanager --hostname=alertmanager "$IMAGE_PREFIX"cortex:$IMAGE_TAG $ALERTMANAGER_ARGS

AM_ADDR=$(container_ip alertmanager)

check_metrics() {
    curl -s -f -m 3 -XGET -H "X-Scope-OrgID: example_user" "$1/metrics"
}

check_api_v1() {
    curl -s -f -m 3 -XGET -H "X-Scope-OrgID: example_user" "$1/api/prom/api/v1/status"
}

wait_for "check_api_v1 $AM_ADDR" "api/v1/status"

docker stop alertmanager
docker network rm cortex || true
