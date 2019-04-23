#! /bin/sh

set -e

. "$(dirname "$0")/common.sh"

"$(dirname "$0")/start-components.sh"

# Note we haven't created any tables in the DB yet - we aren't going to run long enough to flush anything

echo Start first ingester
docker run $RUN_ARGS -d --name=i1 --hostname=i1 quay.io/cortexproject/ingester $INGESTER_ARGS -ingester.claim-on-rollout=true

I1_ADDR=$(container_ip i1)
wait_for "curl -s -f -m 3 $I1_ADDR:/ready" "ingester ready"

has_tokens_owned() {
    curl -s -f -m 3 $1:/metrics | grep -q 'cortex_ring_tokens_owned'
}
DIST_ADDR=$(container_ip distributor)
wait_for "has_tokens_owned $DIST_ADDR" "distributor to see ingester in ring"

docker run $RUN_ARGS --rm weaveworks/avalanche:remote-write --metric-count=2 --label-count=2 --series-count=2 --remote-url=http://distributor/api/prom/push

echo Start second ingester, waiting for first
docker run $RUN_ARGS -d --name=i2 --hostname=i2 quay.io/cortexproject/ingester $INGESTER_ARGS -ingester.join-after=300s -ingester.claim-on-rollout=true

echo Stop first ingester so it should hand over
docker stop i1

I2_ADDR=$(container_ip i2)
wait_for "curl -s -f -m 3 $I2_ADDR:/ready" "ingester ready"
