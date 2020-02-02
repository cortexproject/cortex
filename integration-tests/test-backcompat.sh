#! /bin/sh

set -e

BACKWARD_IMAGE=v0.6.0

. "$(dirname "$0")/common.sh"

"$(dirname "$0")/start-components.sh"

# Note we haven't created any tables in the DB yet - we aren't going to run long enough to flush anything

echo Start first ingester, using older version
docker run $RUN_ARGS -d --name=i1 --hostname=i1 "$IMAGE_PREFIX"cortex:$BACKWARD_IMAGE -target=ingester $INGESTER_ARGS -ingester.claim-on-rollout=true

I1_ADDR=$(container_ip i1)
wait_for "curl -s -f -m 3 $I1_ADDR/ready" "ingester ready"

has_tokens_owned() {
    curl -s -f -m 3 $1/metrics | grep -q 'cortex_ring_tokens_owned'
}
DIST_ADDR=$(container_ip distributor)
wait_for "has_tokens_owned $DIST_ADDR" "distributor to see ingester in ring"

docker run $RUN_ARGS --rm $AVALANCHE_IMAGE --metric-count=2 --label-count=2 --series-count=2 --remote-requests-count=1 --remote-url=http://distributor/api/prom/push
# compute a timestamp just after the one Avalanche used, so we can query back
TIMESTAMP=$(($(date +%s)+1))

echo Start second ingester, waiting for first
docker run $RUN_ARGS -d --name=i2 --hostname=i2 "$IMAGE_PREFIX"cortex:$IMAGE_TAG -target=ingester $INGESTER_ARGS -ingester.join-after=300s -ingester.claim-on-rollout=true

echo Stop first ingester so it should hand over
docker stop i1

I2_ADDR=$(container_ip i2)
wait_for "curl -s -f -m 3 $I2_ADDR/ready" "ingester ready"

echo Start querier
docker run $RUN_ARGS -d --name=querier --hostname=querier "$IMAGE_PREFIX"cortex:$IMAGE_TAG -target=querier $COMMON_ARGS $STORAGE_ARGS -distributor.replication-factor=1 -querier.query-store-after=10m
QUERIER_ADDR=$(container_ip querier)
wait_for "curl -s -f -m 3 $QUERIER_ADDR/ready" "querier ready"

echo Check how many series are under one metric
QUERY_RESULT=$(curl -H X-Scope-OrgID:0 -sS -f -m 3 "$QUERIER_ADDR/api/prom/api/v1/query?query=count(avalanche_metric_mmmmm_0_0)&time=$TIMESTAMP")
[ $(echo $QUERY_RESULT |  jq .data.result[0].value[1]) = '"2"' ] || (echo unexpected result $QUERY_RESULT ; exit 1)

docker rm -f querier
echo Start back-version querier
docker run $RUN_ARGS -d --name=querier --hostname=querier "$IMAGE_PREFIX"cortex:$BACKWARD_IMAGE -target=querier $COMMON_ARGS $STORAGE_ARGS -distributor.replication-factor=1 -store.min-chunk-age=10m
QUERIER_ADDR=$(container_ip querier)
wait_for "curl -s -f -m 3 $QUERIER_ADDR/ready" "querier ready"

echo Check how many series are under one metric
QUERY_RESULT=$(curl -H X-Scope-OrgID:0 -sS -f -m 3 "$QUERIER_ADDR/api/prom/api/v1/query?query=count(avalanche_metric_mmmmm_0_0)&time=$TIMESTAMP")
[ $(echo $QUERY_RESULT |  jq .data.result[0].value[1]) = '"2"' ] || (echo unexpected result $QUERY_RESULT ; exit 1)
