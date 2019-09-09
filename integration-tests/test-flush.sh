#! /bin/sh

set -e

. "$(dirname "$0")/common.sh"

"$(dirname "$0")/start-components.sh"

# TODO: wait for DynamoDB ready
sleep 5
echo Start table-manager
docker run $RUN_ARGS -d --name=tm --hostname=tm --restart=always "$IMAGE_PREFIX"cortex:$IMAGE_TAG -target=table-manager $STORAGE_ARGS -table-manager.retention-period=168h -dynamodb.poll-interval=5s

echo Start ingester
docker run $RUN_ARGS -d --name=i1 --hostname=i1 "$IMAGE_PREFIX"cortex:$IMAGE_TAG -target=ingester $INGESTER_ARGS

sleep 5
I1_ADDR=$(container_ip i1)
wait_for "curl -s -f -m 3 $I1_ADDR/ready" "ingester ready"

has_tokens_owned() {
    curl -s -f -m 3 $1/metrics | grep 'cortex_ring_tokens_owned' >/dev/null
}
DIST_ADDR=$(container_ip distributor)
wait_for "has_tokens_owned $DIST_ADDR" "distributor to see ingester in ring"

docker run $RUN_ARGS --rm $AVALANCHE_IMAGE --metric-count=2 --label-count=2 --series-count=2 --remote-requests-count=1 --remote-url=http://distributor/api/prom/push

echo Stop ingester so it should flush
docker stop -t=60 i1 # allow 1 minute for flush to complete
