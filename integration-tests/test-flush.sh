#! /bin/sh

set -e

. "$(dirname "$0")/common.sh"

"$(dirname "$0")/start-components.sh"

# TODO: wait for DynamoDB ready
sleep 5
echo Start table-manager
docker run $RUN_ARGS -d --name=tm --hostname=tm --restart=always quay.io/cortexproject/table-manager $STORAGE_ARGS

echo Start ingester
docker run $RUN_ARGS -d --name=i1 --hostname=i1 quay.io/cortexproject/ingester $INGESTER_ARGS -event.sample-rate=1 

sleep 5
I1_ADDR=$(container_ip i1)
wait_for "curl -s -f -m 3 $I1_ADDR:/ready" "ingester ready"

has_tokens_owned() {
    curl -s -f -m 3 $1:/metrics | grep -q 'cortex_ring_tokens_owned'
}
DIST_ADDR=$(container_ip distributor)
wait_for "has_tokens_owned $DIST_ADDR" "distributor to see ingester in ring"

docker run $RUN_ARGS --rm weaveworks/avalanche:remote-write --metric-count=2 --label-count=2 --series-count=2 --remote-url=http://distributor/api/prom/push

echo Stop ingester so it should flush
docker stop -t=60 i1 # allow 1 minute for flush to complete
