# file intended to be sourced by other shell scripts

TEST_DIR="$( cd "$(dirname "$0")" ; pwd -P )"

IMAGE_TAG=${CIRCLE_TAG:-$($TEST_DIR/../tools/image-tag)}
IMAGE_PREFIX="${IMAGE_PREFIX:-quay.io/cortexproject/}"

COMMON_ARGS="-consul.hostname=consul:8500"
STORAGE_ARGS="-config-yaml=/tests/schema1.yaml -dynamodb.url=dynamodb://u:p@dynamodb.cortex.:8000"
INGESTER_ARGS="$COMMON_ARGS $STORAGE_ARGS -ingester.num-tokens=4 -ingester.min-ready-duration=1s --ingester.final-sleep=0s -ingester.concurrent-flushes=5"
RUN_ARGS="--net=cortex -v $TEST_DIR:/tests"
AVALANCHE_IMAGE=quay.io/freshtracks.io/avalanche:master-2019-07-28-47b00e3

# Execute command $1 repeatedly until it returns true; description in $2, optional repeat limit in $3
wait_for() {
    reps=${3:-10}
    for i in $(seq 1 $reps); do
        echo "Waiting for $2"
        if $1; then
            return
        fi
        sleep 1
    done
    echo "Timed out waiting for $2" >&2
    exit 1
}

container_ip() {
    docker inspect -f "{{.NetworkSettings.Networks.cortex.IPAddress}}" $@
}
