#! /bin/sh

set -e

docker rm -f tm i2 i1 distributor dynamodb consul || true
docker network rm cortex || true
