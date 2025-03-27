#!/bin/sh
set -e
while ! wget -qO- http://127.0.0.1:8333/status; do
  sleep 10
done
echo "s3.bucket.create -name cortex-blocks; s3.bucket.create -name cortex-ruler; s3.bucket.create -name cortex-alertmanager"|/usr/bin/weed -logtostderr=true shell
