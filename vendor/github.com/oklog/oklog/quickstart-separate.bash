#!/usr/bin/env bash

trap 'kill 0' SIGTERM

for i in `seq 1 3`
do
  sleep 0.25
  oklog ingest \
    -api            tcp://0.0.0.0:100${i}0 \
    -ingest.fast    tcp://0.0.0.0:100${i}1 \
    -ingest.durable tcp://0.0.0.0:100${i}2 \
    -ingest.bulk    tcp://0.0.0.0:100${i}3 \
    -cluster        tcp://0.0.0.0:100${i}9 \
    -ingest.path    data/ingest/${i} \
    $PEERS 2>&1 | sed -e "s/^/[I$i] /" &
  PEERS="$PEERS -peer 127.0.0.1:100${i}9"
done

for i in `seq 1 3`
do
  sleep 0.25
  oklog store \
    -api                       tcp://0.0.0.0:200${i}0 \
    -cluster                   tcp://0.0.0.0:200${i}9 \
    -store.path                data/store/${i} \
    -store.segment-target-size 1000000 \
    -store.segment-retain      2m \
    -store.segment-purge       10m \
    $PEERS 2>&1 | sed -e "s/^/[S$i] /" &
  PEERS="$PEERS -peer 127.0.0.1:200${i}9"
done

wait
