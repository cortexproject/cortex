#!/bin/bash

# Exit on any problems.
set -e

if [[ $# -lt 4 ]]; then
    echo "Usage: $0 <namespace> <from_statefulset> <to_statefulset> <instances_to_downscale | 'all'>"
    exit 1
fi

NAMESPACE=$1
DOWNSCALE_STATEFULSET=$2
UPSCALE_STATEFULSET=$3
INSTANCES_TO_DOWNSCALE=$4

DOWNSCALE_REPLICA_SIZE=$(kubectl get statefulset "$DOWNSCALE_STATEFULSET" -o 'jsonpath={.spec.replicas}' --namespace="$NAMESPACE")
UPSCALE_REPLICA_SIZE=$(kubectl get statefulset "$UPSCALE_STATEFULSET" -o 'jsonpath={.spec.replicas}' --namespace="$NAMESPACE")

if [[ "$INSTANCES_TO_DOWNSCALE" = "all" ]]; then
  INSTANCES_TO_DOWNSCALE=$DOWNSCALE_REPLICA_SIZE
fi

echo "Going to downscale $NAMESPACE/$DOWNSCALE_STATEFULSET and upscale $NAMESPACE/$UPSCALE_STATEFULSET by $INSTANCES_TO_DOWNSCALE instances"

while [[ $INSTANCES_TO_DOWNSCALE -gt 0 ]]; do
  echo "----------------------------------------"
  echo "$(date): Scaling UP $UPSCALE_STATEFULSET to $((UPSCALE_REPLICA_SIZE + 1))"
  # Scale up
  kubectl scale statefulset "$UPSCALE_STATEFULSET" --namespace="$NAMESPACE" --current-replicas="$UPSCALE_REPLICA_SIZE" --replicas=$((UPSCALE_REPLICA_SIZE + 1))
  kubectl rollout status statefulset "$UPSCALE_STATEFULSET" --namespace="$NAMESPACE" --timeout=30m
  UPSCALE_REPLICA_SIZE=$((UPSCALE_REPLICA_SIZE + 1))

  # Call /shutdown on the pod manually, so that it has enough time to flush chunks. By doing standard termination, pod may not have enough time.
  # Wget is special BusyBox version. -T allows it to wait for 30m for shutdown to complete.
  POD_TO_SHUTDOWN=$DOWNSCALE_STATEFULSET-$((DOWNSCALE_REPLICA_SIZE - 1))

  echo "$(date): Triggering flush on $POD_TO_SHUTDOWN"

  # wget (BusyBox version) will fail, but we don't care ... important thing is that it has triggered shutdown.
  # -T causes wget to wait only 5 seconds, otherwise /shutdown takes a long time.
  # Preferably we would wait for /shutdown to return, but unfortunately that doesn't work (even with big timeout), wget complains with weird error.
  kubectl exec "$POD_TO_SHUTDOWN" --namespace="$NAMESPACE" -- wget -T 5 http://localhost:80/shutdown >/dev/null 2>/dev/null || true

  # While request to /shutdown completes only after flushing has finished, it unfortunately returns 204 status code,
  # which confuses wget. That is the reason why instead of waiting for /shutdown to complete, this script waits for
  # specific log messages to appear in the log file that signal start/end of data flushing.
  if kubectl logs -f "$POD_TO_SHUTDOWN" --namespace="$NAMESPACE" | grep -E -q "starting to flush all the chunks|starting to flush and ship TSDB blocks"; then
    echo "$(date): Flushing started"
  else
    echo "$(date): Flushing not started? Check logs for pod $POD_TO_SHUTDOWN"
    exit 1
  fi

  if kubectl logs -f "$POD_TO_SHUTDOWN" --namespace="$NAMESPACE" | grep -E -q "flushing of chunks complete|finished flushing and shipping TSDB blocks"; then
    echo "$(date): Flushing complete"
  else
    echo "$(date): Failed to flush? Check logs for pod $POD_TO_SHUTDOWN"
    exit 1
  fi

  echo

  echo "$(date): Scaling DOWN $DOWNSCALE_STATEFULSET to $((DOWNSCALE_REPLICA_SIZE - 1))"
  kubectl scale statefulset "$DOWNSCALE_STATEFULSET" --namespace="$NAMESPACE" --current-replicas="$DOWNSCALE_REPLICA_SIZE" --replicas=$((DOWNSCALE_REPLICA_SIZE - 1))
  kubectl rollout status statefulset "$DOWNSCALE_STATEFULSET" --namespace="$NAMESPACE" --timeout=30m
  DOWNSCALE_REPLICA_SIZE=$((DOWNSCALE_REPLICA_SIZE - 1))

  INSTANCES_TO_DOWNSCALE=$((INSTANCES_TO_DOWNSCALE - 1))

  echo "----------------------------------------"
  echo
done
