---
date: 2025-01-14
title: "Introducing READONLY State: Gradual and Safe Ingester Scaling"
linkTitle: READONLY Ingester Scaling
tags: [ "blog", "cortex", "ingester", "scaling" ]
categories: [ "blog" ]
projects: [ "cortex" ]
description: >
  Learn about Cortex's new READONLY state for ingesters introduced in version 1.19.0 that enables gradual, safe scaling down operations without data loss or performance impact.
author: Cortex Team
---

## Introduction

Scaling down ingesters in Cortex has traditionally been a complex and risky operation. The conventional approach required setting `querier.query-store-after=0s`, which forces all queries to hit storage directly, significantly impacting performance. With Cortex 1.19.0, we introduced a new **READONLY state** for ingesters that changes how you can safely scale down your Cortex clusters.

## Why Traditional Scaling Falls Short

The legacy approach to ingester scaling had several issues:

**Performance Impact**: Setting `querier.query-store-after=0s` forces all queries to bypass ingesters entirely, increasing query latency and storage load.

**Operational Complexity**: Traditional scaling required coordinating configuration changes across multiple components, precise timing, manual monitoring of bucket scanning intervals, and scaling ingesters one by one with waiting periods between each shutdown.

**Risk of Data Loss**: Without proper coordination, scaling down could result in data loss if in-memory data wasn't properly flushed to storage before ingester termination.

## What is the READONLY State?

The READONLY state addresses these challenges. When an ingester transitions to READONLY state:

- **Stops accepting new writes** - Push requests are rejected and redistributed to ACTIVE ingesters
- **Continues serving queries** - Existing data remains available, maintaining query performance  
- **Gradually ages out data** - Data naturally expires according to your retention settings
- **Enables safe removal** - Ingesters can be terminated once data has aged out

## How to Use READONLY State

### Step 1: Transition to READONLY

```bash
# Set multiple ingesters to READONLY simultaneously
curl -X POST http://ingester-1:8080/ingester/mode -d '{"mode": "READONLY"}'
curl -X POST http://ingester-2:8080/ingester/mode -d '{"mode": "READONLY"}'
curl -X POST http://ingester-3:8080/ingester/mode -d '{"mode": "READONLY"}'
```

### Step 2: Monitor Data Status (Optional)

```bash
# Check user statistics and loaded blocks on the ingester
curl http://ingester-1:8080/ingester/all_user_stats
```

### Step 3: Choose Removal Strategy

You have three options:

- **Immediate removal**: Safe for service availability but may impact query performance
- **Conservative removal**: Wait for `querier.query-ingesters-within` duration (recommended)
- **Complete data aging**: Wait for full retention period

### Step 4: Remove Ingesters

```bash
# Terminate the ingester processes
kubectl delete pod ingester-1 ingester-2 ingester-3
```

## Timeline Example

For a cluster with `querier.query-ingesters-within=5h`:

- **T0**: Set ingesters to READONLY state
- **T1**: Ingesters stop receiving new data but continue serving queries
- **T2 (T0 + 5h)**: Ingesters no longer receive query requests (safe to remove)
- **T3 (T0 + retention_period)**: All blocks naturally removed from ingesters

**Any time after T2 is safe for removal without service impact.**

## Benefits

### Performance Preservation
Unlike the traditional approach, READONLY ingesters continue serving queries, maintaining performance during the scaling transition.

### Operational Simplicity
- No configuration changes required across multiple components
- Batch operations supported - multiple ingesters can transition simultaneously (no more "one by one" requirement)
- No waiting periods between ingester transitions
- Flexible timing - remove ingesters when convenient
- Reversible operations - ingesters can return to ACTIVE state if needed

### Enhanced Safety
- Gradual data aging without manual intervention
- Data remains available during transition
- Monitoring capabilities with `/ingester/all_user_stats` endpoint

## Practical Examples

### Basic READONLY Scaling

```bash
#!/bin/bash
INGESTERS_TO_SCALE=("ingester-1" "ingester-2" "ingester-3")
WAIT_DURATION="5h"

# Set ingesters to READONLY
for ingester in "${INGESTERS_TO_SCALE[@]}"; do
    echo "Setting $ingester to READONLY..."
    curl -X POST http://$ingester:8080/ingester/mode -d '{"mode": "READONLY"}'
done

# Wait for safe removal window
echo "Waiting $WAIT_DURATION for safe removal..."
sleep $WAIT_DURATION

# Remove ingesters
for ingester in "${INGESTERS_TO_SCALE[@]}"; do
    echo "Removing $ingester..."
    kubectl delete pod $ingester
done
```

### Advanced: Check for Empty Users Before Removal

```bash
#!/bin/bash
check_ingester_ready() {
    local ingester=$1
    local response=$(curl -s http://$ingester:8080/ingester/all_user_stats)
    
    # Empty array "[]" indicates no users/data remaining
    if [[ "$response" == "[]" ]]; then
        return 0  # Ready for removal
    else
        return 1  # Still has user data
    fi
}

INGESTERS_TO_SCALE=("ingester-1" "ingester-2" "ingester-3")

# Set ingesters to READONLY
for ingester in "${INGESTERS_TO_SCALE[@]}"; do
    echo "Setting $ingester to READONLY..."
    curl -X POST http://$ingester:8080/ingester/mode -d '{"mode": "READONLY"}'
done

# Wait and check for data removal
for ingester in "${INGESTERS_TO_SCALE[@]}"; do
    echo "Waiting for $ingester to be ready for removal..."
    while ! check_ingester_ready $ingester; do
        echo "$ingester still has user data, waiting 30s..."
        sleep 30
    done
    
    echo "Removing $ingester (no user data remaining)..."
    kubectl delete pod $ingester
done
```

## Best Practices

- **Test in non-production first** to validate the process with your configuration
- **Scale gradually** - don't remove too many ingesters simultaneously
- **Monitor throughout** - watch metrics during the entire process
- **Understand your query patterns** - know your `querier.query-ingesters-within` setting

## Emergency Rollback

If issues arise, return ingesters to ACTIVE state:

```bash
# Revert to ACTIVE state
curl -X POST http://ingester-1:8080/ingester/mode -d '{"mode": "ACTIVE"}'
```

## Conclusion

The READONLY state improves Cortex's operational capabilities. This feature makes scaling operations safer, simpler, more flexible, and more performant than the traditional approach. Configuration changes across multiple components are no longer required - set ingesters to READONLY and remove them when convenient.

For detailed information and examples, check out our [Ingesters Scaling Guide](../../docs/guides/ingesters-scaling-up-and-down/).