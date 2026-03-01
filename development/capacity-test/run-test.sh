#!/usr/bin/env bash
# Capacity test for Cortex blocks storage (microservices mode).
#
# Starts a local Cortex stack with separate distributor, ingester, and
# querier containers, waits for metrics to accumulate, then queries
# Prometheus for capacity-related measurements and prints per-million-series
# estimates. Because the ingester runs as a dedicated process, memory
# measurements reflect ingester-only resource usage.
#
# Prerequisites:
#   - Docker and Docker Compose
#   - Build the Cortex image first: make ./cmd/cortex/.uptodate
#
# Usage:
#   cd development/capacity-test
#   ./run-test.sh
#
# Cleanup:
#   docker compose down -v

set -euo pipefail

cd "$(dirname "$0")"

WAIT_SECS=300  # 5 minutes = 20 scrape cycles at 15s

echo "==> Starting Docker Compose stack (microservices mode)..."
docker compose up -d

wait_for_ready() {
    local name="$1"
    local url="$2"
    local log_service="$3"
    echo "==> Waiting for ${name} to be ready..."
    for i in $(seq 1 30); do
        if curl -sf "${url}" >/dev/null 2>&1; then
            echo "==> ${name} is ready."
            return 0
        fi
        if [ "$i" -eq 30 ]; then
            echo "ERROR: ${name} did not become ready in time."
            docker compose logs "${log_service}"
            exit 1
        fi
        sleep 2
    done
}

wait_for_ready "Ingester"      "http://localhost:8002/ready" "ingester"
wait_for_ready "Distributor"   "http://localhost:8001/ready" "distributor"
wait_for_ready "Store-gateway" "http://localhost:8004/ready" "store-gateway"
wait_for_ready "Querier"       "http://localhost:8003/ready" "querier"
wait_for_ready "Prometheus"    "http://localhost:9090/-/ready" "prometheus"

echo "==> Waiting ${WAIT_SECS}s for metrics to accumulate..."
sleep "$WAIT_SECS"

echo "==> Querying Prometheus for capacity metrics..."
echo ""

query_prometheus() {
    local query="$1"
    curl -s --fail-with-body "http://localhost:9090/api/v1/query" \
        --data-urlencode "query=${query}" 2>/dev/null
}

# Collect ingester-specific metrics (job="ingester")
INGESTER_RSS_JSON=$(query_prometheus 'process_resident_memory_bytes{job="ingester"}')
INGESTER_HEAP_JSON=$(query_prometheus 'go_memstats_alloc_bytes{job="ingester"}')
SERIES_JSON=$(query_prometheus 'cortex_ingester_memory_series{job="ingester"}')
SAMPLE_RATE_JSON=$(query_prometheus 'rate(cortex_ingester_ingested_samples_total{job="ingester"}[2m])')
INGESTER_CPU_JSON=$(query_prometheus 'rate(process_cpu_seconds_total{job="ingester"}[2m])')

# Collect distributor CPU for distributor throughput estimates
DIST_CPU_JSON=$(query_prometheus 'rate(process_cpu_seconds_total{job="distributor"}[2m])')
DIST_SAMPLE_RATE_JSON=$(query_prometheus 'rate(cortex_distributor_received_samples_total{job="distributor"}[2m])')

python3 - "$INGESTER_RSS_JSON" "$INGESTER_HEAP_JSON" "$SERIES_JSON" "$SAMPLE_RATE_JSON" "$INGESTER_CPU_JSON" "$DIST_CPU_JSON" "$DIST_SAMPLE_RATE_JSON" <<'PYEOF'
import json
import sys

def extract_value(json_str, label=""):
    """Extract the first numeric value from a Prometheus query result."""
    try:
        data = json.loads(json_str)
        results = data.get("data", {}).get("result", [])
        if not results:
            print(f"  WARNING: No data for {label}")
            return None
        # Sum all values (e.g. multiple tenants for sample rates)
        return sum(float(r["value"][1]) for r in results)
    except (json.JSONDecodeError, KeyError, IndexError, ValueError) as e:
        print(f"  WARNING: Failed to parse {label}: {e}")
        return None

def extract_first_value(json_str, label=""):
    """Extract only the first value (for process-level metrics like RSS/CPU)."""
    try:
        data = json.loads(json_str)
        results = data.get("data", {}).get("result", [])
        if not results:
            print(f"  WARNING: No data for {label}")
            return None
        return float(results[0]["value"][1])
    except (json.JSONDecodeError, KeyError, IndexError, ValueError) as e:
        print(f"  WARNING: Failed to parse {label}: {e}")
        return None

ingester_rss = extract_first_value(sys.argv[1], "ingester RSS")
ingester_heap = extract_first_value(sys.argv[2], "ingester heap")
series = extract_first_value(sys.argv[3], "ingester memory_series")
sample_rate = extract_value(sys.argv[4], "ingester ingested_samples rate")
ingester_cpu = extract_first_value(sys.argv[5], "ingester CPU")
dist_cpu = extract_first_value(sys.argv[6], "distributor CPU")
dist_sample_rate = extract_value(sys.argv[7], "distributor received_samples rate")

print("=" * 60)
print("  CORTEX BLOCKS STORAGE — CAPACITY MEASUREMENTS")
print("  (microservices mode: ingester-only metrics)")
print("=" * 60)
print()

# --- Raw measurements ---
print("--- Raw Measurements (Ingester) ---")
if ingester_rss is not None:
    print(f"  Ingester RSS:             {ingester_rss / 1e6:.1f} MB")
if ingester_heap is not None:
    print(f"  Ingester Go heap:         {ingester_heap / 1e6:.1f} MB")
if series is not None:
    print(f"  Active series:            {series:.0f}")
if sample_rate is not None:
    print(f"  Sample ingest rate:       {sample_rate:.1f} samples/sec")
if ingester_cpu is not None:
    print(f"  Ingester CPU:             {ingester_cpu:.3f} cores ({ingester_cpu * 100:.1f}%)")
print()

print("--- Raw Measurements (Distributor) ---")
if dist_cpu is not None:
    print(f"  Distributor CPU:          {dist_cpu:.3f} cores ({dist_cpu * 100:.1f}%)")
if dist_sample_rate is not None:
    print(f"  Distributor sample rate:  {dist_sample_rate:.1f} samples/sec")
print()

# --- Per-million-series estimates ---
print("--- Per-Million-Series Estimates ---")
print()

if series is not None and series > 0:
    scale = 1_000_000 / series

    if ingester_rss is not None:
        rss_per_m = ingester_rss * scale
        print(f"  Ingester RSS per 1M series:   {rss_per_m / 1e9:.1f} GB")
        print()

    if ingester_heap is not None:
        heap_per_m = ingester_heap * scale
        print(f"  Ingester heap per 1M series:  {heap_per_m / 1e9:.1f} GB")
        print()

    # Disk space: 30KB per series with 24h retention and RF=3 (from production-tips.md)
    # In this test RF=1, so scale accordingly
    disk_per_series_rf3 = 30 * 1024  # 30 KB
    disk_per_series_rf1 = disk_per_series_rf3 / 3  # ~10 KB for RF=1
    disk_per_m_rf1 = 1_000_000 * disk_per_series_rf1
    disk_per_m_rf3 = 1_000_000 * disk_per_series_rf3
    print(f"  Ingester disk (24h retention, RF=1): {disk_per_m_rf1 / 1e9:.1f} GB per 1M series")
    print(f"  Ingester disk (24h retention, RF=3): {disk_per_m_rf3 / 1e9:.1f} GB per 1M series")
    print(f"    (from production-tips.md: ~30KB per series with RF=3)")
    print()

    # Object storage: ~1.5 bytes/sample with Gorilla/TSDB compression
    # At 15s scrape interval: 5760 samples/series/day
    samples_per_day = 86400 / 15  # 5760
    bytes_per_sample = 1.5
    obj_per_series_day = samples_per_day * bytes_per_sample  # ~8640 bytes
    obj_per_m_day = 1_000_000 * obj_per_series_day
    print(f"  Object storage per 1M series/day:    {obj_per_m_day / 1e9:.1f} GB")
    print(f"    (~{bytes_per_sample} bytes/sample x {samples_per_day:.0f} samples/series/day)")
    print()
else:
    print("  WARNING: No series data available, cannot compute per-million estimates.")
    print()

# --- CPU estimates ---
print("--- CPU Estimates ---")
if ingester_cpu is not None and sample_rate is not None and ingester_cpu > 0:
    ingester_samples_per_core = sample_rate / ingester_cpu
    print(f"  Ingester samples/sec per core:     {ingester_samples_per_core:.0f}")
if dist_cpu is not None and dist_sample_rate is not None and dist_cpu > 0:
    dist_samples_per_core = dist_sample_rate / dist_cpu
    print(f"  Distributor samples/sec per core:   {dist_samples_per_core:.0f}")
if (ingester_cpu is None or ingester_cpu == 0) and (dist_cpu is None or dist_cpu == 0):
    print("  WARNING: Insufficient data for CPU estimates.")
print()

# --- Caveats ---
print("--- Caveats ---")
print("  - Microservices mode: ingester RSS reflects ingester-only memory.")
if series is not None:
    print(f"  - Small series count ({series:.0f}) means fixed overhead dominates;")
    print(f"    per-series memory estimates will be higher than at scale.")
print("  - Disk and object storage numbers are calculated from production-tips.md")
print("    and TSDB compression ratios, not directly measured in this test.")
print()

print("=" * 60)
PYEOF

echo ""
echo "==> Test stack is running."
echo "    Grafana:      http://localhost:3000"
echo "    Distributor:  http://localhost:8001"
echo "    Ingester:     http://localhost:8002"
echo "    Querier:      http://localhost:8003"
echo "    Prometheus:   http://localhost:9090"
echo ""
echo "    To clean up: docker compose down -v"
