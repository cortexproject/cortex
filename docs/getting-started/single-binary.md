---
title: "Single Binary Mode"
linkTitle: "Single Binary Mode"
weight: 2
slug: "single-binary"
---

This guide will help you get Cortex running in single-binary mode using Docker Compose. In this mode, all Cortex components run in a single process, making it perfect for learning, development, and testing.

**Time to complete:** ~15 minutes

## What You'll Learn

- How to run Cortex with Docker Compose
- How to send metrics from Prometheus to Cortex using remote_write
- How to query metrics stored in Cortex using Grafana
- How to configure recording rules and alerting rules
- How to set up the Cortex Alertmanager

## Prerequisites

### Software Requirements

- [Docker](https://docs.docker.com/get-docker/) (v20.10+)
- [Docker Compose](https://docs.docker.com/compose/install/) (v2.30+)

### System Requirements

- 4GB RAM minimum
- 10GB disk space
- Linux, macOS, or Windows with WSL2

### Optional Tools

- [cortextool](https://github.com/cortexproject/cortex-tools/) - For managing rules and alerts (we'll use Docker to run this)

## Architecture

This setup creates the following services:

```
┌─────────────┐     remote_write     ┌─────────────┐
│ Prometheus  │ ───────────────────> │   Cortex    │
│             │                      │  (single)   │
└─────────────┘                      └─────────────┘
                                             │
                                             │ stores blocks
                                             ▼
┌─────────────┐                      ┌─────────────┐
│   Grafana   │ ────── queries ────> │  SeaweedFS  │
│   Perses    │                      │     (S3)    │
└─────────────┘                      └─────────────┘
```

**Components:**
- **SeaweedFS**: S3-compatible object storage for storing metric blocks
- **Cortex**: Single-process Cortex instance with all components (distributor, ingester, querier, compactor, etc.)
- **Prometheus**: Scrapes its own metrics and sends them to Cortex
- **Grafana**: Visualizes metrics stored in Cortex
- **Perses**: Modern dashboard alternative (optional)

## Step 1: Clone the Repository

```sh
git clone https://github.com/cortexproject/cortex.git
cd cortex/docs/getting-started
```

The `getting-started` directory contains all the configuration files needed for this guide.

## Step 2: Start the Services

```sh
docker compose up -d
```

This command starts all services in the background. Docker Compose will:
1. Pull required images (first time only)
2. Start SeaweedFS (S3-compatible storage)
3. Initialize S3 buckets
4. Start Cortex
5. Start Prometheus (configured to send metrics to Cortex)
6. Start Grafana (pre-configured with Cortex datasource)

**What's happening?** Check the logs:
```sh
# View all logs
docker compose logs -f

# View Cortex logs only
docker compose logs -f cortex
```

## Step 3: Verify Services Are Running

After ~30 seconds, all services should be healthy. Verify by checking:

```sh
docker compose ps
```

You should see all services with status "Up" or "healthy".

### Access the UIs

Open these URLs in your browser:

- **Cortex**: [http://localhost:9009](http://localhost:9009) - Admin interface and ring status
- **Prometheus**: [http://localhost:9090](http://localhost:9090) - Prometheus UI
- **Grafana**: [http://localhost:3000](http://localhost:3000) - Dashboards (no auth needed)
- **SeaweedFS S3 API**: http://localhost:8333 - S3-compatible API (use curl with `--user any:any`)

## Step 4: Verify Data Flow

Let's verify that metrics are flowing from Prometheus → Cortex → Grafana.

### Check Prometheus is Sending Metrics

1. Open [Prometheus](http://localhost:9090)
2. Go to Status → Targets
3. Verify the targets are UP
4. Go to Query - you should see `prometheus_remote_storage_samples_total` increasing

### Query Metrics in Cortex

Test that Cortex is receiving metrics:

```sh
curl -H "X-Scope-OrgID: cortex" "http://localhost:9009/prometheus/api/v1/query?query=up" | jq
```

You should see JSON output with metrics data.

**Note:** The `X-Scope-OrgID` header specifies which tenant's data to query. Cortex is multi-tenant by default. Prometheus automatically adds this header when writing metrics via remote_write.

### View Metrics in Grafana

1. Open [Grafana](http://localhost:3000) (login: `admin` / `admin`)
2. Go to [Explore](http://localhost:3000/explore)
3. Select the "Cortex" datasource
4. Run a query: `up`
5. You should see metrics from Prometheus!

### View Cortex Dashboards

Pre-built dashboards are available at [Dashboards](http://localhost:3000/dashboards?tag=cortex):

- **Cortex / Writes**: Monitor metric ingestion
- **Cortex / Reads**: Monitor query performance
- **Cortex / Object Store**: Monitor block storage

## Step 5: Configure Recording and Alerting Rules (Optional)

Cortex can evaluate PromQL recording rules and alerting rules, similar to Prometheus. This is optional but demonstrates an important Cortex feature.

**What are these?**
- **Recording rules**: Pre-compute expensive queries and store results as new metrics
- **Alerting rules**: Define conditions that trigger alerts

The repository includes example rules in `rules.yaml` and `alerts.yaml`.

### Load Rules into Cortex

**For Linux users:**
```sh
docker run --network host \
  -v "$(pwd):/workspace" -w /workspace \
  quay.io/cortexproject/cortex-tools:v0.17.0 \
  rules sync rules.yaml alerts.yaml --id cortex --address http://localhost:9009
```

**For macOS/Windows users:**
```sh
docker run --network cortex-docs-getting-started_default \
  -v "$(pwd):/workspace" -w /workspace \
  quay.io/cortexproject/cortex-tools:v0.17.0 \
  rules sync rules.yaml alerts.yaml --id cortex --address http://cortex:9009
```

**Note:** The `--id cortex` flag specifies the tenant ID. Cortex is multi-tenant, so rules are namespaced by tenant.

### Verify Rules Are Loaded

View rules in Grafana: [Alerting → Alert rules](http://localhost:3000/alerting/list?view=list&search=datasource:Cortex)

Or check via API:
```sh
curl -H "X-Scope-OrgID: cortex" "http://localhost:9009/prometheus/api/v1/rules" | jq
```

## Step 6: Configure Alertmanager (Optional)

Cortex includes a multi-tenant Alertmanager that receives alerts from the ruler.

### Load Alertmanager Configuration

**For Linux users:**
```sh
docker run --network host \
  -v "$(pwd):/workspace" -w /workspace \
  quay.io/cortexproject/cortex-tools:v0.17.0 \
  alertmanager load alertmanager-config.yaml --id cortex --address http://localhost:9009
```

**For macOS/Windows users:**
```sh
docker run --network cortex-docs-getting-started_default \
  -v "$(pwd):/workspace" -w /workspace \
  quay.io/cortexproject/cortex-tools:v0.17.0 \
  alertmanager load alertmanager-config.yaml --id cortex --address http://cortex:9009
```

### View Alertmanager in Grafana

Configure Alertmanager notification policies in Grafana: [Alerting → Notification policies](http://localhost:3000/alerting/notifications?search=&alertmanager=Cortex%20Alertmanager)

## Explore and Experiment

Now that everything is running, try these experiments to learn how Cortex works:

### Experiment 1: Stop the Ingester

Cortex runs all components in one process, so stopping Cortex simulates an ingester failure.

```sh
docker compose stop cortex
```

**Observe:**
- Prometheus continues running and queues samples
- Grafana queries fail (no ingesters available)
- Metrics are NOT lost - Prometheus will retry

**Restart Cortex:**
```sh
docker compose start cortex
```

**Result:** Prometheus catches up by sending queued samples. Check the Cortex / Writes dashboard to see the backlog being processed.

### Experiment 2: Query Old vs Recent Data

Cortex stores recent data (last ~2 hours) in memory and older data in object storage (S3).

**Query recent metrics (from ingester memory):**
```sh
curl "http://localhost:9009/prometheus/api/v1/query?query=up" | jq
```

**After 2+ hours, query old metrics (from S3 blocks):**
```sh
curl "http://localhost:9009/prometheus/api/v1/query?query=up[24h]" | jq
```

**Observe:** Both queries work! Cortex seamlessly queries both sources.

### Experiment 3: Compare Prometheus vs Cortex

**In Prometheus:** [Query `up`](http://localhost:9090/graph?g0.expr=up)

**In Grafana (Cortex datasource):** [Query `up`](http://localhost:3000/explore)

**Are they the same?** Initially yes, but after Prometheus sends data to Cortex via remote_write, the data diverges:
- Prometheus has local storage (limited retention)
- Cortex has long-term storage in S3

### Experiment 4: Explore the Ring

Cortex uses a hash ring for consistent hashing of time series to ingesters.

View the ring status: [http://localhost:9009/ring](http://localhost:9009/ring)

In single-binary mode, you'll see one ingester. In microservices mode, you'd see multiple ingesters.

### Experiment 5: Inspect Object Storage

SeaweedFS stores Cortex blocks. You can inspect them using the S3 API:

**List buckets:**
```sh
curl --aws-sigv4 "aws:amz:local:seaweedfs" --user "any:any" http://localhost:8333
```

**List objects in the cortex-blocks bucket:**
```sh
curl --aws-sigv4 "aws:amz:local:seaweedfs" --user "any:any" http://localhost:8333/cortex-blocks?list-type=2
```

You'll see:
- `cortex/` directory (tenant ID)
- Block directories named by ULID (e.g., `01J8KRQ7M8...`)
- Each block contains `index`, `chunks/`, and `meta.json`

**Tip:** You can also use the AWS CLI with SeaweedFS:
```sh
export AWS_ACCESS_KEY_ID=any
export AWS_SECRET_ACCESS_KEY=any
aws --endpoint-url=http://localhost:8333 s3 ls s3://cortex-blocks/
```

## Configuration Files

This setup uses several configuration files. Here's what each does:

| File                             | Purpose                                                       |
|----------------------------------|---------------------------------------------------------------|
| `docker-compose.yaml`            | Defines all services (Cortex, Prometheus, Grafana, SeaweedFS) |
| `cortex-config.yaml`             | Cortex configuration (storage, limits, components)            |
| `prometheus-config.yaml`         | Prometheus configuration with remote_write to Cortex          |
| `grafana-datasource-docker.yaml` | Grafana datasource pointing to Cortex                         |
| `rules.yaml`                     | Example recording rules                                       |
| `alerts.yaml`                    | Example alerting rules                                        |
| `alertmanager-config.yaml`       | Alertmanager configuration                                    |

**Want to customize?** Edit these files and restart services:
```sh
docker compose restart cortex
```

## Troubleshooting

### Services won't start
```sh
# Check logs
docker compose logs

# Check port conflicts
lsof -i :9009  # Cortex
lsof -i :9090  # Prometheus
lsof -i :3000  # Grafana
```

### No metrics in Grafana
1. Check Prometheus is sending metrics: [Status → Targets](http://localhost:9090/targets)
2. Check Cortex is receiving metrics: `curl "http://localhost:9009/prometheus/api/v1/query?query=up"`
3. Check Grafana datasource: Settings → Data sources → Cortex → Test

### cortextool fails on macOS/Windows
The `--network host` flag doesn't work on macOS/Windows. Use the Docker network name instead:
```sh
docker run --network cortex-docs-getting-started_default ...
```

### Out of memory errors
Increase Docker's memory limit to 4GB or more:
- Docker Desktop → Settings → Resources → Memory

## Clean Up

When you're done, stop and remove all services:

```sh
docker compose down -v
```

The `-v` flag removes volumes (stored data). Omit it to keep data between runs.

## Next Steps

Congratulations! You've successfully run Cortex in single-binary mode. Here's what to explore next:

1. **Try Microservices Mode**: [Get started with microservices mode →](microservices.md)
2. **Learn the Architecture**: [Understand Cortex's design →](../architecture.md)
3. **Production Deployment**: [Run Cortex on Kubernetes →](../guides/running-cortex-on-kubernetes.md)
4. **Deep Dive into Blocks Storage**: [Learn about blocks storage →](../blocks-storage/_index.md)
5. **Configure Multi-tenancy**: [Set up authentication →](../guides/authentication-and-authorisation.md)

## Additional Resources

- [Cortex Documentation](https://cortexmetrics.io/docs/)
- [Cortex Helm Chart](https://github.com/cortexproject/cortex-helm-chart)
- [cortextool CLI](https://github.com/cortexproject/cortex-tools)
- [CNCF Slack #cortex](https://cloud-native.slack.com/archives/cortex)
