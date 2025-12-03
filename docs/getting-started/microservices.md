---
title: "Microservices Mode"
linkTitle: "Microservices Mode"
weight: 3
slug: "microservices"
---

This guide will help you run Cortex in microservices mode using Kubernetes (Kind). In this mode, each Cortex component runs as an independent service, mirroring how Cortex runs in production.

**Time to complete:** ~30 minutes

## What You'll Learn

- How to deploy Cortex on Kubernetes with Helm
- How Cortex microservices communicate with each other
- How to configure Prometheus to send metrics to a distributed Cortex
- How to query metrics across multiple Cortex services
- How to configure rules and alerts in a distributed setup

## Prerequisites

### Software Requirements

- [Kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) (Kubernetes in Docker)
- [kubectl](https://kubernetes.io/docs/tasks/tools/) (v1.20+)
- [Helm](https://helm.sh/docs/intro/install/) (v3.0+)
- [Docker](https://docs.docker.com/get-docker/) (required by Kind)

### System Requirements

- 8GB RAM minimum (for local Kubernetes cluster)
- 20GB disk space
- Linux, macOS, or Windows with WSL2

### Optional Tools

- [cortextool](https://github.com/cortexproject/cortex-tools/) - For managing rules and alerts
- [jq](https://jqlang.github.io/jq/) - For parsing JSON responses

## Architecture

This setup creates a production-like Cortex deployment with independent microservices:

```
                            ┌─────────────────────────────────────┐
                            │      Kubernetes Cluster (Kind)      │
                            │                                     │
┌─────────────┐             │  ┌──────────────┐  ┌──────────────┐ │
│ Prometheus  │────remote───┼─>│ Distributor  │  │   Ingester   │ │
│             │    write    │  └──────────────┘  └──────────────┘ │
└─────────────┘             │          │                │         │
                            │          │                │         │
                            │          └────────────────┘         │
                            │                   │                 │
                            │                   ▼                 │
      ┌─────────────┐       │  ┌──────────────┐  ┌──────────────┐ │
      │   Grafana   │◄──────┼──┤   Querier    │  │  SeaweedFS   │ │
      └─────────────┘       │  └──────────────┘  │     (S3)     │ │
                            │          ▲         └──────────────┘ │
                            │          │                │         │
                            │  ┌──────────────┐         │         │
                            │  │Store Gateway │◄────────┘         │
                            │  └──────────────┘                   │
                            │                                     │
                            │  ┌──────────────┐                   │
                            │  │  Compactor   │                   │
                            │  └──────────────┘                   │
                            │                                     │
                            │  ┌──────────────┐                   │
                            │  │    Ruler     │                   │
                            │  └──────────────┘                   │
                            └─────────────────────────────────────┘
```

**Components:**
- **Distributor**: Receives metrics from Prometheus, validates, and forwards to ingesters
- **Ingester**: Stores recent metrics in memory and flushes to S3
- **Querier**: Queries both ingesters (recent data) and store-gateway (historical data)
- **Store Gateway**: Queries historical blocks from S3
- **Compactor**: Compacts and deduplicates blocks in S3
- **Ruler**: Evaluates recording and alerting rules
- **Alertmanager**: Manages alerts (optional)

## Step 1: Create a Kubernetes Cluster

Create a local Kubernetes cluster using Kind:

```sh
kind create cluster --name cortex-demo
```

**What's happening?**
- Kind creates a Kubernetes cluster running inside Docker
- This takes ~2 minutes
- The cluster is named `cortex-demo`

**Verify the cluster:**
```sh
kubectl cluster-info
kubectl get nodes
```

You should see one node in the `Ready` state.

## Step 2: Configure Helm Repositories

Add the Helm repositories for Cortex, Grafana, and Prometheus:

```sh
helm repo add cortex-helm https://cortexproject.github.io/cortex-helm-chart
helm repo add grafana https://grafana.github.io/helm-charts
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
```

## Step 3: Clone the Repository

```sh
git clone https://github.com/cortexproject/cortex.git
cd cortex/docs/getting-started
```

The `getting-started` directory contains Helm values files and Kubernetes manifests.

## Step 4: Create the Cortex Namespace

```sh
kubectl create namespace cortex
```

All Cortex components will be deployed in this namespace.

## Step 5: Deploy SeaweedFS (S3-Compatible Storage)

Cortex requires object storage for blocks. We'll use SeaweedFS as an S3-compatible alternative.

### Deploy SeaweedFS

```sh
kubectl --namespace cortex apply -f seaweedfs.yaml --wait --timeout=5m
```

### Wait for SeaweedFS to be Ready

```sh
kubectl --namespace cortex wait --for=condition=ready pod -l app=seaweedfs --timeout=5m
```

**What's happening?**
- SeaweedFS starts as a StatefulSet
- It provides an S3-compatible API on port 8333
- Initial startup takes ~1 minute

### Create S3 Buckets

SeaweedFS needs buckets for Cortex data. First, port-forward to SeaweedFS:

```sh
kubectl --namespace cortex port-forward svc/seaweedfs 8333 &
```

**Note:** This runs in the background. If the command fails with "port already in use", kill the existing process:
```sh
lsof -ti:8333 | xargs kill
```

Now create the buckets:

```sh
for bucket in cortex-blocks cortex-ruler cortex-alertmanager; do
  curl --aws-sigv4 "aws:amz:local:seaweedfs" \
       --user "any:any" \
       -X PUT http://localhost:8333/$bucket
done
```

**What are these buckets?**
- `cortex-blocks`: Stores TSDB blocks (metric data)
- `cortex-ruler`: Stores ruler configuration
- `cortex-alertmanager`: Stores alertmanager configuration

**Verify buckets were created:**
```sh
curl --aws-sigv4 "aws:amz:local:seaweedfs" --user "any:any" http://localhost:8333
```

## Step 6: Deploy Cortex

Deploy Cortex using the Helm chart with the provided values file:

```sh
helm upgrade --install \
  --version=2.4.0 \
  --namespace cortex \
  cortex cortex-helm/cortex \
  -f cortex-values.yaml \
  --wait
```

**What's in `cortex-values.yaml`?**
- Configures blocks storage to use SeaweedFS
- Sets resource limits for each component
- Enables the ruler and alertmanager
- Configures the ingester to use 3 replicas

**This takes ~5 minutes.** The `--wait` flag waits for all pods to be ready.

### Verify Cortex Components

```sh
kubectl --namespace cortex get pods
```

You should see pods for each Cortex component:
- `cortex-distributor-*`
- `cortex-ingester-*` (multiple replicas)
- `cortex-querier-*`
- `cortex-query-frontend-*`
- `cortex-store-gateway-*`
- `cortex-compactor-*`
- `cortex-ruler-*`
- `cortex-nginx-*` (reverse proxy)

**Check logs if pods aren't starting:**
```sh
kubectl --namespace cortex logs -l app.kubernetes.io/name=cortex -f --max-log-requests 20
```

## Step 7: Deploy Prometheus

Deploy Prometheus to scrape metrics from Kubernetes and send them to Cortex:

```sh
helm upgrade --install \
  --version=25.20.1 \
  --namespace cortex \
  prometheus prometheus-community/prometheus \
  -f prometheus-values.yaml \
  --wait
```

**What's in `prometheus-values.yaml`?**
- Configures remote_write to send metrics to `cortex-distributor`
- Sets up scrape configs for Kubernetes services
- Adds the `X-Scope-OrgID: cortex` header for multi-tenancy

**Verify Prometheus is running:**
```sh
kubectl --namespace cortex get pods -l app.kubernetes.io/name=prometheus
```

## Step 8: Deploy Grafana

Deploy Grafana to visualize metrics from Cortex:

```sh
helm upgrade --install \
  --version=7.3.9 \
  --namespace cortex \
  grafana grafana/grafana \
  -f grafana-values.yaml \
  --wait
```

**What's in `grafana-values.yaml`?**

- Configures Cortex as a datasource (pointing to `cortex-nginx`)
- Enables sidecar for loading dashboards from ConfigMaps

### Load Cortex Dashboards

Create ConfigMaps for Cortex operational dashboards:

```sh
for dashboard in $(ls dashboards); do
  basename=$(basename -s .json $dashboard)
  cmname=grafana-dashboard-$basename
  kubectl create --namespace cortex configmap $cmname \
    --from-file=$dashboard=dashboards/$dashboard \
    --save-config=true -o yaml --dry-run=client | kubectl apply -f -
  kubectl patch --namespace cortex configmap $cmname \
    -p '{"metadata":{"labels":{"grafana_dashboard":"1"}}}'
done
```

**What's happening?**

- Each dashboard JSON is stored in a ConfigMap
- The `grafana_dashboard` label tells Grafana's sidecar to load it
- Dashboards appear automatically in Grafana

## Step 9: Access the Services

Port-forward to access Grafana:

```sh
kubectl --namespace cortex port-forward deploy/grafana 3000 &
```

Open [Grafana](http://localhost:3000).

**For other services, port-forward as needed:**

```sh
# Prometheus
kubectl --namespace cortex port-forward deploy/prometheus-server 9090 &

# Cortex Nginx (API gateway)
kubectl --namespace cortex port-forward svc/cortex-nginx 8080:80 &

# Cortex Distributor (admin UI)
kubectl --namespace cortex port-forward deploy/cortex-distributor 9009:8080 &
```

**Tip:** Open a new terminal for each port-forward, or use `&` to run in the background.

## Step 10: Verify Data Flow

### Check Prometheus is Sending Metrics

Port-forward to Prometheus:
```sh
kubectl --namespace cortex port-forward deploy/prometheus-server 9090 &
```

Open [Prometheus](http://localhost:9090) and:

1. Go to Status → Targets - verify targets are UP
2. Go to Query - verify `prometheus_remote_storage_samples_total` is increasing

### Query Metrics in Cortex

Port-forward to the Cortex API:

```sh
kubectl --namespace cortex port-forward svc/cortex-nginx 8080:80 &
```

Query metrics:

```sh
curl -H "X-Scope-OrgID: cortex" \
  "http://localhost:8080/prometheus/api/v1/query?query=up" | jq
```

**Note:** The `X-Scope-OrgID` header specifies the tenant. Cortex is multi-tenant.

### View Metrics in Grafana

1. Open [Grafana](http://localhost:3000)
2. Go to [Explore](http://localhost:3000/explore)
3. Select the "Cortex" datasource
4. Run a query: `up`
5. You should see metrics from Prometheus!

### View Cortex Dashboards

Navigate to [Dashboards](http://localhost:3000/dashboards?tag=cortex) to see:

- **Cortex / Writes**: Monitor the distributor and ingesters
- **Cortex / Reads**: Monitor the querier and query-frontend
- **Cortex / Object Store**: Monitor block storage operations
- **Cortex / Compactor**: Monitor block compaction

## Step 11: Configure Rules and Alerts (Optional)

### Port-Forward to Cortex API

If not already running:
```sh
kubectl --namespace cortex port-forward svc/cortex-nginx 8080:80 &
```

### Install cortextool (if needed)

**macOS:**
```sh
brew install cortexproject/tap/cortex-tools
```

**Linux:**
```sh
wget https://github.com/cortexproject/cortex-tools/releases/download/v0.17.0/cortex-tools_0.17.0_linux_x86_64.tar.gz
tar -xzf cortex-tools_0.17.0_linux_x86_64.tar.gz
sudo mv cortextool /usr/local/bin/
```

**Or use Docker:**
```sh
alias cortextool="docker run --rm --network host -v $(pwd):/workspace -w /workspace quay.io/cortexproject/cortex-tools:v0.17.0 cortextool"
```

### Load Recording and Alerting Rules

```sh
cortextool rules sync rules.yaml alerts.yaml \
  --id cortex \
  --address http://localhost:8080
```

**What's happening?**
- `rules.yaml` contains recording rules (pre-computed PromQL queries)
- `alerts.yaml` contains alerting rules (conditions that trigger alerts)
- Rules are stored in S3 and evaluated by the ruler component

### Verify Rules Are Loaded

View rules in Grafana: [Alerting → Alert rules](http://localhost:3000/alerting/list?view=list&search=datasource:Cortex)

Or check via API:
```sh
curl -H "X-Scope-OrgID: cortex" \
  "http://localhost:8080/prometheus/api/v1/rules" | jq
```

## Step 12: Configure Alertmanager (Optional)

Load Alertmanager configuration:

```sh
cortextool alertmanager load alertmanager-config.yaml \
  --id cortex \
  --address http://localhost:8080
```

**Verify in Grafana:** [Alerting → Notification policies](http://localhost:3000/alerting/notifications?search=&alertmanager=Cortex%20Alertmanager)

## Explore and Experiment

### Experiment 1: Scale Ingesters

Cortex uses a hash ring to distribute time series across ingesters. Let's add more ingesters:

```sh
kubectl --namespace cortex scale deployment cortex-ingester --replicas=5
```

**Observe:**
- New ingesters join the ring
- Time series are rebalanced
- View the ring: Port-forward to a distributor and visit [http://localhost:9009/ingester/ring](http://localhost:9009/ingester/ring)

```sh
kubectl --namespace cortex port-forward deploy/cortex-distributor 9009:8080
```

**Scale back down:**
```sh
kubectl --namespace cortex scale deployment cortex-ingester --replicas=3
```

### Experiment 2: Kill an Ingester

Simulate a failure by deleting a single ingester pod. This demonstrates Cortex's resilience.

**Step 1: List the ingester pods**
```sh
kubectl --namespace cortex get pods -l app.kubernetes.io/component=ingester
```

You should see multiple ingester pods (typically 3 replicas).

**Step 2: Delete one specific ingester pod**
```sh
# Replace <pod-name> with an actual pod name from the list above
kubectl --namespace cortex delete pod <pod-name> --force --grace-period=0
```

Example:
```sh
kubectl --namespace cortex delete pod cortex-ingester-76d95464d8 --force --grace-period=0
```

**Observe:**
- Queries still work (data is replicated across the remaining ingesters)
- Kubernetes automatically restarts the deleted pod
- The new ingester rejoins the ring
- Check the ring status: Port-forward to a distributor and visit [http://localhost:9009/ingester/ring](http://localhost:9009/ingester/ring)

**Why it works:** Cortex replicates data across multiple ingesters (default: 3 replicas), so losing one ingester doesn't cause data loss.

### Experiment 3: View Component Logs

See what each component is doing:

```sh
# Distributor logs (receives metrics from Prometheus)
kubectl --namespace cortex logs -l app.kubernetes.io/component=distributor -f

# Ingester logs (stores metrics in memory)
kubectl --namespace cortex logs -l app.kubernetes.io/component=ingester -f

# Querier logs (handles PromQL queries)
kubectl --namespace cortex logs -l app.kubernetes.io/component=querier -f

# Compactor logs (compacts blocks in S3)
kubectl --namespace cortex logs -l app.kubernetes.io/component=compactor -f
```

### Experiment 4: Inspect the Ring

Cortex uses a hash ring for consistent hashing. View the ingester ring:

```sh
kubectl --namespace cortex port-forward deploy/cortex-distributor 9009:8080 &
```

Open [http://localhost:9009/ingester/ring](http://localhost:9009/ingester/ring) to see:
- Ingester tokens in the ring
- Health status of each ingester
- Token ownership ranges

### Experiment 5: Check Block Storage

View blocks in SeaweedFS using the S3 API:

```sh
kubectl --namespace cortex port-forward svc/seaweedfs 8333 &
```

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
- Block directories (named by ULID)
- Each block contains `index`, `chunks/`, and `meta.json`

**Tip:** You can also use the AWS CLI:
```sh
export AWS_ACCESS_KEY_ID=any
export AWS_SECRET_ACCESS_KEY=any
aws --endpoint-url=http://localhost:8333 s3 ls s3://cortex-blocks/cortex/
```

## Configuration Files

| File                       | Purpose                                                   |
|----------------------------|-----------------------------------------------------------|
| `seaweedfs.yaml`           | Kubernetes manifest for SeaweedFS (S3)                    |
| `cortex-values.yaml`       | Helm values for Cortex (component config, storage)        |
| `prometheus-values.yaml`   | Helm values for Prometheus (scrape configs, remote_write) |
| `grafana-values.yaml`      | Helm values for Grafana (datasources, dashboards)         |
| `rules.yaml`               | Recording rules for the ruler                             |
| `alerts.yaml`              | Alerting rules for the ruler                              |
| `alertmanager-config.yaml` | Alertmanager notification configuration                   |

**Want to customize?** Edit the Helm values files and upgrade:
```sh
helm upgrade --namespace cortex cortex cortex-helm/cortex -f cortex-values.yaml
```

## Troubleshooting

### Pods are pending or crashing
```sh
# Check pod status
kubectl --namespace cortex get pods

# Describe a pod to see events
kubectl --namespace cortex describe pod <pod-name>

# Check logs
kubectl --namespace cortex logs <pod-name>
```

### Ingesters won't start
- Check that SeaweedFS is running and buckets are created
- Verify the `cortex-values.yaml` has correct S3 config
- Check logs: `kubectl --namespace cortex logs -l app.kubernetes.io/component=ingester`

### No metrics in Grafana
1. Check Prometheus remote_write is configured: `kubectl --namespace cortex get configmap prometheus-server -o yaml`
2. Check distributor is receiving metrics: `kubectl --namespace cortex logs -l app.kubernetes.io/component=distributor`
3. Test Cortex API directly: `curl -H "X-Scope-OrgID: cortex" "http://localhost:8080/prometheus/api/v1/query?query=up"`

### Port-forward fails
- Check if port is already in use: `lsof -i :<port>`
- Kill existing process: `lsof -ti:<port> | xargs kill`

### Out of memory
Kind requires Docker to have enough resources:
- Docker Desktop → Settings → Resources → Memory (set to 8GB+)

### cortextool commands fail
- Make sure port-forward is running: `kubectl --namespace cortex port-forward svc/cortex-nginx 8080:80 &`
- Verify Cortex is responding: `curl http://localhost:8080/ready`

## Clean Up

### Remove Port-Forwards

```sh
# Kill all kubectl port-forwards
killall kubectl

# Or kill specific port-forwards
lsof -ti:3000,8080,9009,9090 | xargs kill
```

### Delete the Kind Cluster

```sh
kind delete cluster --name cortex-demo
```

This removes all Kubernetes resources and the Kind cluster.

## Next Steps

Congratulations! You've successfully deployed Cortex in microservices mode on Kubernetes. Here's what to explore next:

1. **Production Deployment**: [Run Cortex on real Kubernetes →](../guides/running-cortex-on-kubernetes.md)
2. **Learn the Architecture**: [Understand each component →](../architecture.md)
3. **Blocks Storage Deep Dive**: [How blocks storage works →](../blocks-storage/_index.md)
4. **High Availability**: [Configure zone replication →](../guides/zone-replication.md)
5. **Monitoring Cortex**: [Capacity planning guide →](../guides/capacity-planning.md)
6. **Secure Your Deployment**: [Set up authentication →](../guides/authentication-and-authorisation.md)

## Comparison: Single Binary vs Microservices

| Aspect                | Single Binary                  | Microservices                     |
|-----------------------|--------------------------------|-----------------------------------|
| **Components**        | All in one process             | Separate pods per component       |
| **Scaling**           | Vertical (bigger instance)     | Horizontal (more pods)            |
| **Resource Usage**    | Lower (1 process)              | Higher (multiple processes)       |
| **Complexity**        | Simple                         | Complex (orchestration needed)    |
| **Failure Isolation** | None (single point of failure) | Yes (component failures isolated) |
| **Use Case**          | Dev, testing, learning         | Production deployments            |

## Additional Resources

- [Cortex Documentation](https://cortexmetrics.io/docs/)
- [Cortex Helm Chart](https://github.com/cortexproject/cortex-helm-chart)
- [Cortex Architecture](../architecture.md)
- [Running on Production Kubernetes](../guides/running-cortex-on-kubernetes.md)
- [CNCF Slack #cortex](https://cloud-native.slack.com/archives/cortex)
