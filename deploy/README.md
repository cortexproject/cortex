# Deploying Cortex

A set of templates to generate kubernetes manifests for deploying Cortex.

### Requirements

Most of the required software is installed during the build to minimize configuration of deployment environments.

Minimal Requirements
- linux-amd64
- cURL

### Usage

There are two entrypoints into the Jsonnet templates. `cortex.jsonnet` and `demo.jsonnet`

**cortex.jsonnet** - provides all cortex components

```bash
make cortex
kubectl apply -f deployments/cortex.yaml
```

**demo.jsonnet** - provides all cortex components and the basic configuration of prometheus & nginx for testing and demo purposes

The nginx deployment provides a proxy which applies the 'X-Org-ScopeID' header to all requests and proxies to distributor or query-frontend. The Prometheus deployment scrapes all cortex components and is hooked up to cortex via remote_storage through the nginx proxy.

```bash
make demo
kubectl apply -f deployments/demo.yaml
```

### Testing

There is a `test` build target which will run `kubecfg validate` on the generated manifests. It will use a demo installation to test. The validate function ensures that the generated templates are valid within the K8s API spec. You will need a valid kubeconfig and a working kubernetes cluster to perform this test.

```bash
make test
```
