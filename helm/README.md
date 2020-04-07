# Cortex Helm Chart

## Prerequisites

Make sure you have Helm 3 [installed](https://helm.sh/docs/intro/install/). Then execute the commands given below
in the Cortex Helm chart catalogue.

To customize behavior of the deployed Cortex stack, modify the chart's `config` value to match your needs - it is 
directly mapped to the Cortex's [configuration file](https://cortexmetrics.io/docs/configuration/configuration-file/).

## Deploy Cortex to your cluster

### Deploy with default config

```bash
$ helm upgrade --install cortex .
```

### Deploy in a custom namespace

```bash
$ helm upgrade --install cortex --namespace=cortex .
```

### Deploy with custom config (as command-line arguments)

```bash
$ helm upgrade --install cortex . --set "key1=val1,key2=val2,..."
```

### Deploy with custom config (from file)

```bash
$ helm upgrade --install cortex . --values cortex-values.yaml
```

## How to contribute

After adding your new feature to the appropriate chart, you can build and deploy it locally 
by executing the following command in the chart's directory:

```bash
$ helm upgrade --install cortex .
```

After verifying your changes, you need to bump the chart version following [semantic versioning](https://semver.org) rules.
For example, if you update the cortex chart, you need to bump the versions as follows:

- Update _version_ field in Chart.yaml