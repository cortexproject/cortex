# Cortex On Kubernetes

The microservice [architecture](docs/architecture.md) of Cortex makes it a
great fit for deployment on Kubernetes. You can use the manifests in this
directory as a basis to get started running Cortex in your own Kubernetes
cluster.

## Using Kustomize

To try out Cortex quickly you can use
[Kustomize](https://github.com/kubernetes-sigs/kustomize) to apply these
manifests directly to create the resources in a "cortex" namespace.

```sh
kustomize build github.com/cortexproject/cortex/k8s | kubectl apply -f -
```

Alternatively, you can just use `kubectl` if you have version >= v1.14.

```sh
kubectl apply -k github.com/cortexproject/cortex/k8s
```

To layer modifications on top of these manifests such as image tags, namespace,
etc create your own `kustomization.yaml` and use
`github.com/cortexproject/cortex/k8s` as a base. Take a look at the official
[kustomization docs](https://github.com/kubernetes-sigs/kustomize#usage) for
more detailed usage information.
