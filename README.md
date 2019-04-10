<p align="center"><img src="imgs/logo.png" alt="Cortex Logo"></p>

# Open source, horizontally scalable, multi-tenant Prometheus as a service

[![Circle CI](https://circleci.com/gh/cortexproject/cortex/tree/master.svg?style=shield)](https://circleci.com/gh/cortexproject/cortex/tree/master)
[![GoDoc](https://godoc.org/github.com/cortexproject/cortex?status.svg)](https://godoc.org/github.com/cortexproject/cortex)

Cortex is a [CNCF](https://cncf.io) sandbox project used in several production
systems including [Weave Cloud](https://cloud.weave.works), [GrafanaCloud](https://grafana.com/cloud)
and [FreshTracks.io](https://www.freshtracks.io/).

Cortex provides horizontally scalable, multi-tenant, long term storage for
[Prometheus](https://prometheus.io) metrics when used as a [remote
write](https://prometheus.io/docs/operating/configuration/#remote_write)
destination, and a horizontally scalable, Prometheus-compatible query
API.

Multi-tenant means it handles metrics from multiple independent
Prometheus sources, keeping them separate.

[Instrumenting Your App: Best Practices](https://www.weave.works/docs/cloud/latest/tasks/monitor/best-instrumenting/)

## Hosted Cortex (Prometheus as a service)

There are several commercial services where you can use Cortex
on-demand:

### Weave Cloud

[Weave Cloud](https://cloud.weave.works) from
[Weaveworks](https://weave.works) lets you deploy, manage, and monitor
container-based applications. Sign up at https://cloud.weave.works
and follow the instructions there. Additional help can also be found
in the [Weave Cloud documentation](https://www.weave.works/docs/cloud/latest/overview/).

### GrafanaCloud

To use Cortex as part of Grafana Cloud, sign up for [GrafanaCloud](https://grafana.com/cloud)
by clicking "Log In" in the top right and then "Sign Up Now".  Cortex is included
as part of the Starter and Basic Hosted Grafana plans.

## Contributing

For a guide to contributing to Cortex, see the [contributor guidelines](CONTRIBUTING.md).

## For Developers

To build:
```
make
```

(By default, the build runs in a Docker container, using an image built
with all the tools required. The source code is mounted from where you
run `make` into the build container as a Docker volume.)

To run the test suite:
```
make test
```

## Playing in `minikube`

First, start `minikube`.

You may need to load the Docker images into your minikube environment. There is
a convenience rule in the Makefile to do this:

```
make prime-minikube
```

Then run Cortex in minikube:
```
kubectl apply -f ./k8s
```

(these manifests use `latest` tags, i.e. this will work if you have
just built the images and they are available on the node(s) in your
Kubernetes cluster)

Cortex will sit behind an nginx instance exposed on port 30080.  A job is deployed to scrape itself.  Try it:

http://192.168.99.100:30080/api/prom/api/v1/query?query=up

If that doesn't work, your Minikube might be using a different ip address. Check with `minikube status`.

### Dependency management

We uses [Go modules](https://golang.org/cmd/go/#hdr-Modules__module_versions__and_more) to manage dependencies on external packages. This requires a working Go e
nvironment with version 1.11 or greater, git and [bzr](http://wiki.bazaar.canonical.com/Download) installed.

To add or update a new dependency, use the `go get` command:

```bash
# Pick the latest tagged release.
go get example.com/some/module/pkg

# Pick a specific version.
go get example.com/some/module/pkg@vX.Y.Z
```

Tidy up the `go.mod` and `go.sum` files:

```bash
go mod tidy
git add go.mod go.sum
git commit
```

You have to commit the changes to `go.mod` and `go.sum` before submitting the pull request.

## Further reading

To learn more about Cortex, consult the following documents/talks:

- [Original design document for Project Frankenstein](http://goo.gl/prdUYV)
- PromCon 2016 Talk: "Project Frankenstein: Multitenant, Scale-Out Prometheus": [video](https://youtu.be/3Tb4Wc0kfCM), [slides](http://www.slideshare.net/weaveworks/project-frankenstein-a-multitenant-horizontally-scalable-prometheus-as-a-service)
- KubeCon Prometheus Day talk "Weave Cortex: Multi-tenant, horizontally scalable Prometheus as a Service" [slides](http://www.slideshare.net/weaveworks/weave-cortex-multitenant-horizontally-scalable-prometheus-as-a-service)
- CNCF TOC Presentation; "Horizontally Scalable, Multi-tenant Prometheus" [slides](https://docs.google.com/presentation/d/190oIFgujktVYxWZLhLYN4q8p9dtQYoe4sxHgn4deBSI/edit#slide=id.g3b8e2d6f7e_0_6)

## <a name="help"></a>Getting Help

If you have any questions about Cortex:

- Ask a question on the [Cortex Slack channel](https://cloud-native.slack.com/messages/cortex/). To invite yourself to the CNCF Slack, visit http://slack.cncf.io/.
- <a href="https://github.com/cortexproject/cortex/issues/new">File an issue.</a>
- Send an email to <a href="mailto:cortex-users@lists.cncf.io">cortex-users@lists.cncf.io</a>

Your feedback is always welcome.
