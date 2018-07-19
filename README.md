<p align="center"><img src="imgs/logo.png" alt="Weave Cortex Logo"></p>

# Open source, horizontally scalable Prometheus as a service

[![Circle CI](https://circleci.com/gh/weaveworks/cortex/tree/master.svg?style=shield)](https://circleci.com/gh/weaveworks/cortex/tree/master)
[![GoDoc](https://godoc.org/github.com/weaveworks/cortex?status.svg)](https://godoc.org/github.com/weaveworks/cortex)
[![Slack Status](https://slack.weave.works/badge.svg)](https://slack.weave.works)

Cortex is a [Weaveworks](https://weave.works) project that forms the monitoring backend of [Weave Cloud](https://cloud.weave.works).

It provides horizontally scalable, long term storage for [Prometheus](https://prometheus.io) metrics when used as a [remote write](https://prometheus.io/docs/operating/configuration/#remote_write) destination, and a horizontally scalable, Prometheus-compatible query API.

To use Cortex, sign up for [Weave Cloud](https://cloud.weave.works) and follow the instructions there.

Additional help can also be found in the [Weave Cloud documentation](https://www.weave.works/docs/cloud/latest/overview/):

* [Installing the Weave Cloud Agents](https://www.weave.works/docs/cloud/latest/install/installing-agents/#weave-cloud-supported)
* [Prometheus Monitoring in Weave Cloud](https://www.weave.works/docs/cloud/latest/concepts/prometheus-monitoring/)
* [Instrumenting Your App: Best Practises](https://www.weave.works/docs/cloud/latest/concepts/prometheus-monitoring/)

## Developing

To build (requires Docker):
```
make
```

To run the test suite:
```
make test
```

To checkout Cortex in minikube:
```
kubectl create -f ./k8s
```

Cortex will sit behind an nginx instance exposed on port 30080.  A job is deployed to scrape it itself.  Try it:

http://192.168.99.100:30080/api/prom/api/v1/query?query=up

### Vendoring

We use `dep` to vendor dependencies.  To fetch a new dependency, run:

    make update-vendor

To update dependencies, run

    dep ensure --update && make update-vendor

## Further reading

To learn more about Cortex, consult the following documents / talks:

- [Original design document for Project Frankenstein](http://goo.gl/prdUYV)
- PromCon 2016 Talk: "Project Frankenstein: Multitenant, Scale-Out Prometheus": [video](https://youtu.be/3Tb4Wc0kfCM), [slides](http://www.slideshare.net/weaveworks/project-frankenstein-a-multitenant-horizontally-scalable-prometheus-as-a-service)
- KubeCon Prometheus Day talk "Weave Cortex: Multi-tenant, horizontally scalable Prometheus as a Service" [slides](http://www.slideshare.net/weaveworks/weave-cortex-multitenant-horizontally-scalable-prometheus-as-a-service)
- CNCF TOC Presentation; "Horizontally Scalable, Multi-tenant Prometheus" [slides](https://docs.google.com/presentation/d/190oIFgujktVYxWZLhLYN4q8p9dtQYoe4sxHgn4deBSI/edit#slide=id.g3b8e2d6f7e_0_6)

## <a name="help"></a>Getting Help

If you have any questions regarding Cortex, or on using Prometheus with Weave Cloud:


- Ask a question on the <a href="https://weave-community.slack.com/messages/general/"> #weave-community</a> Slack channel. To invite yourself to the Weave Community Slack channel, visit https://weaveworks.github.io/community-slack/.
- Join the <a href="https://www.meetup.com/pro/Weave/"> Weave User Group </a> and get invited to online talks, hands-on training and meetups in your area.
- Send an email to <a href="mailto:weave-users@weave.works">weave-users@weave.works</a>
- <a href="https://github.com/weaveworks/cortex/issues/new">File an issue.</a>

Your feedback is always welcome.
