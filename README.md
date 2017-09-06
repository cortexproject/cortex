<p align="center"><img src="imgs/logo.png" alt="Weave Cortex Logo"></p>

# Open source, horizontally scalable Prometheus as a service

[![Circle CI](https://circleci.com/gh/weaveworks/cortex/tree/master.svg?style=shield)](https://circleci.com/gh/weaveworks/cortex/tree/master)
[![Slack Status](https://slack.weave.works/badge.svg)](https://slack.weave.works)

Cortex is a [Weaveworks](https://weave.works) project that forms the monitoring backend of [Weave Cloud](https://cloud.weave.works).

It provides horizontally scalable, long term storage for [Prometheus](https://prometheus.io) metrics when used as a [remote write](https://prometheus.io/docs/operating/configuration/#remote_write) destination, and a horizontally scalable, Prometheus-compatible query API.

To use Cortex, sign up for [Weave Cloud](https://cloud.weave.works) and follow the instructions there.

Additional help can also be found in the [Weave Cloud documentation](https://www.weave.works/docs/cloud/latest/overview/):

* [Installing the Weave Cloud Agents](https://www.weave.works/docs/cloud/latest/install/installing-agents/#weave-cloud-supported)
* [Prometheus Monitoring in Weave Cloud](https://www.weave.works/docs/cloud/latest/concepts/prometheus-monitoring/)
* [Instrumenting Your App: Best Practises](https://www.weave.works/docs/cloud/latest/concepts/prometheus-monitoring/)

## Developing

To build & test, install minikube, and run:

    eval $(minikube docker-env)
    make
    kubectl create -f ./k8s

Cortex will sit behind an nginx instance exposed on port 30080.  A job is deployed to scrape it itself.  Try it:

http://192.168.99.100:30080/api/prom/api/v1/query?query=up

### Experimental Bazel builds

We also have early support for Bazel builds.  Once you have [installed
bazel](https://bazel.build/versions/master/docs/install.html), try these commands:

    make bazel-test
    make bazel

This will require Bazel 0.5.3 or later.

Bazel can be useful for running fast, local, incremental builds and tests.
Currently [bazel does not support cross-compiling](https://github.com/bazelbuild/rules_go/issues/70)
so it is not used to produce the final binaries and docker container images.

### Vendoring

We use `dep` to vendor dependencies.  To fetch a new dependency, run:

    dep ensure && dep prune

To update dependencies, run

    dep ensure --update && dep prune

## Further reading

To learn more about Cortex, consult the following documents / talks:

- [Original design document for Project Frankenstein](http://goo.gl/prdUYV)
- PromCon 2016 Talk: "Project Frankenstein: Multitenant, Scale-Out Prometheus": [video](https://youtu.be/3Tb4Wc0kfCM), [slides](http://www.slideshare.net/weaveworks/project-frankenstein-a-multitenant-horizontally-scalable-prometheus-as-a-service)
- KubeCon Prometheus Day talk "Weave Cortex: Multi-tenant, horizontally scalable Prometheus as a Service" [slides](http://www.slideshare.net/weaveworks/weave-cortex-multitenant-horizontally-scalable-prometheus-as-a-service)

## <a name="help"></a>Getting Help

If you have any questions regarding Cortex, or on using Prometheus with Weave Cloud:


- Ask a question on the <a href="https://weave-community.slack.com/messages/general/"> #weave-community</a> Slack channel. To invite yourself to the Weave Community Slack channel, visit https://weaveworks.github.io/community-slack/.
- Join the <a href="https://www.meetup.com/pro/Weave/"> Weave User Group </a> and get invited to online talks, hands-on training and meetups in your area.
- Send an email to <a href="mailto:weave-users@weave.works">weave-users@weave.works</a>
- <a href="https://github.com/weaveworks/cortex/issues/new">File an issue.</a>

Your feedback is always welcome.
