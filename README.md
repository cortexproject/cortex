<p align="center"><img src="imgs/logo.png" alt="Cortex Logo"></p>

[![Circle CI](https://circleci.com/gh/cortexproject/cortex/tree/master.svg?style=shield)](https://circleci.com/gh/cortexproject/cortex/tree/master)
[![GoDoc](https://godoc.org/github.com/cortexproject/cortex?status.svg)](https://godoc.org/github.com/cortexproject/cortex)
<a href="https://goreportcard.com/report/github.com/cortexproject/cortex"><img src="https://goreportcard.com/badge/github.com/cortexproject/cortex" alt="Go Report Card" /></a>
<a href="https://cloud-native.slack.com/messages/cortex/"><img src="https://img.shields.io/badge/join%20slack-%23cortex-brightgreen.svg" alt="Slack" /></a>

# Cortex: horizontally scalable, highly available, multi-tenant, long term storage for  Prometheus.

Cortex provides horizontally scalable, highly available, multi-tenant, long term storage for
[Prometheus](https://prometheus.io).

- **Horizontally scalable:** Cortex can run across multiple machines in a cluster, exceeding the throughput and storage of a single machine.  This enables you to send the metrics from multiple Prometheus servers to a single Cortex cluster and run "globally aggregated" queries across all data in a single place.
- **Highly available:** When run in a cluster, Cortex can replicate data between machines.  This allows you to survive machine failure without gaps in your graphs.
- **Multi-tenant:** Cortex can isolate data and queries from multiple different independent
Prometheus sources in a single cluster, allowing untrusted parties to share the same cluster.
- **Long term storage:** Cortex supports Amazon DynamoDB, Google Bigtable, Cassandra, S3 and GCS for long term storage of metric data.  This allows you to durably store data for longer than the lifetime of any single machine, and use this data for long term capacity planning.

Cortex is a [CNCF](https://cncf.io) sandbox project used in several production systems including [Weave Cloud](https://cloud.weave.works) and [GrafanaCloud](https://grafana.com/cloud).
Cortex is a primarily used as a [remote write](https://prometheus.io/docs/operating/configuration/#remote_write) destination for Prometheus, with a Prometheus-compatible query API.

## Documentaion

Read the [getting started guide](docs/getting_started.md) if you're new to the project.

For a guide to contributing to Cortex, see the [contributor guidelines](CONTRIBUTING.md).

You can also read [an overview of Cortex's architecture](docs/architecture.md) and [information about configuring Cortex](docs/arguments.md).

## Further reading

To learn more about Cortex, consult the following documents & talks:

- Dev 2018 KubeCon talk; "[Cortex: Infinitely Scalable Prometheus][kubecon-2018-talk]" [video][kubecon-2018-video], ([slide][kubecon-2018-slides])
- Dec 2018 CNCF blog post; "[Cortex: a multi-tenant, horizontally scalable Prometheus-as-a-Service][cncf-2018-blog]"
- Nov 2018 CloudNative London meetup talk; "Cortex: Horizontally Scalable, Highly Available Prometheus" ([slides][cloudnative-london-2018-slides])
- Nov 2018 CNCF TOC Presentation; "Horizontally Scalable, Multi-tenant Prometheus" ([slides][cncf-toc-presentation])
- Aug 2017 PromCon talk; "[Cortex: Prometheus as a Service, One Year On][promcon-2017-talk]" ([videos][promcon-2017-video], [slides][promcon-2017-slides])
- July 2017 design doc; "[Cortex Query Optimisations][cortex-query-optimisation-2017]"
- June 2017 Prometheus London meetup talk; "Cortex: open-source, horizontally-scalable, distributed Prometheus" ([video][prometheus-london-2017-video])
- Dec 2016 KubeCon talk; "Weave Cortex: Multi-tenant, horizontally scalable Prometheus as a Service" ([video][kubecon-2016-video], [slides][kubecon-2016-slides])
- Aug 2016 PromCon talk; "Project Frankenstein: Multitenant, Scale-Out Prometheus": ([video][promcon-2016-video], [slides][promcon-2016-slides])
- June 2018 design document; "[Project Frankenstein: A Multi Tenant, Scale Out Prometheus](http://goo.gl/prdUYV)"

[kubecon-2018-talk]: https://kccna18.sched.com/event/GrXL/cortex-infinitely-scalable-prometheus-bryan-boreham-weaveworks
[kubecon-2018-video]: https://www.youtube.com/watch?v=iyN40FsRQEo
[kubecon-2018-slides]: https://static.sched.com/hosted_files/kccna18/9b/Cortex%20CloudNativeCon%202018.pdf
[cloudnative-london-2018-slides]: https://www.slideshare.net/grafana/cortex-horizontally-scalable-highly-available-prometheus
[cncf-2018-blog]: https://www.cncf.io/blog/2018/12/18/cortex-a-multi-tenant-horizontally-scalable-prometheus-as-a-service/
[cncf-toc-presentation]: https://docs.google.com/presentation/d/190oIFgujktVYxWZLhLYN4q8p9dtQYoe4sxHgn4deBSI/edit#slide=id.g3b8e2d6f7e_0_6
[prometheus-london-2017-video]: https://www.youtube.com/watch?v=Xi4jq2IUbLs
[promcon-2017-talk]: https://promcon.io/2017-munich/talks/cortex-prometheus-as-a-service-one-year-on/
[promcon-2017-video]: https://www.youtube.com/watch?v=_8DmPW4iQBQ
[promcon-2017-slides]: https://promcon.io/2017-munich/slides/cortex-prometheus-as-a-service-one-year-on.pdf
[cortex-query-optimisation-2017]: https://docs.google.com/document/d/1lsvSkv0tiAMPQv-V8vI2LZ8f4i9JuTRsuPI_i-XcAqY
[kubecon-2016-video]: https://www.youtube.com/watch?v=9Uctgnazfwk
[kubecon-2016-slides]: http://www.slideshare.net/weaveworks/weave-cortex-multitenant-horizontally-scalable-prometheus-as-a-service
[promcon-2016-video]: https://youtu.be/3Tb4Wc0kfCM
[promcon-2016-slides]: http://www.slideshare.net/weaveworks/project-frankenstein-a-multitenant-horizontally-scalable-prometheus-as-a-service

## <a name="help"></a>Getting Help

If you have any questions about Cortex:

- Ask a question on the [Cortex Slack channel](https://cloud-native.slack.com/messages/cortex/). To invite yourself to the CNCF Slack, visit http://slack.cncf.io/.
- <a href="https://github.com/cortexproject/cortex/issues/new">File an issue.</a>
- Send an email to <a href="mailto:cortex-users@lists.cncf.io">cortex-users@lists.cncf.io</a>

Your feedback is always welcome.

## Hosted Cortex (Prometheus as a service)

There are several commercial services where you can use Cortex
on-demand:

### Weave Cloud

[Weave Cloud](https://cloud.weave.works) from
[Weaveworks](https://weave.works) lets you deploy, manage, and monitor
container-based applications. Sign up at https://cloud.weave.works
and follow the instructions there. Additional help can also be found
in the [Weave Cloud documentation](https://www.weave.works/docs/cloud/latest/overview/).

[Instrumenting Your App: Best Practices](https://www.weave.works/docs/cloud/latest/tasks/monitor/best-instrumenting/)

### GrafanaCloud

To use Cortex as part of Grafana Cloud, sign up for [GrafanaCloud](https://grafana.com/cloud)
by clicking "Log In" in the top right and then "Sign Up Now".  Cortex is included
as part of the Starter and Basic Hosted Grafana plans.