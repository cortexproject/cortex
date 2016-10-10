# Prism: A multitenant, horizontally scalable Prometheus as a service

[![Circle CI](https://circleci.com/gh/weaveworks/prism/tree/master.svg?style=shield)](https://circleci.com/gh/weaveworks/prism/tree/master)
[![Slack Status](https://slack.weave.works/badge.svg)](https://slack.weave.works)

*NB this is a pre-release, pre-alpha service. Availability will not be 100%.
APIs will change. Data will be lost.*

Prism is an API compatible [Prometheus](https://prometheus.io)
implementation, that natively supports multitenancy and scale-out clustering.

Prism is a [Weaveworks](https://weave.works) project that forms a major part
of [Weave Cloud](https://cloud.weave.works). If you are interested in using
our hosted Prometheus as a service, please
contact [help@weave.works](mailto:help@weave.works).

## Getting started

Go to https://cloud.weave.works and sign up. Follow the steps to create
a new instance.

Once you have created your instance, note down the 'Service Token' listed
underneath 'Probes' in the box on the right of the screen. You will use this
token to authenticate your retrieval agent with Weave Cloud.

The retrieval agent is a normal Prometheus instance, which you
can [download from the Prometheus website](https://prometheus.io/download/).
You will need v1.2.0 or later.

Once you've got the retrieval agent, you will need to provide a standard
[Prometheus config file](https://prometheus.io/docs/operating/configuration/)
so it can discover your services and relabel them appropriately, and then run
the following:

    prometheus -config.file=... -storage.remote.generic-url=http://user:<token>@cloud.weave.works/api/prom/push

Where `<token>` is the Service Token you obtained from Weave Cloud.

Once you have started your retrieval agent you can enter Prometheus queries
into Weave Cloud. Go to https://cloud.weave.works and click the graph icon to
the right of the instance selector:

![Cropped screenshot of Weave Cloud showing Prometheus button as graph](weave-cloud-snippet.png?raw=true)

To use the Prometheus Service with Grafana, configure your Grafana instance to
have a datasource with the following parameters:

- Type: Prometheus
- Url: https://cloud.weave.works/api/prom
- Access: Direct
- Http Auth: Basic Auth
- User: `user` (this is ignored, but must be non-empty)
- Password: <Service Token> (the same one your retrieval agent uses)

For more information, please see:
- [Original design document](http://goo.gl/prdUYV)
- [PromCon 2016 talk video](https://www.youtube.com/watch?v=3Tb4Wc0kfCM)

Or join us on [our Slack channel](https://slack.weave.works) and ask us
directly!
