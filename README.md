# Cortex: A multitenant, horizontally scalable Prometheus as a service

[![Circle CI](https://circleci.com/gh/weaveworks/cortex/tree/master.svg?style=shield)](https://circleci.com/gh/weaveworks/cortex/tree/master)
[![Slack Status](https://slack.weave.works/badge.svg)](https://slack.weave.works)

*NB this is a pre-release, pre-alpha service. Availability will not be 100%.
APIs will change. Data will be lost.*

Cortex is an API compatible [Prometheus](https://prometheus.io)
implementation, that natively supports multitenancy and scale-out clustering.

Cortex is a [Weaveworks](https://weave.works) project that forms a major part
of [Weave Cloud](https://cloud.weave.works). If you are interested in using
our hosted Prometheus as a service, please
contact [help@weave.works](mailto:help@weave.works).

## Getting started

Go to https://cloud.weave.works and sign up. Follow the steps to create
a new instance.

Once you have created your instance, note down the 'Service Token' listed
underneath 'Probes' in the box on the right of the screen. You will use this
token to authenticate your local Prometheus with Weave Cloud.

You can [download Prometheus from its website](https://prometheus.io/download/).
You will need v1.2.1 or later.

When you've got Prometheus, you will need
to
[configure it to discover your services](https://prometheus.io/docs/operating/configuration/) and
also configure it to send its data to Weave Cloud by adding the following
top-level stanza to `prometheus.yml`:

    remote_write:
      url: https://cloud.weave.works/api/prom/push
      basic_auth:
        password: <token>

Where `<token>` is the Service Token you obtained from Weave Cloud.

Once your local Prometheus is running you can enter Prometheus queries into
Weave Cloud. Go to https://cloud.weave.works and click the graph icon to the
right of the instance selector:

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
- [PromCon 2016 talk video](https://www.youtube.com/watch?v=3Tb4Wc0kfCM)

Or join us on [our Slack channel](https://slack.weave.works) and ask us
directly!
