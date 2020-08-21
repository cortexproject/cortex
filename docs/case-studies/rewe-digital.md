---
title: "How Cortex helped REWE digital ensure stability while scaling services during the Covid-19 pandemic"
linkTitle: "REWE digital"
weight: 2
slug: rewe-digital
---

[REWE digital](https://www.rewe-digital.com/) builds the technology that drives the e-commerce, app, and food pickup and delivery services for one of Germany’s largest grocery chains, REWE. Like other companies involved in the food supply chain, REWE has seen demand spike during the Covid-19 pandemic. Thanks to its adoption of Cortex last year, the monitoring team has been able to ensure stability at growing scale.

The REWE digital subsidiary was started in 2014 to advance its parent company’s digital transformation, so “we have a rather modern tech stack,” says Cloud Platform Engineer Martin Schneppenheim. A lot of the platform is run on Kubernetes using GKE while some is still running on-prem using Nomad. Still, there were some challenges that arose from the Spotify tribe model that the organization adopted. Each of the four tribes at REWE digital -- ECOM, FULFILLMENT, CONTENT and PLATFORM, and while the tribes mostly converged on the same technologies, he says, “each platform team has its own solution for, say, Kafka, for Prometheus, for Grafana.”

Plus, over the past six years, the company has grown from 30 employees to around 600.

With this rapid growth, they realized by the end of 2018 that they needed a new solution. The tipping point came when they experienced some out-of-memory issues with Prometheus, he says, “and we had no idea why.”

Each tribe had one Prometheus HA pair that was used by all the teams in the tribe. One of the tribes used one Prometheus pair which required 30-60 gigabytes of RAM per instance. “We still saw some random out-of-memory kills, and we believe it was because some queries were loading too many samples,” Schneppenheim says. “We had several platform teams doing basically the same thing, and we wanted to tackle such issues organization-wide.”

![Grafana powered by Cortex](/images/case-studies/rewe-workplace.jpg)

### Searching for a scalable monitoring solution

The solution needed to support the Prometheus format, since all of REWE’s microservices had a Prometheus end point. And the team wanted to have trust in the project’s longevity. After considering M3, Victoria Metrics, and Thanos, the REWE digital team decided to go with Cortex. “Cortex had just been released as a CNCF project, and there were several developers from different companies,” he says. “That was another plus point for us.”
The key selling point, he adds, was Cortex’s multi-tenant support, which also involves the different protection mechanisms built into Cortex to limit a tenant’s usage so that a single tenant doesn’t affect the performance for other tenants. “Every platform team was providing one Prometheus for their tribe,” Schneppenheim says, “and we wanted to move to something like a software-as-a-service approach, with just one team that provides Cortex, which can be used by all the teams within the company.”

### Implementation

Implementation began with the Big Data tribe, which has since merged with the other tribes and has been the smallest in the company. “We already had Prometheus set up, and we just switched the data source from Prometheus to Cortex,” Schneppenheim says. “In the beginning it was just one dashboard where we switched the data sources, and later on we switched the data source for the whole tribe so that all dashboards used the Cortex data source by default, and the Prometheus deployment basically acted as remote writing Prometheus. We always had the chance to just switch back to the Prometheus, in case there were any failures, so there was not a big risk.”

In fact, things went smoothly, and a few months later, the ECOM tribe started writing metrics to Cortex. At the same time, the platform tribe decided to create one Grafana instance and use organizations to offer multi-tenancy. After that second migration, the tribe’s teams were able to migrate dashboards to the new Grafana instance, and then start querying against that data. By the end of the year, all the tribes will have migrated to Cortex and the Grafana instance.

REWE digital adopted Cortex at “a very early stage,” Schneppenheim says. At first, “sometimes we had to read the code, because there was little documentation, but we were still confident that we took the right decision, because we got lots of support [from the community] in debugging some problems, which were usually misconfigurations.”

He points out that configuration has become simpler over the past year, with default values set in v1.0, and more documentation: “Things definitely became better.”

### Results

Cortex’s horizontal scaling has proven to be crucial during the Covid-19 pandemic, when REWE’s grocery and food delivery services have seen extremely high demand. “Our primary focus was to ensure stability, so we had to scale, and we deployed more containers,” he says. “That meant we had way more metrics than before, on the one hand, and on the other hand, I believe our developers were watching our dashboards more closely, so we had way more queries as well.”

Schneppenheim estimates that over the past two months, reads and writes have increased significantly, and the platform was able to handle the added load. Plus, “it was quite easy deploying another set of queriers,” he says.

Aside from that, Schneppenheim says, “the biggest advantage for our company is that we now have a team that can offer one thing as an internal service.” While the tribes’ ops teams still have to manage their own Prometheus servers, they have a much more stable and scalable system. The challenges are unpredictable resource usage on querying and some queries that can load too much data causing Prometheus to OOM, but with Cortex handling all the queries, this is no longer a problem. And while Schneppenheim’s team is still just two people ([they’re hiring!](https://www.rewe-digital.com/en/job-overview.html#categories=34)), he adds, “we can spend more time actually learning how to run it, and become an expert within the company for Cortex and the things that come along with it, like high cardinality metric series, which we see every two weeks or so. We are the contact for all the monitoring now.”

There have been other technical advantages, too: “We have no gaps anymore in our Prometheus and Grafana,” he says. “In case a Prometheus instance fails or if it needs to be restarted, we automatically switch over to the replica with the HA tracker, which is a great thing.”

![Cortex Reads](/images/case-studies/rewe-cortex-reads.png)

With Cortex’s query results cache, the queries are cached. The REWE digital team has found that this feature makes dashboards “super fast, because the query is likely already cached and it just has to load the new 30 seconds or so, since the last refresh,” says Schneppenheim. “Preloaded dashboards load or refresh really, really fast.”

Plus, there is a higher retention with Cortex. “We now have 60 days’ retention; we used to have seven days only,” he says.

The benefits are also clear as the infrastructure grows. REWE digital has added a few more small Kubernetes clusters, which “obviously have the same monitoring/alerting needs as our biggest clusters,” he says. Previously, the team would have to deploy Prometheus and a separate Grafana instance (along with NGINX and DNS setup).

“With our new SaaS approach, making monitoring available for these is as easy as adding a Prometheus pair, which sends metrics to our Cortex cluster, and adding this new tenant in our Grafana organization,” he says.

With Cortex, they’ve also been able to solve two use cases (Kubernetes clusters that had been split for technical reasons, and cloud migration) that required metrics from two different clusters to be available with the same tenant. “The dev teams had the need to aggregate metrics across these two clusters, which was easily possible, because we just ingested them under the same tenant ID,” says Schneppenheim.

And those out-of-memory issues? “We are constantly growing, not only on the query side, but also on the ingesting metrics side as we onboard teams and tribes,” he says. “But we have fine-tuned it quite well, and there are not as many OOM kills, and if there are, we don't see them in Grafana. That's important to us, that our developers have a smooth experience.” (Most tribes use Grafana alerting; one uses Prometheus Alertmanager.)

### Looking ahead

REWE digital’s main focus right now is to onboard the rest of the teams to Cortex. But looking ahead, the team is exploring [Grafana Cloud Agent](https://grafana.com/blog/2020/03/18/introducing-grafana-cloud-agent-a-remote_write-focused-prometheus-agent-that-can-save-40-on-memory-usage/) for the tribes that aren’t using Prometheus Alertmanager. “They don't need Prometheus; we only use Prometheus to scrape the targets and send samples to our Cortex,” he says, “so that could definitely be interesting, especially given the performance improvements. Its sole purpose is to send metrics to Cortex as our remote-write backend, so maybe there will be other advantages in the future, like a more close monitoring.”

The in-development Cortex Blocks storage engine is also interesting to the team as a solution for the bottleneck it has around a small BigTable cluster. “We just run three BigTable nodes, and the BigTable read latency sometimes peaks at one second, which is also the upper bucket limit in the histogram,” he says. “This happens if users open Grafana dashboards querying a long time range with many panels. Our hope is that switching from BigTable to the new storage engine would fix this as the object store (GCS) scales on-demand.”

The REWE digital team has built and open sourced its own [Cortex gateway](https://github.com/rewe-digital/cortex-gateway), which is on the project roadmap. “This might be a chance for us to contribute,” says Schneppenheim.

Schneppenheim is also hopeful that the positive results REWE digital has seen with Cortex may lead to its further adoption throughout the greater REWE Group organization. “We're just a small company within the REWE group,” he says, but “we might offer it as internal software as a service for other parts of the Group. They can trust our solution.”
