<p align="center">
<img src="oklog.png" width=500 height=142 alt="OK Log"/><br/>
<a href="https://github.com/oklog/oklog/releases/latest"><img src="https://img.shields.io/github/release/oklog/oklog.svg?style=flat-square"/></a>
<a href="https://travis-ci.org/oklog/oklog"><img src="https://img.shields.io/travis/oklog/oklog.svg?style=flat-square"/></a>
<a href="https://goreportcard.com/report/github.com/oklog/oklog"><img src="https://goreportcard.com/badge/github.com/oklog/oklog?style=flat-square&x=1"/></a>
<a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg?style=flat-square"/></a>
</p>

OK Log is a distributed and coördination-free log management system for big ol' clusters.
It's an on-prem solution that's designed to be a sort of building block: easy to understand, easy to operate, and easy to extend.

- [Article motivating the system](https://peter.bourgon.org/ok-log)
- [Detailed system design](DESIGN.md)

## Is OK Log for me?

You may consider OK Log if...

- You're tailing your logs manually, find it annoying, and want to aggregate them without a lot of fuss
- You're using a hosted solution like Loggly, and want to move logs on-prem
- You're using Elasticsearch, but find it unreliable, difficult to operate, or don't use many of its features
- You're using a custom log pipeline with e.g. Fluentd or Logstash, and having performance problems
- You just wanna, like, grep your logs — why is this all so complicated?

## Getting OK Log

OK Log is distributed as a single, statically-linked binary for a variety of target architectures.
Download the latest release from [the releases page](https://github.com/oklog/oklog/releases).

## Quickstart

```sh
$ oklog ingeststore -store.segment-replication-factor 1
$ ./myservice | oklog forward localhost
$ oklog query -from 5m -q Hello
2017-01-01 12:34:56 Hello world!
```

## Deploying

### Small installations

If you have relatively small log volume, you can deploy a cluster of identical ingeststore nodes.
By default, the replication factor is 2, so you need at least 2 nodes.
Use the -cluster flag to specify a routable IP address or hostname for each node to advertise itself on.
And let each node know about at least one other node with the -peer flag.

```sh
foo$ oklog ingeststore -cluster foo -peer foo -peer bar -peer baz
bar$ oklog ingeststore -cluster bar -peer foo -peer bar -peer baz
baz$ oklog ingeststore -cluster baz -peer foo -peer bar -peer baz
```

To grow the cluster, just add a new node, and tell it about at least one other node via the -peer flag.
Optionally, you can run the rebalance tool (TODO) to redistribute the data over the new topology.
To shrink the cluster, just kill nodes fewer than the replication factor,
 and run the repair tool (TODO) to re-replicate lost records.

All configuration is done via commandline flags.
You can change things like the log retention period (default 7d),
 the target segment file size (default 128MB),
 and maximum time (age) of various stages of the logging pipeline.
Most defaults should be sane, but you should always audit for your environment.

### Large installations

If you have relatively large log volume, you can split the ingest and store (query) responsibilities.
Ingest nodes make lots of sequential writes, and benefit from fast disks and moderate CPU.
Store nodes make lots of random reads and writes, and benefit from large disks and lots of memory.
Both ingest and store nodes join the same cluster, so provide them with the same set of peers.

```sh
ingest1$ oklog ingest -cluster 10.1.0.1 -peer ...
ingest2$ oklog ingest -cluster 10.1.0.2 -peer ...

store1$ oklog store -cluster 10.1.9.1 -peer ...
store2$ oklog store -cluster 10.1.9.2 -peer ...
store3$ oklog store -cluster 10.1.9.3 -peer ...
```

To add more raw ingest capacity, add more ingest nodes to the cluster.
To add more storage or query capacity, add more store nodes.
Also, make sure you have enough store nodes to consume from the ingest nodes without backing up.

## Forwarding

The forwarder is basically just netcat with some reconnect logic.
Pipe the stdout/stderr of your service to the forwarder, configured to talk to your ingesters.

```sh
$ ./myservice | oklog forward ingest1 ingest2
```

OK Log integrates in a straightforward way with runtimes like Docker and Kubernetes.
See [the Integrations page](https://github.com/oklog/oklog/wiki/Integrations) for more details.

## Querying

Querying is an HTTP GET to /query on any of the store nodes.
OK Log comes with a query tool to make it easier to play with.
One good thing is to first use the -stats flag to refine your query.
When you're satisfied it's sufficiently constrained, drop -stats to get results.

```sh
$ oklog query -from 2h -to 1h -q "myservice.*(WARN|ERROR)" -regex
2016-01-01 10:34:58 [myservice] request_id 187634 -- [WARN] Get /check: HTTP 419 (0B received)
2016-01-01 10:35:02 [myservice] request_id 288211 -- [ERROR] Post /ok: HTTP 500 (0B received)
2016-01-01 10:35:09 [myservice] request_id 291014 -- [WARN] Get /next: HTTP 401 (0B received)
 ...
```

To query structured logs, combine a basic grep filter expression with a tool like [jq](https://stedolan.github.io/jq/).

```sh
$ oklog query -from 1h -q /api/v1/login
2016-01-01 09:15:45 {"remote_addr":"10.34.115.3:50032","path":"/api/v1/login","method":"POST","status_code":200}
2016-01-01 09:15:47 {"remote_addr":"10.9.101.113:51442","path":"/api/v1/login","method":"POST","status_code":500}
2016-01-01 09:15:48 {"remote_addr":"10.9.55.2:55210","path":"/api/v1/login","method":"POST","status_code":200}
2016-01-01 09:15:49 {"remote_addr":"10.34.115.1:51610","path":"/api/v1/login","method":"POST","status_code":200}
...

$ oklog query -from 1h -q /api/v1/login | cut -d' ' -f3- | jq '. | select(.status_code == 500)'
{
	"remote_addr": "10.9.55.2:55210",
	"path": "/api/v1/login",
	"method": "POST",
	"status_code": 500
}
...
```

## UI

OK Log ships with a basic UI for making queries.
You can access it on any store or ingeststore node, on the public API port (default 7650), path `/ui`.
So, e.g. http://localhost:7650/ui.

## Further reading

### Integrations

- [Docker](https://github.com/oklog/oklog/wiki/Integrations#docker)
- [Kubernetes](https://github.com/oklog/oklog/wiki/Integrations#kubernetes)

---

[OK icon](https://thenounproject.com/term/like/594444/) by
 [Karthik Srinivas](https://thenounproject.com/aathis/) from the
 [Noun Project](https://thenounproject.com).
Development supported by [DigitalOcean](https://digitalocean.com).
