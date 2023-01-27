# rueidis

[![Go Reference](https://pkg.go.dev/badge/github.com/rueian/rueidis.svg)](https://pkg.go.dev/github.com/rueian/rueidis)
[![Build status](https://badge.buildkite.com/d15fbd91b3b22b55c8d799564f84918a322118ae02590858c4.svg)](https://buildkite.com/rueian/rueidis)
[![Go Report Card](https://goreportcard.com/badge/github.com/rueian/rueidis)](https://goreportcard.com/report/github.com/rueian/rueidis)
[![codecov](https://codecov.io/gh/rueian/rueidis/branch/master/graph/badge.svg?token=wGTB8GdY06)](https://codecov.io/gh/rueian/rueidis)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/rueian/rueidis.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/rueian/rueidis/alerts/)
[![Maintainability](https://api.codeclimate.com/v1/badges/0d93d524c2b8497aacbe/maintainability)](https://codeclimate.com/github/rueian/rueidis/maintainability)

A Fast Golang Redis client that does auto pipelining and supports client side caching.

## Features

* Auto pipeline for non-blocking redis commands
* Connection pooling for blocking redis commands
* Opt-in client side caching in RESP3
* Pub/Sub, Redis 7 Sharded Pub/Sub
* Redis Cluster, Sentinel, Streams, TLS, RedisJSON, RedisBloom, RediSearch, RedisGraph, RedisTimeseries, RedisAI, RedisGears
* IDE friendly redis command builder
* Generic Hash/RedisJSON Object Mapping with client side caching and optimistic locking
* OpenTelemetry tracing and metrics
* Distributed Locks with client side caching
* Helpers for writing tests with rueidis mock

## Limitations

Rueidis is built around RESP2 and RESP3 protocol, and supports almost all redis features.
However, the following features has not yet been implemented in RESP2 mode:

* Client side caching only works in RESP3 and Redis >= 6.0

## Getting Started

```golang
package main

import (
	"context"
	"github.com/rueian/rueidis"
)

func main() {
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"127.0.0.1:6379"},
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	ctx := context.Background()

	// SET key val NX
	c.Do(ctx, c.B().Set().Key("key").Value("val").Nx().Build()).Error()
	// GET key
	c.Do(ctx, c.B().Get().Key("key").Build()).ToString()
}
```

## Auto Pipeline

All non-blocking commands sending to a single redis node are automatically pipelined through connections,
which reduces the overall round trips and system calls, and gets higher throughput.

### Benchmark comparison with go-redis v9

Rueidis has higher throughput than go-redis v9 across 1, 8, and 64 parallelism settings.

It is even able to achieve ~14x throughput over go-redis in a local benchmark of Macbook Pro 16" M1 Pro 2021. (see `parallelism(64)-key(16)-value(64)-10`)

#### Single Client
![client_test_set](https://github.com/rueian/rueidis-benchmark/blob/master/client_test_set_8.png)
#### Cluster Client
![cluster_test_set](https://github.com/rueian/rueidis-benchmark/blob/master/cluster_test_set_8.png)

Benchmark source code: https://github.com/rueian/rueidis-benchmark

There is also a benchmark result performed on two GCP n2-highcpu-2 machines shows that rueidis can achieve higher throughput with lower latencies: https://github.com/rueian/rueidis/pull/93

## Client Side Caching

The Opt-In mode of server-assisted client side caching is enabled by default, and can be used by calling `DoCache()` or `DoMultiCache()` with
an explicit client side TTL.

```golang
c.DoCache(ctx, c.B().Hmget().Key("myhash").Field("1", "2").Cache(), time.Minute).ToArray()
c.DoMultiCache(ctx,
    rueidis.CT(c.B().Get().Key("k1").Cache(), 1*time.Minute),
    rueidis.CT(c.B().Get().Key("k2").Cache(), 2*time.Minute))
```

An explicit client side TTL is required because redis server may not send invalidation message in time when
a key is expired on the server. Please follow [#6833](https://github.com/redis/redis/issues/6833) and [#6867](https://github.com/redis/redis/issues/6867)

Although an explicit client side TTL is required, the `DoCache()` and `DoMultiCache()` still sends a `PTTL` command to server and make sure that
the client side TTL is not longer than the TTL on server side.

Users can use `IsCacheHit()` to verify that if the response came from the client side memory:

```golang
c.DoCache(ctx, c.B().Get().Key("k1").Cache(), time.Minute).IsCacheHit() == true
```

And use `CacheTTL()` to check the remaining client side TTL in seconds:

```golang
c.DoCache(ctx, c.B().Get().Key("k1").Cache(), time.Minute).CacheTTL() == 60
```

If the OpenTelemetry is enabled by the `rueidisotel.WithClient(client)`, then there are also two metrics instrumented:
* rueidis_do_cache_miss
* rueidis_do_cache_hits

### Benchmark

![client_test_get](https://github.com/rueian/rueidis-benchmark/blob/master/client_test_get_8.png)

Benchmark source code: https://github.com/rueian/rueidis-benchmark

### Supported Commands by Client Side Caching

* bitcount
* bitfieldro
* bitpos
* expiretime
* geodist
* geohash
* geopos
* georadiusro
* georadiusbymemberro
* geosearch
* get
* mget
* getbit
* getrange
* hexists
* hget
* hgetall
* hkeys
* hlen
* hmget
* hstrlen
* hvals
* lindex
* llen
* lpos
* lrange
* pexpiretime
* pttl
* scard
* sismember
* smembers
* smismember
* sortro
* strlen
* ttl
* type
* zcard
* zcount
* zlexcount
* zmscore
* zrange
* zrangebylex
* zrangebyscore
* zrank
* zrevrange
* zrevrangebylex
* zrevrangebyscore
* zrevrank
* zscore
* jsonget
* jsonmget
* jsonstrlen
* jsonarrindex
* jsonarrlen
* jsonobjkeys
* jsonobjlen
* jsontype
* jsonresp
* bfexists
* bfinfo
* cfexists
* cfcount
* cfinfo
* cmsquery
* cmsinfo
* topkquery
* topklist
* topkinfo
* aitensorget
* aimodelget
* aimodelexecute
* aiscriptget

### MGET/JSON.MGET Client Side Caching Helpers

`rueidis.MGetCache` and `rueidis.JsonMGetCache` are handy helpers fetching multiple keys across different slots through the client side caching.
They will first group keys by slot to build `MGET` or `JSON.MGET` commands respectively and then send requests with only cache missed keys to redis nodes.

### Broadcast Mode Client Side Caching

Although the default is opt-in mode, you can use broadcast mode by specifying your prefixes in `ClientOption.ClientTrackingOptions`:

```go
c, err := rueidis.NewClient(rueidis.ClientOption{
	InitAddress:           []string{"127.0.0.1:6379"},
	ClientTrackingOptions: []string{"PREFIX", "prefix1:", "PREFIX", "prefix2:", "BCAST"},
})
if err != nil {
	panic(err)
}
c.DoCache(ctx, c.B().Get().Key("prefix1:1").Cache(), time.Minute).IsCacheHit() == false
c.DoCache(ctx, c.B().Get().Key("prefix1:1").Cache(), time.Minute).IsCacheHit() == true
```

Please make sure that commands passed to `DoCache()` and `DoMultiCache()` are covered by your prefixes.
Otherwise, their client-side cache will not be invalidated by redis.

### Disable Client Side Caching

Some Redis provider doesn't support client-side caching, ex. Google Cloud Memorystore.
You can disable client-side caching by setting `ClientOption.DisableCache` to `true`.
This will also fall back `Client.DoCache/Client.DoMultiCache` to `Client.Do/Client.DoMulti`.

## Blocking Commands

The following blocking commands use another connection pool and will not share the same connection
with non-blocking commands and thus will not cause the pipeline to be blocked:

* xread with block
* xreadgroup with block
* blpop
* brpop
* brpoplpush
* blmove
* blmpop
* bzpopmin
* bzpopmax
* bzmpop
* clientpause
* migrate
* wait

## Context Cancellation

`Client.Do()`, `Client.DoMulti()`, `Client.DoCache()` and `Client.DoMultiCache()` can return early if the context is canceled or the deadline is reached.

```golang
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()
c.Do(ctx, c.B().Set().Key("key").Value("val").Nx().Build()).Error() == context.DeadlineExceeded
```

Please note that though operations can return early, the command is likely sent already.

## Pub/Sub

To receive messages from channels, `Client.Receive()` should be used. It supports `SUBSCRIBE`, `PSUBSCRIBE` and Redis 7.0's `SSUBSCRIBE`:

```golang
err = c.Receive(context.Background(), c.B().Subscribe().Channel("ch1", "ch2").Build(), func(msg rueidis.PubSubMessage) {
    // handle the msg
})
```

The provided handler will be called with received message.

It is important to note that `Client.Receive()` will keep blocking and return only when the following cases:
1. return `nil` when received any unsubscribe/punsubscribe message related to the provided `subscribe` command.
2. return `rueidis.ErrClosing` when the client is closed manually.
3. return `ctx.Err()` when the `ctx` is done.
4. return non-nil `err` when the provided `subscribe` command failed.

While the `Client.Receive()` call is blocking, the `Client` is still able to accept other concurrent requests,
and they are sharing the same tcp connection. If your message handler may take some time to complete, it is recommended
to use the `Client.Receive()` inside a `Client.Dedicated()` for not blocking other concurrent requests.

### Alternative PubSub Hooks

The `Client.Receive()` requires users to provide a subscription command in advance.
There is an alternative `DedicatedClient.SetPubSubHooks()` allows users to subscribe/unsubscribe channels later.

```golang
client, cancel := c.Dedicate()
defer cancel()

wait := client.SetPubSubHooks(rueidis.PubSubHooks{
	OnMessage: func(m rueidis.PubSubMessage) {
		// Handle message. This callback will be called sequentially, but in another goroutine.
	}
})
client.Do(ctx, client.B().Subscribe().Channel("ch").Build())
err := <-wait // disconnected with err
```

If the hooks are not nil, the above `wait` channel is guaranteed to be close when the hooks will not be called anymore,
and produce at most one error describing the reason. Users can use this channel to detect disconnection.

## CAS Pattern

To do a CAS operation (WATCH + MULTI + EXEC), a dedicated connection should be used, because there should be no
unintentional write commands between WATCH and EXEC. Otherwise, the EXEC may not fail as expected.

The dedicated connection shares the same connection pool with blocking commands.

```golang
c.Dedicated(func(client client.DedicatedClient) error {
    // watch keys first
    client.Do(ctx, client.B().Watch().Key("k1", "k2").Build())
    // perform read here
    client.Do(ctx, client.B().Mget().Key("k1", "k2").Build())
    // perform write with MULTI EXEC
    client.DoMulti(
        ctx,
        client.B().Multi().Build(),
        client.B().Set().Key("k1").Value("1").Build(),
        client.B().Set().Key("k2").Value("2").Build(),
        client.B().Exec().Build(),
    )
    return nil
})

```

Or use Dedicate and invoke `cancel()` when finished to put the connection back to the pool.

``` golang
client, cancel := c.Dedicate()
defer cancel()

// watch keys first
client.Do(ctx, client.B().Watch().Key("k1", "k2").Build())
// perform read here
client.Do(ctx, client.B().Mget().Key("k1", "k2").Build())
// perform write with MULTI EXEC
client.DoMulti(
    ctx,
    client.B().Multi().Build(),
    client.B().Set().Key("k1").Value("1").Build(),
    client.B().Set().Key("k2").Value("2").Build(),
    client.B().Exec().Build(),
)
```

However, occupying a connection is not good in terms of throughput. It is better to use Lua script to perform
optimistic locking instead.

## Memory Consumption Consideration

Each underlying connection in rueidis allocates a ring buffer for pipelining.
Its size is controlled by the `ClientOption.RingScaleEachConn` and the default value is 10 which results into each ring of size 2^10.

If you have many rueidis connections, you may find that they occupy quite amount of memory.
In that case, you may consider reducing `ClientOption.RingScaleEachConn` to 8 or 9 at the cost of potential throughput degradation.

## Bulk Operations

The `rueidis.Commands` and `DoMulti()` can also be used for bulk operations:

``` golang
cmds := make(rueidis.Commands, 0, 10)
for i := 0; i < 10; i++ {
    cmds = append(cmds, c.B().Set().Key(strconv.Itoa(i)).Value(strconv.Itoa(i)).Build())
}
for _, resp := range c.DoMulti(ctx, cmds...) {
    if err := resp.Error(); err != nil {
        panic(err)
    }
}
```

## Lua Script

The `NewLuaScript` or `NewLuaScriptReadOnly` will create a script which is safe for concurrent usage.

When calling the `script.Exec`, it will try sending EVALSHA to the client and if the server returns NOSCRIPT,
it will send EVAL to try again.

```golang
script := rueidis.NewLuaScript("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}")
// the script.Exec is safe for concurrent call
list, err := script.Exec(ctx, client, []string{"k1", "k2"}, []string{"a1", "a2"}).ToArray()
```

## Redis Cluster, Single Redis and Sentinel

To connect to a redis cluster, the `NewClient` should be used:

```golang
c, err := rueidis.NewClient(rueidis.ClientOption{
    InitAddress: []string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"},
    ShuffleInit: true,
})
```

To connect to a single redis node, still use the `NewClient` with one InitAddress

```golang
c, err := rueidis.NewClient(rueidis.ClientOption{
    InitAddress: []string{"127.0.0.1:6379"},
})
```

To connect to sentinels, specify the required master set name:

```golang
c, err := rueidis.NewClient(rueidis.ClientOption{
    InitAddress: []string{"127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381"},
    Sentinel: rueidis.SentinelOption{
        MasterSet: "my_master",
    },
})
```

## Command Builder

Redis commands are very complex and their formats are very different from each other.

This library provides a type safe command builder within `client.B()` that can be used as
an entrypoint to construct a redis command. Once the command is completed, call the `Build()` or `Cache()` to get the actual command.
And then pass it to either `Client.Do()` or `Client.DoCache()`.

```golang
c.Do(ctx, c.B().Set().Key("mykey").Value("myval").Ex(10).Nx().Build())
```

**Once the command is passed to the `Client.Do()`, `Client.DoCache()`, the command will be recycled and SHOULD NOT be reused.**

**The `ClusterClient.B()` also checks if the command contains multiple keys belongs to different slots. If it does, then panic.**

### Arbitrary command

If you want to construct commands that are not yet supported, you can use `c.B().Arbitrary()`:

```golang
// This will result into [ANY CMD k1 k2 a1 a2]
c.B().Arbitrary("ANY", "CMD").Keys("k1", "k2").Args("a1", "a2").Build()
```

### Working with JSON string and `[]byte`

The command builder treats all the parameters as Redis strings, which are binary safe. This means that users can store `[]byte`
directly into Redis without conversion. And the `rueidis.BinaryString` helper can convert `[]byte` to `string` without copy. For example:

```golang
client.B().Set().Key("b").Value(rueidis.BinaryString([]byte{...})).Build()
```

Treating all the parameters as Redis strings also means that the command builder doesn't do any quoting, conversion automatically for users.

When working with RedisJSON, users frequently need to prepare JSON string in Redis string. And `rueidis.JSON` can help:

```golang
client.B().JsonSet().Key("j").Path("$.myStrField").Value(rueidis.JSON("str")).Build()
// equivalent to
client.B().JsonSet().Key("j").Path("$.myStrField").Value(`"str"`).Build()
```

## High level go-redis like API

Though it is easier to know what command will be sent to redis at first glance if the command is constructed by the command builder,
users may sometimes feel it too verbose to write.

For users who don't like the command builder, `rueidiscompat.Adapter`, contributed mainly by [@418Coffee](https://github.com/418Coffee), is an alternative.
It is a high level API which is close to go-redis's `Cmdable` interface.

### Migrating from go-redis

You can also try adapting `rueidis` with existing go-redis code by replacing go-redis's `UniversalClient` with `rueidiscompat.Adapter`.

### Client side caching

To use client side caching with `rueidiscompat.Adapter`, chain `Cache(ttl)` call in front of supported command.

```golang
package main

import (
	"context"
	"time"
	"github.com/rueian/rueidis"
	"github.com/rueian/rueidis/rueidiscompat"
)

func main() {
	ctx := context.Background()
	client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	compat := rueidiscompat.NewAdapter(client)
	ok, _ := compat.SetNX(ctx, "key", "val", time.Second).Result()

	// with client side caching
	res, _ := compat.Cache(time.Second).Get(ctx, "key").Result()
}
```

## Generic Object Mapping

The `NewHashRepository` and `NewJSONRepository` creates an OM repository backed by redis hash or RedisJSON.

```golang
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/rueian/rueidis"
    "github.com/rueian/rueidis/om"
)

type Example struct {
    Key string `json:"key" redis:",key"` // the redis:",key" is required to indicate which field is the ULID key
    Ver int64  `json:"ver" redis:",ver"` // the redis:",ver" is required to do optimistic locking to prevent lost update
    Str string `json:"myStr"`            // both NewHashRepository and NewJSONRepository use json tag as field name
}

func main() {
    ctx := context.Background()
    c, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
    if err != nil {
        panic(err)
    }
    // create the repo with NewHashRepository or NewJSONRepository
    repo := om.NewHashRepository("my_prefix", Example{}, c)

    exp := repo.NewEntity()
    exp.Str = "mystr"
    fmt.Println(exp.Key) // output 01FNH4FCXV9JTB9WTVFAAKGSYB
    repo.Save(ctx, exp) // success

    // lookup "my_prefix:01FNH4FCXV9JTB9WTVFAAKGSYB" through client side caching
    exp2, _ := repo.FetchCache(ctx, exp.Key, time.Second*5)
    fmt.Println(exp2.Str) // output "mystr", which equals to exp.Str

    exp2.Ver = 0         // if someone changes the version during your GET then SET operation,
    repo.Save(ctx, exp2) // the save will fail with ErrVersionMismatch.
}

```

### Object Mapping + RediSearch

If you have RediSearch, you can create and search the repository against the index.

```golang

if _, ok := repo.(*om.HashRepository[Example]); ok {
    repo.CreateIndex(ctx, func(schema om.FtCreateSchema) om.Completed {
        return schema.FieldName("myStr").Text().Build() // Note that the Example.Str field is mapped to myStr on redis by its json tag
    })
}

if _, ok := repo.(*om.JSONRepository[Example]); ok {
    repo.CreateIndex(ctx, func(schema om.FtCreateSchema) om.Completed {
        return schema.FieldName("$.myStr").Text().Build() // the field name of json index should be a json path syntax
    })
}

exp := repo.NewEntity()
exp.Str = "foo"
repo.Save(ctx, exp)

n, records, _ := repo.Search(ctx, func(search om.FtSearchIndex) om.Completed {
    return search.Query("foo").Build() // you have full query capability by building the command from om.FtSearchIndex
})

fmt.Println("total", n) // n is total number of results matched in redis, which is >= len(records)

for _, v := range records {
    fmt.Println(v.Str) // print "foo"
}
```

### Object Mapping Limitation

`NewHashRepository` only accepts these field types:
* `string`, `*string`
* `int64`, `*int64`
* `bool`, `*bool`
* `[]byte`

Field projection by RediSearch is not supported.

## OpenTelemetry Tracing

Use `rueidisotel.WithClient` to create a client with OpenTelemetry Tracing enabled.

```golang
package main

import (
    "github.com/rueian/rueidis"
    "github.com/rueian/rueidis/rueidisotel"
)

func main() {
    client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
    if err != nil {
        panic(err)
    }
    client = rueidisotel.WithClient(client)
    defer client.Close()
}
```

## Hooks and Other Observability Integration

In addition to `rueidisotel`, `rueidishook` provides a general hook mechanism for users to intercept `rueidis.Client` interface.

See [rueidishook](./rueidishook) for more details.

## Distributed Locks with client side caching

See [rueidislock](./rueidislock) for more details.

## Writing tests by mocking rueidis

See [mock](./mock) for more details.

## Command Response Cheatsheet

It is hard to remember what message type is returned from redis and which parsing method should be used with. So, here is some common examples:

```golang
// GET
client.Do(ctx, client.B().Get().Key("k").Build()).ToString()
client.Do(ctx, client.B().Get().Key("k").Build()).AsInt64()
// MGET
client.Do(ctx, client.B().Mget().Key("k1", "k2").Build()).ToArray()
// SET
client.Do(ctx, client.B().Set().Key("k").Value("v").Build()).Error()
// INCR
client.Do(ctx, client.B().Incr().Key("k").Build()).AsInt64()
// HGET
client.Do(ctx, client.B().Hget().Key("k").Field("f").Build()).ToString()
// HMGET
client.Do(ctx, client.B().Hmget().Key("h").Field("a", "b").Build()).ToArray()
// HGETALL
client.Do(ctx, client.B().Hgetall().Key("h").Build()).AsStrMap()
// ZRANGE
client.Do(ctx, client.B().Zrange().Key("k").Min("1").Max("2").Build()).AsStrSlice()
// ZRANK
client.Do(ctx, client.B().Zrank().Key("k").Member("m").Build()).AsInt64()
// ZSCORE
client.Do(ctx, client.B().Zscore().Key("k").Member("m").Build()).AsFloat64()
// ZRANGE
client.Do(ctx, client.B().Zrange().Key("k").Min("0").Max("-1").Build()).AsStrSlice()
client.Do(ctx, client.B().Zrange().Key("k").Min("0").Max("-1").Withscores().Build()).AsZScores()
// ZPOPMIN
client.Do(ctx, client.B().Zpopmin().Key("k").Build()).AsZScore()
client.Do(ctx, client.B().Zpopmin().Key("myzset").Count(2).Build()).AsZScores()
// SCARD
client.Do(ctx, client.B().Scard().Key("k").Build()).AsInt64()
// SMEMBERS
client.Do(ctx, client.B().Smembers().Key("k").Build()).AsStrSlice()
// LINDEX
client.Do(ctx, client.B().Lindex().Key("k").Index(0).Build()).ToString()
// LPOP
client.Do(ctx, client.B().Lpop().Key("k").Build()).ToString()
client.Do(ctx, client.B().Lpop().Key("k").Count(2).Build()).AsStrSlice()
```
