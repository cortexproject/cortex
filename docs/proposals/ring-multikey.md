---
title: "Ring Multikey"
linkTitle: "Ring Multikey"
weight: 1
slug: ring-multikey
---

- Author: [Daniel Blando](https://github.com/danielblando)
- Date: August 2022
- Status: Proposed

## Background

Cortex implements a ring structure to share information of registered pods for each
service. The data stored and used by the ring need to be implemented via Codec interface. Currently, the only supported
Codec to the ring is [Desc](https://github.com/cortexproject/cortex/blob/c815b3cb61e4d0a3f01e9947d44fa111bc85aa08/pkg/ring/ring.proto#L10).
Desc is a proto.Message with a list of instances descriptions. It is used to store the data for each pod and
saved on a supported KV store. Currently, Cortex supports memberlist, consul and etcd as KV stores. Memberlist works
implementing a gossip protocol while consul and etcd are a KV store service.

The ring is used by different services using a different ring key to store and receive the values from the KV store.
For example, ingester service uses the key "ingester" to save and load data from KV. As the saved data is a Desc
struct only one key is used for all the information.

## Problem

Each service using a single key to save and load information creates a concurrency issue when multiple pods are saving
the same key. When using memberlist, the issue is mitigate as the information is owned by all pods and timestamp is used
to confirm latest data. For consul and etcd, all pods compete to update the key at the same time causing an increase on
latency and failures direct related to number of pods running. Cortex and etcd implementation use a version tag to
make sure no data is being overridden causing the problem of write failures.

On a test running cortex with etcd, distributor was scaled to 300 pods and latency increase was noticed coming from etcd usage.
We can also notice 5xx happening when etcd was running.
![Latency using etcd](/images/proposals/ring-multikey-latency.png)

17:14 - Running memberlist, p99 around 5ms
17:25 - Running etcd, p99 around 200ms
17:25 to 17:34 migrating to multikey
After running etcd multikey poc, p99 around 25ms

## Proposal

### Multikey interface

The proposal is separate the current Desc struct which contains a list of key value in multiple keys. Instead of saving one
"ingester" key, the KV store will have "ingester-1", "ingester-2" keys saved.

Current:
```
Key: ingester/ring/ingester
Value:
{
  "ingesters": {
    "ingester-0": {
      "addr": "10.0.0.1:9095",
      "timestamp": 1660760278,
      "tokens": [
        1,
        2
      ],
      "zone": "us-west-2b",
      "registered_timestamp": 1660708390
    },
    "ingester-1": ...
  }
}
```

Proposal:
```
Key: ingester/ring/ingester-0
Value:
{
    "addr": "10.0.0.1:9095",
    "timestamp": 1660760278,
    "tokens": [
        1,
        15
    ],
    "zone": "us-west-2b",
    "registered_timestamp": 1660708390
}

Key: ingester/ring/ingester-1
Value:
{
    "addr": "10.0.0.2:9095",
    "timestamp": 1660760378,
    "tokens": [
        5,
        28
    ],
    "zone": "us-west-2b",
    "registered_timestamp": 1660708572
}
```

The proposal is to create an interface called MultiKey. The interface allows KV store to request the codec to split and
join the values is separated keys.

```
type MultiKey interface {
    SplitById() map[string]interface{}

    JoinIds(map[string]interface{}) Multikey

    GetChildFactory() proto.Message

    FindDifference(MultiKey) (Multikey, []string, error)
}
```

* SplitById - responsible to split the codec in multiple keys and interface.
* JoinIds - responsible to receive multiple keys and interface creating the codec objec
* GetChildFactory - Allow the kv store to know how to serialize and deserialize the interface returned by “SplitById”.
  The interface returned by SplitById need to be a proto.Message
* FindDifference - optimization used to know what need to be updated or deleted from a codec. This avoids updating all keys every
  time the coded change. First parameter returns a subset of the Multikey to be updated. Second is a list of keys to
  be deleted.

The codec implementation will change to support multiple keys. Currently, the codec interface for KV store supports
only Encode and Decode. New methods will be added which would be used only by the KV stores implementing the multi
key functionality.

```
type Codec interface {
    //Existen
    Decode([]byte) (interface{}, error)
    Encode(interface{}) ([]byte, error)
    CodecID() string

    //Proposed
    DecodeMultiKey(map[string][]byte) (interface{}, error)
    EncodeMultiKey(interface{}) (map[string][]byte, error)
}
```

* DecodeMultiKey - called by KV store to decode data downloaded. This function will use the JoinIds method.
* EncodeMultiKey - called by KV store to encode data to be saved. This function will use the SplitById method.

The new KV store will know the data being saved is a reference for multikey. It will use the FindDifference
to know which keys need to be updated. The codec implementation for the new methods will use the JoinIds and SplitById
to know how to separate the codec in multiple keys. The DecodeMultiKey will also use GetChildFactory to know how to
decode the data stored in the kv store.

Example of CAS being used with multikey design:
![Sequence diagram](/images/proposals/ring-multikey-sequence.png)