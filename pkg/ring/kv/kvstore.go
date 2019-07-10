package kv

import (
	"context"
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
)

// KVClient is a high-level client for key-value stores (such as Etcd and
// Consul) that exposes operations such as CAS and Watch which take callbacks.
// It also deals with serialisation by using a Codec and having a instance of
// the the desired type passed in to methods ala json.Unmarshal.
type KVClient interface {
	CAS(ctx context.Context, key string, f CASCallback) error

	// WatchKey calls f whenever the value stored under key changes.
	WatchKey(ctx context.Context, key string, f func(interface{}) bool)

	// WatchPrefix calls f whenever any value stored under prefix changes.
	WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool)

	Get(ctx context.Context, key string) (interface{}, error)
	PutBytes(ctx context.Context, key string, buf []byte) error
}

// CASCallback is the type of the callback to CAS.  If err is nil, out must be non-nil.
type CASCallback func(in interface{}) (out interface{}, retry bool, err error)

// Codec allows KV clients to serialise and deserialise values.
type Codec interface {
	Decode([]byte) (interface{}, error)
	Encode(interface{}) ([]byte, error)
}

// ProtoCodec is a Codec for proto/snappy
type ProtoCodec struct {
	Factory func() proto.Message
}

// Decode implements Codec
func (p ProtoCodec) Decode(bytes []byte) (interface{}, error) {
	out := p.Factory()
	bytes, err := snappy.Decode(nil, bytes)
	if err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(bytes, out); err != nil {
		return nil, err
	}
	return out, nil
}

// Encode implements Codec
func (p ProtoCodec) Encode(msg interface{}) ([]byte, error) {
	bytes, err := proto.Marshal(msg.(proto.Message))
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, bytes), nil
}

type prefixedKVClient struct {
	prefix string
	client KVClient
}

// PrefixClient takes a KVClient and forces a prefix on all its operations.
func PrefixClient(client KVClient, prefix string) KVClient {
	return &prefixedKVClient{prefix, client}
}

// CAS atomically modifies a value in a callback. If the value doesn't exist,
// you'll get 'nil' as an argument to your callback.
func (c *prefixedKVClient) CAS(ctx context.Context, key string, f CASCallback) error {
	return c.client.CAS(ctx, c.prefix+key, f)
}

// WatchKey watches a key.
func (c *prefixedKVClient) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	c.client.WatchKey(ctx, c.prefix+key, f)
}

// WatchPrefix watches a prefix. For a prefix client it appends the prefix argument to the clients prefix.
func (c *prefixedKVClient) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	c.client.WatchPrefix(ctx, fmt.Sprintf("%s%s", c.prefix, prefix), func(k string, i interface{}) bool {
		return f(strings.TrimPrefix(k, c.prefix), i)
	})
}

// PutBytes writes bytes to the KVClient.
func (c *prefixedKVClient) PutBytes(ctx context.Context, key string, buf []byte) error {
	return c.client.PutBytes(ctx, c.prefix+key, buf)
}

func (c *prefixedKVClient) Get(ctx context.Context, key string) (interface{}, error) {
	return c.client.Get(ctx, c.prefix+key)
}
