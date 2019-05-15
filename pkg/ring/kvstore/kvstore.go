package kvstore

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
)

// KVClient is a high-level client for Consul, that exposes operations
// such as CAS and Watch which take callbacks.  It also deals with serialisation
// by having an instance factory passed in to methods and deserialising into that.
type KVClient interface {
	CAS(ctx context.Context, key string, f CASCallback) error
	WatchKey(ctx context.Context, key string, f func(interface{}) bool)
	WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool)
	Get(ctx context.Context, key string) (interface{}, error)
	PutBytes(ctx context.Context, key string, buf []byte) error
}

// CASCallback is the type of the callback to CAS.  If err is nil, out must be non-nil.
type CASCallback func(in interface{}) (out interface{}, retry bool, err error)

// Codec allows the consult client to serialise and deserialise values.
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
