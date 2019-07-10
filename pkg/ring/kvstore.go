package ring

import (
	"flag"
	"fmt"
	"sync"

	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/ring/kv/etcd"
)

var inmemoryStoreInit sync.Once
var inmemoryStore kv.KVClient

// KVConfig is config for a KVStore currently used by ring and HA tracker,
// where store can be consul or inmemory.
type KVConfig struct {
	Store  string        `yaml:"store,omitempty"`
	Consul consul.Config `yaml:"consul,omitempty"`
	Etcd   etcd.Config   `yaml:"etcd,omitempty"`
	Prefix string        `yaml:"prefix,omitempty"`

	Mock kv.KVClient
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet.
// If prefix is an empty string we will register consul flags with no prefix and the
// store flag with the prefix ring, so ring.store. For everything else we pass the prefix
// to the Consul flags.
// If prefix is not an empty string it should end with a period.
func (cfg *KVConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	// We need Consul flags to not have the ring prefix to maintain compatibility.
	// This needs to be fixed in the future (1.0 release maybe?) when we normalize flags.
	// At the moment we have consul.<flag-name>, and ring.store, going forward it would
	// be easier to have everything under ring, so ring.consul.<flag-name>
	cfg.Consul.RegisterFlags(f, prefix)
	cfg.Etcd.RegisterFlags(f, prefix)
	if prefix == "" {
		prefix = "ring."
	}
	f.StringVar(&cfg.Prefix, prefix+"prefix", "", "The prefix for the keys in the store. Should end with a /.")
	f.StringVar(&cfg.Store, prefix+"store", "consul", "Backend storage to use for the ring (consul, etcd, inmemory).")
}

// CASCallback is the type of the callback to CAS.  If err is nil, out must be non-nil.
type CASCallback func(in interface{}) (out interface{}, retry bool, err error)

// NewKVStore creates a new KVstore client (inmemory or consul) based on the config,
// encodes and decodes data for storage using the codec.
func NewKVStore(cfg KVConfig, codec kv.Codec) (kv.KVClient, error) {
	if cfg.Mock != nil {
		return cfg.Mock, nil
	}

	var kvclient kv.KVClient
	var err error

	switch cfg.Store {
	case "consul":
		kvclient, err = consul.NewConsulClient(cfg.Consul, codec)
	case "etcd":
		kvclient, err = etcd.New(cfg.Etcd, codec)
	case "inmemory":
		// If we use the in-memory store, make sure everyone gets the same instance
		// within the same process.
		inmemoryStoreInit.Do(func() {
			inmemoryStore = consul.NewInMemoryKVClient(codec)
		})
		kvclient = inmemoryStore
	default:
		return nil, fmt.Errorf("invalid KV store type: %s", cfg.Store)
	}

	if err != nil {
		return nil, err
	}

	if cfg.Prefix != "" {
		kvclient = kv.PrefixClient(kvclient, cfg.Prefix)
	}

	return kvclient, nil
}

// GetCodec returns the codec used to encode and decode data being put by ring.
func GetCodec() kv.Codec {
	return kv.ProtoCodec{Factory: ProtoDescFactory}
}
