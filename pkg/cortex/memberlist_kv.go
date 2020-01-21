package cortex

import (
	"sync"

	"github.com/cortexproject/cortex/pkg/ring/kv/memberlist"
)

// This struct holds state of initialization of memberlist.KV instance.
type memberlistKVState struct {
	// config used for initialization
	cfg *memberlist.KVConfig

	// init function, to avoid multiple initializations.
	init sync.Once

	// state
	kv  *memberlist.KV
	err error
}

func newMemberlistKVState(cfg *memberlist.KVConfig) *memberlistKVState {
	return &memberlistKVState{
		cfg: cfg,
	}
}

func (kvs *memberlistKVState) getMemberlistKV() (*memberlist.KV, error) {
	kvs.init.Do(func() {
		kvs.kv, kvs.err = memberlist.NewKV(*kvs.cfg)
	})

	return kvs.kv, kvs.err
}
