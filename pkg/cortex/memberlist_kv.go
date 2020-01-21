package cortex

import (
	"sync"

	"github.com/cortexproject/cortex/pkg/ring/kv/memberlist"
)

type memberlistKVState struct {
	cfg  *memberlist.KVConfig
	init sync.Once
	kv   *memberlist.KV
	err  error
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
