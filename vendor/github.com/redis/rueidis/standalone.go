package rueidis

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/redis/rueidis/internal/cmds"
)

func newStandaloneClient(opt *ClientOption, connFn connFn, retryer retryHandler) (*standalone, error) {
	if len(opt.InitAddress) == 0 {
		return nil, ErrNoAddr
	}
	p := connFn(opt.InitAddress[0], opt)
	if err := p.Dial(); err != nil {
		return nil, err
	}
	s := &standalone{
		toReplicas: opt.SendToReplicas,
		primary:    newSingleClientWithConn(p, cmds.NewBuilder(cmds.NoSlot), !opt.DisableRetry, opt.DisableCache, retryer, false),
		replicas:   make([]*singleClient, len(opt.Standalone.ReplicaAddress)),
	}
	opt.ReplicaOnly = true
	for i := range s.replicas {
		replicaConn := connFn(opt.Standalone.ReplicaAddress[i], opt)
		if err := replicaConn.Dial(); err != nil {
			s.primary.Close() // close primary if any replica fails
			for j := 0; j < i; j++ {
				s.replicas[j].Close()
			}
			return nil, err
		}
		s.replicas[i] = newSingleClientWithConn(replicaConn, cmds.NewBuilder(cmds.NoSlot), !opt.DisableRetry, opt.DisableCache, retryer, false)
	}
	return s, nil
}

type standalone struct {
	toReplicas func(Completed) bool
	primary    *singleClient
	replicas   []*singleClient
}

func (s *standalone) B() Builder {
	return s.primary.B()
}

func (s *standalone) pick() int {
	if len(s.replicas) == 1 {
		return 0
	}
	return rand.IntN(len(s.replicas))
}

func (s *standalone) Do(ctx context.Context, cmd Completed) (resp RedisResult) {
	if s.toReplicas(cmd) {
		return s.replicas[s.pick()].Do(ctx, cmd)
	}
	return s.primary.Do(ctx, cmd)
}

func (s *standalone) DoMulti(ctx context.Context, multi ...Completed) (resp []RedisResult) {
	toReplica := true
	for _, cmd := range multi {
		if !s.toReplicas(cmd) {
			toReplica = false
			break
		}
	}
	if toReplica {
		return s.replicas[s.pick()].DoMulti(ctx, multi...)
	}
	return s.primary.DoMulti(ctx, multi...)
}

func (s *standalone) Receive(ctx context.Context, subscribe Completed, fn func(msg PubSubMessage)) error {
	if s.toReplicas(subscribe) {
		return s.replicas[s.pick()].Receive(ctx, subscribe, fn)
	}
	return s.primary.Receive(ctx, subscribe, fn)
}

func (s *standalone) Close() {
	s.primary.Close()
	for _, replica := range s.replicas {
		replica.Close()
	}
}

func (s *standalone) DoCache(ctx context.Context, cmd Cacheable, ttl time.Duration) (resp RedisResult) {
	return s.primary.DoCache(ctx, cmd, ttl)
}

func (s *standalone) DoMultiCache(ctx context.Context, multi ...CacheableTTL) (resp []RedisResult) {
	return s.primary.DoMultiCache(ctx, multi...)
}

func (s *standalone) DoStream(ctx context.Context, cmd Completed) RedisResultStream {
	if s.toReplicas(cmd) {
		return s.replicas[s.pick()].DoStream(ctx, cmd)
	}
	return s.primary.DoStream(ctx, cmd)
}

func (s *standalone) DoMultiStream(ctx context.Context, multi ...Completed) MultiRedisResultStream {
	toReplica := true
	for _, cmd := range multi {
		if !s.toReplicas(cmd) {
			toReplica = false
			break
		}
	}
	if toReplica {
		return s.replicas[s.pick()].DoMultiStream(ctx, multi...)
	}
	return s.primary.DoMultiStream(ctx, multi...)
}

func (s *standalone) Dedicated(fn func(DedicatedClient) error) (err error) {
	return s.primary.Dedicated(fn)
}

func (s *standalone) Dedicate() (client DedicatedClient, cancel func()) {
	return s.primary.Dedicate()
}

func (s *standalone) Nodes() map[string]Client {
	nodes := make(map[string]Client, len(s.replicas)+1)
	for addr, client := range s.primary.Nodes() {
		nodes[addr] = client
	}
	for _, replica := range s.replicas {
		for addr, client := range replica.Nodes() {
			nodes[addr] = client
		}
	}
	return nodes
}

func (s *standalone) Mode() ClientMode {
	return ClientModeStandalone
}
