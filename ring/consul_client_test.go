package ring

import (
	"sync"
	"time"

	consul "github.com/hashicorp/consul/api"
	"github.com/prometheus/common/log"
)

type mockKV struct {
	mtx     sync.Mutex
	cond    *sync.Cond
	kvps    map[string]*consul.KVPair
	current uint64 // the current 'index in the log'
}

func newMockConsulClient() ConsulClient {
	m := mockKV{
		kvps: map[string]*consul.KVPair{},
	}
	m.cond = sync.NewCond(&m.mtx)
	go m.loop()
	return &consulClient{
		kv:    &m,
		codec: ProtoCodec{Factory: ProtoDescFactory},
	}
}

func copyKVPair(in *consul.KVPair) *consul.KVPair {
	out := *in
	out.Value = make([]byte, len(in.Value))
	copy(out.Value, in.Value)
	return &out
}

// periodic loop to wake people up, so they can honour timeouts
func (m *mockKV) loop() {
	for range time.Tick(1 * time.Second) {
		m.mtx.Lock()
		m.cond.Broadcast()
		m.mtx.Unlock()
	}
}

func (m *mockKV) Put(p *consul.KVPair, q *consul.WriteOptions) (*consul.WriteMeta, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.current++
	existing, ok := m.kvps[p.Key]
	if ok {
		existing.Value = p.Value
		existing.ModifyIndex = m.current
	} else {
		m.kvps[p.Key] = &consul.KVPair{
			Key:         p.Key,
			Value:       p.Value,
			CreateIndex: m.current,
			ModifyIndex: m.current,
		}
	}

	m.cond.Broadcast()
	return nil, nil
}

func (m *mockKV) CAS(p *consul.KVPair, q *consul.WriteOptions) (bool, *consul.WriteMeta, error) {
	log.Debugf("CAS %s (%d) <- %s", p.Key, p.ModifyIndex, p.Value)

	m.mtx.Lock()
	defer m.mtx.Unlock()
	existing, ok := m.kvps[p.Key]
	if ok && existing.ModifyIndex != p.ModifyIndex {
		return false, nil, nil
	}

	m.current++
	if ok {
		existing.Value = p.Value
		existing.ModifyIndex = m.current
	} else {
		m.kvps[p.Key] = &consul.KVPair{
			Key:         p.Key,
			Value:       p.Value,
			CreateIndex: m.current,
			ModifyIndex: m.current,
		}
	}

	m.cond.Broadcast()
	return true, nil, nil
}

func (m *mockKV) Get(key string, q *consul.QueryOptions) (*consul.KVPair, *consul.QueryMeta, error) {
	log.Debugf("Get %s (%d)", key, q.WaitIndex)

	m.mtx.Lock()
	defer m.mtx.Unlock()

	value, ok := m.kvps[key]
	if !ok {
		log.Debugf("Get %s - not found", key)
		return nil, &consul.QueryMeta{LastIndex: m.current}, nil
	}

	if q.WaitTime > 0 {
		deadline := time.Now().Add(q.WaitTime)
		for q.WaitIndex >= value.ModifyIndex && time.Now().Before(deadline) {
			m.cond.Wait()
		}
		if time.Now().After(deadline) {
			log.Debugf("Get %s - deadline exceeded", key)
			return nil, &consul.QueryMeta{LastIndex: q.WaitIndex}, nil
		}
	}

	log.Debugf("Get %s (%d) = %s", key, value.ModifyIndex, value.Value)
	return copyKVPair(value), &consul.QueryMeta{LastIndex: value.ModifyIndex}, nil
}

func (m *mockKV) List(prefix string, q *consul.QueryOptions) (consul.KVPairs, *consul.QueryMeta, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	deadline := time.Now().Add(q.WaitTime)
	for q.WaitIndex >= m.current && time.Now().Before(deadline) {
		m.cond.Wait()
	}
	if time.Now().After(deadline) {
		return nil, &consul.QueryMeta{LastIndex: q.WaitIndex}, nil
	}

	result := consul.KVPairs{}
	for _, kvp := range m.kvps {
		if kvp.ModifyIndex >= q.WaitIndex {
			result = append(result, copyKVPair(kvp))
		}
	}
	return result, &consul.QueryMeta{LastIndex: m.current}, nil
}
