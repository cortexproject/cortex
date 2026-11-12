package ring

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-kit/log"
)

var errDescRingNoKVStore = errors.New("ring is not backed by a KV store")

// NewRingFromDesc returns a Ring which does not watch a KV store. Its state is
// set by calling UpdateDesc, typically with a Desc another component has already
// read, so that a process can use a second view of a ring it already holds
// without watching the store again.
//
// It answers everything a watched Ring does from the last Desc it was given. It
// must not be started as a service.
//
// It exports no metrics, so it does not compute the detailed per-member ones a
// watched ring updates on every topology change.
func NewRingFromDesc(cfg Config, name string, logger log.Logger) (*Ring, error) {
	cfg.DetailedMetricsEnabled = false
	return NewWithStoreClientAndStrategy(cfg, name, "", &descClient{}, NewDefaultReplicationStrategy(), nil, logger)
}

// UpdateDesc sets the state of a Ring created by NewRingFromDesc to desc. As with
// a watched ring, an update which changes only instance states and timestamps is
// cheap, and keeps the cached shuffle-shard subrings.
//
// The Ring keeps desc, which must not be modified afterwards.
func (r *Ring) UpdateDesc(desc *Desc) {
	if client, ok := r.KVClient.(*descClient); ok {
		client.setLastUpdate(time.Now())
	}
	if len(r.cfg.ExcludedZones) > 0 {
		// updateRingState deletes the instances of excluded zones from the Desc
		// it is given, so give it a copy.
		desc = cloneDesc(desc)
	}
	r.updateRingState(desc)
}

// cloneDesc returns a copy of d which shares nothing with it, so that either can
// be modified without affecting the other.
func cloneDesc(d *Desc) *Desc {
	if d == nil {
		return nil
	}
	c := &Desc{Ingesters: make(map[string]InstanceDesc, len(d.Ingesters))}
	for id, instance := range d.Ingesters {
		instance.Tokens = append([]uint32(nil), instance.Tokens...)
		c.Ingesters[id] = instance
	}
	return c
}

// ownershipMayHaveChanged reports whether next differs from prev in anything
// series ownership depends on: the set of instances, their zones, tokens and
// states. Heartbeat timestamps are ignored. A nil prev always differs.
func ownershipMayHaveChanged(prev, next *Desc) bool {
	if prev == nil || next == nil {
		return prev != next
	}
	switch prev.RingCompare(next) {
	case Equal:
		return false
	case EqualButStatesAndTimestamps:
		for id, instance := range next.Ingesters {
			if prev.Ingesters[id].State != instance.State {
				return true
			}
		}
		return false
	default:
		return true
	}
}

// descClient is the KV client of a Ring created by NewRingFromDesc. There is no
// store behind it. It only reports when the ring was last updated, which the
// replication strategy compares heartbeats against.
type descClient struct {
	mu         sync.Mutex
	lastUpdate time.Time
}

func (c *descClient) setLastUpdate(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastUpdate = t
}

func (c *descClient) LastUpdateTime(_ string) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastUpdate
}

func (c *descClient) List(context.Context, string) ([]string, error) {
	return nil, errDescRingNoKVStore
}

func (c *descClient) Get(context.Context, string) (any, error) {
	return nil, errDescRingNoKVStore
}

func (c *descClient) Delete(context.Context, string) error {
	return errDescRingNoKVStore
}

func (c *descClient) CAS(context.Context, string, func(in any) (out any, retry bool, err error)) error {
	return errDescRingNoKVStore
}

func (c *descClient) WatchKey(context.Context, string, func(any) bool) {}

func (c *descClient) WatchPrefix(context.Context, string, func(string, any) bool) {}
