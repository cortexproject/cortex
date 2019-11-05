package kv

import (
	"context"
	"flag"
	"fmt"
	"sync"

	"github.com/go-kit/kit/log/level"
	"github.com/uber-go/atomic"
	"golang.org/x/time/rate"

	"github.com/cortexproject/cortex/pkg/util"
)

// MultiConfig is a configuration for MultiClient.
type MultiConfig struct {
	Primary   string `yaml:"primary"`
	Secondary string `yaml:"secondary"`

	MirrorRateLimit float64 `yaml:"mirror-rate-limit"`
	MirrorRateBurst int     `yaml:"mirror-rate-burst"`
	MirrorEnabled   bool    `yaml:"mirror-enabled"`
	MirrorPrefix    string  `yaml:"mirror-prefix"`

	// ConfigProvider returns channel with MultiRuntimeConfig updates.
	ConfigProvider func() <-chan MultiRuntimeConfig
}

// RegisterFlagsWithPrefix registers flags with prefix.
func (cfg *MultiConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Primary, prefix+"multi.primary", "", "Primary backend storage used by multi-client.")
	f.StringVar(&cfg.Secondary, prefix+"multi.secondary", "", "Secondary backend storage used by multi-client.")
	f.StringVar(&cfg.MirrorPrefix, prefix+"multi.mirror-prefix", "", "Prefix for keys that should be mirrored")
	f.BoolVar(&cfg.MirrorEnabled, prefix+"multi.mirror-enabled", false, "Mirror values to secondary store")
	f.Float64Var(&cfg.MirrorRateLimit, prefix+"multi.mirror-rate-limit", 1, "Rate limit used for mirroring. 0 disables rate limit.")
	f.IntVar(&cfg.MirrorRateBurst, prefix+"multi.mirror-burst-size", 1, "Burst size used by rate limiter. Values less than 1 are treated as 1.")
}

// MultiRuntimeConfig has values that can change in runtime (via overrides)
type MultiRuntimeConfig struct {
	// Primary store used by MultiClient. Can be updated in runtime to switch to a different store (eg. consul -> etcd,
	// or to gossip). Doing this allows nice migration between stores. Empty values are ignored.
	PrimaryStore string `yaml:"primary"`

	// Put "enabled", "enable" or "true" here to enable. Any other value = disabled. Empty = ignore.
	// We don't use bool here, because it would not be possible to distinguish between false = missing value,
	// or false = disabled.
	Mirroring string `yaml:"mirroring"`
}

type kvclient struct {
	client Client
	name   string
}

type clientInProgress struct {
	client int
	cancel context.CancelFunc
}

// MultiClient implements kv.Client by forwarding all API calls to primary client.
// At the same time, MultiClient watches for changes in values in the primary store,
// and forwards them to remaining clients.
type MultiClient struct {
	// Available KV clients
	clients []kvclient

	rateLimiter      *rate.Limiter
	mirroringEnabled *atomic.Bool

	// Primary client used for interaction. Values from this KV are copied to all the others.
	primaryID *atomic.Int32

	cancel context.CancelFunc

	inProgressMu sync.Mutex
	// Cancel functions for ongoing operations. key is a value from inProgressCnt.
	// What we really need is a []context.CancelFunc, but functions cannot be compared against each other using ==,
	// so we use this map instead.
	inProgress    map[int]clientInProgress
	inProgressCnt int
}

// NewMultiClient creates new MultiClient with given KV Clients.
// First client in the slice is the primary client.
// Channel is used to get notifications about what store to use as primary.
func NewMultiClient(cfg MultiConfig, clients []kvclient) *MultiClient {
	c := &MultiClient{
		clients:    clients,
		primaryID:  atomic.NewInt32(0),
		inProgress: map[int]clientInProgress{},

		rateLimiter:      createRateLimiter(cfg.MirrorRateLimit, cfg.MirrorRateBurst),
		mirroringEnabled: atomic.NewBool(cfg.MirrorEnabled),
	}

	ctx, cancelFn := context.WithCancel(context.Background())
	c.cancel = cancelFn

	// Start mirroring.
	go c.mirrorChanges(ctx, cfg.MirrorPrefix)

	if cfg.ConfigProvider != nil {
		go c.watchConfigChannel(cfg.ConfigProvider())
	}

	return c
}

func (m *MultiClient) watchConfigChannel(configChannel <-chan MultiRuntimeConfig) {
	for cfg := range configChannel {
		if cfg.Mirroring != "" {
			enable := cfg.Mirroring == "true" || cfg.Mirroring == "enable" || cfg.Mirroring == "enabled"

			old := m.mirroringEnabled.Swap(enable)
			if old != enable {
				level.Info(util.Logger).Log("msg", "toggled mirroring", "enabled", enable)
			}
		}

		if cfg.PrimaryStore != "" {
			switched, err := m.setNewPrimaryClient(cfg.PrimaryStore)
			if switched {
				level.Info(util.Logger).Log("msg", "switched primary KV store", "primary", cfg.PrimaryStore)
			}
			if err != nil {
				level.Error(util.Logger).Log("msg", "failed to switch primary KV store", "primary", cfg.PrimaryStore, "err", err)
			}
		}
	}
}

func (m *MultiClient) getPrimaryClient() (int, kvclient) {
	v := m.primaryID.Load()
	return int(v), m.clients[v]
}

// returns true, if primary client has changed
func (m *MultiClient) setNewPrimaryClient(store string) (bool, error) {
	newPrimaryIx := -1
	for ix, c := range m.clients {
		if c.name == store {
			newPrimaryIx = ix
			break
		}
	}

	if newPrimaryIx < 0 {
		return false, fmt.Errorf("KV store not found")
	}

	prev := int(m.primaryID.Swap(int32(newPrimaryIx)))
	if prev == newPrimaryIx {
		return false, nil
	}

	// switching to new primary... cancel clients using previous one
	m.inProgressMu.Lock()
	defer m.inProgressMu.Unlock()

	for _, inp := range m.inProgress {
		if inp.client == prev {
			inp.cancel()
		}
	}
	return true, nil
}

func (m *MultiClient) registerCancelFn(clientID int, fn context.CancelFunc) int {
	m.inProgressMu.Lock()
	defer m.inProgressMu.Unlock()

	m.inProgressCnt++
	id := m.inProgressCnt
	m.inProgress[id] = clientInProgress{client: clientID, cancel: fn}
	return id
}

func (m *MultiClient) unregisterCancelFn(id int) {
	m.inProgressMu.Lock()
	defer m.inProgressMu.Unlock()

	delete(m.inProgress, id)
}

// Runs supplied fn with current primary client. If primary client changes, fn is restarted.
// When fn finishes (with or without error), this method returns given error value.
func (m *MultiClient) runWithPrimaryClient(origCtx context.Context, fn func(newCtx context.Context, primary kvclient) error) error {
	cancelFn := context.CancelFunc(nil)
	cancelFnID := 0

	cleanup := func() {
		if cancelFn != nil {
			cancelFn()
		}
		if cancelFnID > 0 {
			m.unregisterCancelFn(cancelFnID)
		}
	}

	defer cleanup()

	// This only loops if switchover to a new primary backend happens while calling 'fn', which is very rare.
	for {
		cleanup()
		pid, kv := m.getPrimaryClient()

		var cancelCtx context.Context
		cancelCtx, cancelFn = context.WithCancel(origCtx)
		cancelFnID = m.registerCancelFn(pid, cancelFn)

		err := fn(cancelCtx, kv)

		if err == nil {
			return nil
		}

		if cancelCtx.Err() == context.Canceled && origCtx.Err() == nil {
			// our context was cancelled, but outer context is not done yet. retry
			continue
		}

		return err
	}
}

// Get is a part of kv.Client interface
func (m *MultiClient) Get(ctx context.Context, key string) (interface{}, error) {
	var result interface{}
	err := m.runWithPrimaryClient(ctx, func(newCtx context.Context, primary kvclient) error {
		var err2 error
		result, err2 = primary.client.Get(newCtx, key)
		return err2
	})
	return result, err
}

// CAS is a part of kv.Client interface
func (m *MultiClient) CAS(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	return m.runWithPrimaryClient(ctx, func(newCtx context.Context, primary kvclient) error {
		return primary.client.CAS(newCtx, key, f)
	})
}

// WatchKey is a part of kv.Client interface
func (m *MultiClient) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	_ = m.runWithPrimaryClient(ctx, func(newCtx context.Context, primary kvclient) error {
		primary.client.WatchKey(newCtx, key, f)
		return newCtx.Err()
	})
}

// WatchPrefix is a part of kv.Client interface
func (m *MultiClient) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	_ = m.runWithPrimaryClient(ctx, func(newCtx context.Context, primary kvclient) error {
		primary.client.WatchPrefix(newCtx, prefix, f)
		return newCtx.Err()
	})
}

// mirrorChanges performs mirroring -- it watches for changes in the primary client, and puts them into secondary.
func (m *MultiClient) mirrorChanges(ctx context.Context, prefix string) {
	for ctx.Err() == nil {
		err := m.runWithPrimaryClient(ctx, func(newCtx context.Context, primary kvclient) error {
			primary.client.WatchPrefix(newCtx, prefix, func(key string, val interface{}) bool {
				level.Debug(util.Logger).Log("msg", "value updated", "key", key, "store", primary.name)

				if m.mirroringEnabled.Load() {
					// don't pass new context to keyUpdated, we don't want to react on primary client changes here.
					m.keyUpdated(ctx, primary, key, val)
				}

				err := m.rateLimiter.Wait(newCtx)
				if err != nil && err != context.Canceled {
					level.Error(util.Logger).Log("msg", "error while rate limiting multi-client", "err", err)
				}

				return true
			})

			return ctx.Err()
		})

		if err != nil {
			level.Warn(util.Logger).Log("msg", "watching changes returned error", "err", err)
		}
	}
}

func (m *MultiClient) keyUpdated(ctx context.Context, primary kvclient, key string, newValue interface{}) {
	// let's propagate this to all remaining clients
	for _, kvc := range m.clients {
		if kvc == primary {
			continue
		}

		stored := true
		err := kvc.client.CAS(ctx, key, func(in interface{}) (out interface{}, retry bool, err error) {
			stored = true

			if eq, ok := in.(withEqual); ok && eq.Equal(newValue) {
				stored = false
				level.Debug(util.Logger).Log("msg", "no change", "key", key)
				return nil, false, nil
			}

			// try once
			return in, false, nil
		})

		if err != nil {
			level.Error(util.Logger).Log("msg", "failed to update value in secondary store", "key", key, "err", err, "primary", primary.name, "secondary", kvc.name)
		} else if stored {
			level.Info(util.Logger).Log("msg", "stored updated value to secondary store", "key", key, "primary", primary.name, "secondary", kvc.name)
		}
	}
}

// Stop the multiClient (mirroring part), and all configured clients.
func (m *MultiClient) Stop() {
	m.cancel()

	for _, kv := range m.clients {
		kv.client.Stop()
	}
}

func createRateLimiter(rateLimit float64, burst int) *rate.Limiter {
	if rateLimit <= 0 {
		// burst is ignored when limit = rate.Inf
		return rate.NewLimiter(rate.Inf, 0)
	}
	if burst < 1 {
		burst = 1
	}
	return rate.NewLimiter(rate.Limit(rateLimit), burst)
}

type withEqual interface {
	Equal(that interface{}) bool
}
