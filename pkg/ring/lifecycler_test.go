package ring

import (
	"context"
	"io/ioutil"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

type flushTransferer struct {
	lifecycler *Lifecycler
}

func (f *flushTransferer) Flush() {}
func (f *flushTransferer) TransferOut(ctx context.Context) error {
	if err := f.lifecycler.ClaimTokensFor(ctx, "ing1"); err != nil {
		return err
	}
	return f.lifecycler.ChangeState(ctx, ACTIVE)
}

func testLifecyclerConfig(ringConfig Config, id string) LifecyclerConfig {
	var lifecyclerConfig LifecyclerConfig
	flagext.DefaultValues(&lifecyclerConfig)
	lifecyclerConfig.Addr = "0.0.0.0"
	lifecyclerConfig.Port = 1
	lifecyclerConfig.RingConfig = ringConfig
	lifecyclerConfig.NumTokens = 1
	lifecyclerConfig.ID = id
	lifecyclerConfig.FinalSleep = 0
	lifecyclerConfig.HeartbeatPeriod = 100 * time.Millisecond

	return lifecyclerConfig
}

func checkNormalised(d interface{}, id string) bool {
	desc, ok := d.(*Desc)
	return ok &&
		len(desc.Ingesters) == 1 &&
		desc.Ingesters[id].State == ACTIVE &&
		len(desc.Ingesters[id].Tokens) == 1
}

func TestLifecycler_HealthyInstancesCount(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())

	r, err := New(ringConfig, "ingester", IngesterRingKey)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	// Add the first ingester to the ring
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")
	lifecyclerConfig1.HeartbeatPeriod = 100 * time.Millisecond
	lifecyclerConfig1.JoinAfter = 100 * time.Millisecond

	lifecycler1, err := NewLifecycler(lifecyclerConfig1, &flushTransferer{}, "ingester", IngesterRingKey, true)
	require.NoError(t, err)
	assert.Equal(t, 0, lifecycler1.HealthyInstancesCount())

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), lifecycler1))

	// Assert the first ingester joined the ring
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		return lifecycler1.HealthyInstancesCount() == 1
	})

	// Add the second ingester to the ring
	lifecyclerConfig2 := testLifecyclerConfig(ringConfig, "ing2")
	lifecyclerConfig2.HeartbeatPeriod = 100 * time.Millisecond
	lifecyclerConfig2.JoinAfter = 100 * time.Millisecond

	lifecycler2, err := NewLifecycler(lifecyclerConfig2, &flushTransferer{}, "ingester", IngesterRingKey, true)
	require.NoError(t, err)
	assert.Equal(t, 0, lifecycler2.HealthyInstancesCount())

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), lifecycler2))

	// Assert the second ingester joined the ring
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		return lifecycler2.HealthyInstancesCount() == 2
	})

	// Assert the first ingester count is updated
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		return lifecycler1.HealthyInstancesCount() == 2
	})
}

func TestLifecycler_NilFlushTransferer(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())
	lifecyclerConfig := testLifecyclerConfig(ringConfig, "ing1")

	// Create a lifecycler with nil FlushTransferer to make sure it operates correctly
	lifecycler, err := NewLifecycler(lifecyclerConfig, nil, "ingester", IngesterRingKey, true)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), lifecycler))

	// Ensure the lifecycler joined the ring
	test.Poll(t, time.Second, 1, func() interface{} {
		return lifecycler.HealthyInstancesCount()
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), lifecycler))

	assert.Equal(t, 0, lifecycler.HealthyInstancesCount())
}

func TestLifecycler_TwoRingsWithDifferentKeysOnTheSameKVStore(t *testing.T) {
	// Create a shared ring
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())

	// Create two lifecyclers, each on a separate ring
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "instance-1")
	lifecyclerConfig2 := testLifecyclerConfig(ringConfig, "instance-2")

	lifecycler1, err := NewLifecycler(lifecyclerConfig1, nil, "service-1", "ring-1", true)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), lifecycler1))
	defer services.StopAndAwaitTerminated(context.Background(), lifecycler1) //nolint:errcheck

	lifecycler2, err := NewLifecycler(lifecyclerConfig2, nil, "service-2", "ring-2", true)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), lifecycler2))
	defer services.StopAndAwaitTerminated(context.Background(), lifecycler2) //nolint:errcheck

	// Ensure each lifecycler reports 1 healthy instance, because they're
	// in a different ring
	test.Poll(t, time.Second, 1, func() interface{} {
		return lifecycler1.HealthyInstancesCount()
	})

	test.Poll(t, time.Second, 1, func() interface{} {
		return lifecycler2.HealthyInstancesCount()
	})
}

type nopFlushTransferer struct{}

func (f *nopFlushTransferer) Flush() {}
func (f *nopFlushTransferer) TransferOut(ctx context.Context) error {
	panic("should not be called")
}

func TestRingRestart(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	codec := GetCodec()
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(codec)

	r, err := New(ringConfig, "ingester", IngesterRingKey)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	// Add an 'ingester' with normalised tokens.
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")
	l1, err := NewLifecycler(lifecyclerConfig1, &nopFlushTransferer{}, "ingester", IngesterRingKey, true)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), l1))

	// Check this ingester joined, is active, and has one token.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), IngesterRingKey)
		require.NoError(t, err)
		return checkNormalised(d, "ing1")
	})

	token := l1.tokens[0]

	// Add a second ingester with the same settings, so it will think it has restarted
	l2, err := NewLifecycler(lifecyclerConfig1, &nopFlushTransferer{}, "ingester", IngesterRingKey, true)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), l2))

	// Check the new ingester picked up the same token
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), IngesterRingKey)
		require.NoError(t, err)
		l2Tokens := l2.getTokens()
		return checkNormalised(d, "ing1") &&
			len(l2Tokens) == 1 &&
			l2Tokens[0] == token
	})
}

type MockClient struct {
	GetFunc         func(ctx context.Context, key string) (interface{}, error)
	CASFunc         func(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error
	WatchKeyFunc    func(ctx context.Context, key string, f func(interface{}) bool)
	WatchPrefixFunc func(ctx context.Context, prefix string, f func(string, interface{}) bool)
}

func (m *MockClient) Get(ctx context.Context, key string) (interface{}, error) {
	if m.GetFunc != nil {
		return m.GetFunc(ctx, key)
	}

	return nil, nil
}

func (m *MockClient) CAS(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	if m.CASFunc != nil {
		return m.CASFunc(ctx, key, f)
	}

	return nil
}

func (m *MockClient) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	if m.WatchKeyFunc != nil {
		m.WatchKeyFunc(ctx, key, f)
	}
}

func (m *MockClient) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	if m.WatchPrefixFunc != nil {
		m.WatchPrefixFunc(ctx, prefix, f)
	}
}

// Ensure a check ready returns error when consul returns a nil key and the ingester already holds keys. This happens if the ring key gets deleted
func TestCheckReady(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = &MockClient{}

	r, err := New(ringConfig, "ingester", IngesterRingKey)
	require.NoError(t, err)
	require.NoError(t, r.StartAsync(context.Background()))
	// This is very atypical, but if we used AwaitRunning, that would fail, because of how quickly service terminates ...
	// by the time we check for Running state, it is already terminated, because mock ring has no WatchFunc, so it
	// will just exit.
	require.NoError(t, r.AwaitTerminated(context.Background()))

	cfg := testLifecyclerConfig(ringConfig, "ring1")
	cfg.MinReadyDuration = 1 * time.Nanosecond
	l1, err := NewLifecycler(cfg, &nopFlushTransferer{}, "ingester", IngesterRingKey, true)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), l1))

	l1.setTokens(Tokens([]uint32{1}))

	// Delete the ring key before checking ready
	err = l1.CheckReady(context.Background())
	require.Error(t, err)
}

type noopFlushTransferer struct {
}

func (f *noopFlushTransferer) Flush()                                {}
func (f *noopFlushTransferer) TransferOut(ctx context.Context) error { return nil }

func TestTokensOnDisk(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())

	r, err := New(ringConfig, "ingester", IngesterRingKey)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	tokenDir, err := ioutil.TempDir(os.TempDir(), "tokens_on_disk")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(tokenDir))
	}()

	lifecyclerConfig := testLifecyclerConfig(ringConfig, "ing1")
	lifecyclerConfig.NumTokens = 512
	lifecyclerConfig.TokensFilePath = tokenDir + "/tokens"

	// Start first ingester.
	l1, err := NewLifecycler(lifecyclerConfig, &noopFlushTransferer{}, "ingester", IngesterRingKey, true)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), l1))

	// Check this ingester joined, is active, and has 512 token.
	var expTokens []uint32
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), IngesterRingKey)
		require.NoError(t, err)
		desc, ok := d.(*Desc)
		if ok {
			expTokens = desc.Ingesters["ing1"].Tokens
		}
		return ok &&
			len(desc.Ingesters) == 1 &&
			desc.Ingesters["ing1"].State == ACTIVE &&
			len(desc.Ingesters["ing1"].Tokens) == 512
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), l1))

	// Start new ingester at same token directory.
	lifecyclerConfig.ID = "ing2"
	l2, err := NewLifecycler(lifecyclerConfig, &noopFlushTransferer{}, "ingester", IngesterRingKey, true)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), l2))
	defer services.StopAndAwaitTerminated(context.Background(), l2) //nolint:errcheck

	// Check this ingester joined, is active, and has 512 token.
	var actTokens []uint32
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), IngesterRingKey)
		require.NoError(t, err)
		desc, ok := d.(*Desc)
		if ok {
			actTokens = desc.Ingesters["ing2"].Tokens
		}
		return ok &&
			len(desc.Ingesters) == 1 &&
			desc.Ingesters["ing2"].State == ACTIVE &&
			len(desc.Ingesters["ing2"].Tokens) == 512
	})

	// Check for same tokens.
	sort.Slice(expTokens, func(i, j int) bool { return expTokens[i] < expTokens[j] })
	sort.Slice(actTokens, func(i, j int) bool { return actTokens[i] < actTokens[j] })
	for i := 0; i < 512; i++ {
		require.Equal(t, expTokens, actTokens)
	}
}

// JoinInLeavingState ensures that if the lifecycler starts up and the ring already has it in a LEAVING state that it still is able to auto join
func TestJoinInLeavingState(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	codec := GetCodec()
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(codec)

	r, err := New(ringConfig, "ingester", IngesterRingKey)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	cfg := testLifecyclerConfig(ringConfig, "ing1")
	cfg.NumTokens = 2
	cfg.MinReadyDuration = 1 * time.Nanosecond

	// Set state as LEAVING
	err = r.KVClient.CAS(context.Background(), IngesterRingKey, func(in interface{}) (interface{}, bool, error) {
		r := &Desc{
			Ingesters: map[string]IngesterDesc{
				"ing1": {
					State:  LEAVING,
					Tokens: []uint32{1, 4},
				},
				"ing2": {
					Tokens: []uint32{2, 3},
				},
			},
		}

		return r, true, nil
	})
	require.NoError(t, err)

	l1, err := NewLifecycler(cfg, &nopFlushTransferer{}, "ingester", IngesterRingKey, true)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), l1))

	// Check that the lifecycler was able to join after coming up in LEAVING
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), IngesterRingKey)
		require.NoError(t, err)
		desc, ok := d.(*Desc)
		return ok &&
			len(desc.Ingesters) == 2 &&
			desc.Ingesters["ing1"].State == ACTIVE &&
			len(desc.Ingesters["ing1"].Tokens) == cfg.NumTokens &&
			len(desc.Ingesters["ing2"].Tokens) == 2
	})
}
