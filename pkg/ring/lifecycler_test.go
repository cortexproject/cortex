package ring

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/test"
)

type flushTransferer struct {
	lifecycler *Lifecycler
}

func (f *flushTransferer) StopIncomingRequests() {}
func (f *flushTransferer) Flush()                {}
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
	return lifecyclerConfig
}

func checkDenormalised(d interface{}, id string) bool {
	desc, ok := d.(*Desc)
	return ok &&
		len(desc.Ingesters) == 1 &&
		desc.Ingesters[id].State == ACTIVE &&
		len(desc.Ingesters[id].Tokens) == 0 &&
		len(desc.Tokens) == 1
}

func checkNormalised(d interface{}, id string) bool {
	desc, ok := d.(*Desc)
	return ok &&
		len(desc.Ingesters) == 1 &&
		desc.Ingesters[id].State == ACTIVE &&
		len(desc.Ingesters[id].Tokens) == 1 &&
		len(desc.Tokens) == 0
}

func TestRingNormaliseMigration(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())

	r, err := New(ringConfig, "ingester")
	require.NoError(t, err)
	defer r.Stop()

	// Add an 'ingester' with denormalised tokens.
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")

	ft := &flushTransferer{}
	l1, err := NewLifecycler(lifecyclerConfig1, ft, "ingester")
	require.NoError(t, err)
	l1.Start()

	// Check this ingester joined, is active, and has one token.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)
		return checkDenormalised(d, "ing1")
	})

	token := l1.tokens[0]

	// Add a second ingester with normalised tokens.
	var lifecyclerConfig2 = testLifecyclerConfig(ringConfig, "ing2")
	lifecyclerConfig2.JoinAfter = 100 * time.Second
	lifecyclerConfig2.NormaliseTokens = true

	l2, err := NewLifecycler(lifecyclerConfig2, &flushTransferer{}, "ingester")
	require.NoError(t, err)
	l2.Start()

	// This will block until l1 has successfully left the ring.
	ft.lifecycler = l2 // When l1 shutsdown, call l2.ClaimTokensFor("ing1")
	l1.Shutdown()

	// Check the new ingester joined, has the same token, and is active.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)
		return checkNormalised(d, "ing2") &&
			d.(*Desc).Ingesters["ing2"].Tokens[0] == token
	})
}

func TestLifecycler_HealthyInstancesCount(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())

	r, err := New(ringConfig, "ingester")
	require.NoError(t, err)
	defer r.Stop()

	// Add the first ingester to the ring
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")
	lifecyclerConfig1.HeartbeatPeriod = 100 * time.Millisecond
	lifecyclerConfig1.JoinAfter = 100 * time.Millisecond

	lifecycler1, err := NewLifecycler(lifecyclerConfig1, &flushTransferer{}, "ingester")
	require.NoError(t, err)
	assert.Equal(t, 0, lifecycler1.HealthyInstancesCount())

	lifecycler1.Start()

	// Assert the first ingester joined the ring
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		return lifecycler1.HealthyInstancesCount() == 1
	})

	// Add the second ingester to the ring
	lifecyclerConfig2 := testLifecyclerConfig(ringConfig, "ing2")
	lifecyclerConfig2.HeartbeatPeriod = 100 * time.Millisecond
	lifecyclerConfig2.JoinAfter = 100 * time.Millisecond

	lifecycler2, err := NewLifecycler(lifecyclerConfig2, &flushTransferer{}, "ingester")
	require.NoError(t, err)
	assert.Equal(t, 0, lifecycler2.HealthyInstancesCount())

	lifecycler2.Start()

	// Assert the second ingester joined the ring
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		return lifecycler2.HealthyInstancesCount() == 2
	})

	// Assert the first ingester count is updated
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		return lifecycler1.HealthyInstancesCount() == 2
	})
}

type nopFlushTransferer struct{}

func (f *nopFlushTransferer) StopIncomingRequests() {}
func (f *nopFlushTransferer) Flush()                {}
func (f *nopFlushTransferer) TransferOut(ctx context.Context) error {
	panic("should not be called")
}

func TestRingRestart(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	codec := GetCodec()
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(codec)

	r, err := New(ringConfig, "ingester")
	require.NoError(t, err)
	defer r.Stop()

	// Add an 'ingester' with normalised tokens.
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")
	lifecyclerConfig1.NormaliseTokens = true
	l1, err := NewLifecycler(lifecyclerConfig1, &nopFlushTransferer{}, "ingester")
	require.NoError(t, err)
	l1.Start()

	// Check this ingester joined, is active, and has one token.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)
		return checkNormalised(d, "ing1")
	})

	token := l1.tokens[0]

	// Add a second ingester with the same settings, so it will think it has restarted
	l2, err := NewLifecycler(lifecyclerConfig1, &nopFlushTransferer{}, "ingester")
	require.NoError(t, err)
	l2.Start()

	// Check the new ingester picked up the same token
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
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

	r, err := New(ringConfig, "ingester")
	require.NoError(t, err)
	defer r.Stop()
	cfg := testLifecyclerConfig(ringConfig, "ring1")
	cfg.MinReadyDuration = 1 * time.Nanosecond
	l1, err := NewLifecycler(cfg, &nopFlushTransferer{}, "ingester")
	l1.setTokens([]uint32{1})
	l1.Start()
	require.NoError(t, err)

	// Delete the ring key before checking ready
	err = l1.CheckReady(context.Background())
	require.Error(t, err)
}
