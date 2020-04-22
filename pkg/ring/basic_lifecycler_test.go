package ring

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

const (
	testRingKey    = "test"
	testRingName   = "test"
	testInstanceID = "test-id"
)

func TestBasicLifecycler_RegisterOnStart(t *testing.T) {
	tests := map[string]struct {
		initialInstanceID   string
		initialInstanceDesc *IngesterDesc
		registerState       IngesterState
		registerTokens      Tokens
	}{
		"initial ring is empty": {
			registerState:  ACTIVE,
			registerTokens: Tokens{1, 2, 3, 4, 5},
		},
		"initial ring non empty (containing another instance)": {
			initialInstanceID: "instance-1",
			initialInstanceDesc: &IngesterDesc{
				Addr:   "1.1.1.1",
				State:  ACTIVE,
				Tokens: Tokens{6, 7, 8, 9, 10},
			},
			registerState:  ACTIVE,
			registerTokens: Tokens{1, 2, 3, 4, 5},
		},
		"initial ring contains the same instance with a different address and tokens": {
			initialInstanceID: testInstanceID,
			initialInstanceDesc: &IngesterDesc{
				Addr:   "1.1.1.1",
				State:  ACTIVE,
				Tokens: Tokens{6, 7, 8, 9, 10},
			},
			registerState:  JOINING,
			registerTokens: Tokens{1, 2, 3, 4, 5},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()
			cfg := prepareBasicLifecyclerConfig()
			lifecycler, delegate, store, err := prepareBasicLifecycler(cfg)
			require.NoError(t, err)
			defer services.StopAndAwaitTerminated(ctx, lifecycler) //nolint:errcheck

			// Add an initial instance to the ring.
			if testData.initialInstanceDesc != nil {
				require.NoError(t, store.CAS(ctx, testRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
					desc := testData.initialInstanceDesc

					ringDesc := GetOrCreateRingDesc(in)
					ringDesc.AddIngester(testData.initialInstanceID, desc.Addr, desc.Zone, desc.Tokens, desc.State)
					return ringDesc, true, nil
				}))
			}

			// Assert on the lifecycler state once the instance register delegate function will be called.
			delegate.onRegister = func(_ *BasicLifecycler, ringDesc Desc, instanceExists bool, instanceID string, instanceDesc IngesterDesc) (IngesterState, Tokens) {
				assert.Equal(t, services.Starting, lifecycler.State())
				assert.False(t, lifecycler.IsRegistered())
				assert.Equal(t, testInstanceID, instanceID)
				assert.NotNil(t, ringDesc)

				if testData.initialInstanceID == instanceID {
					assert.True(t, instanceExists)
					assert.Equal(t, testData.initialInstanceDesc.Addr, instanceDesc.Addr)
					assert.Equal(t, testData.initialInstanceDesc.Zone, instanceDesc.Zone)
					assert.Equal(t, testData.initialInstanceDesc.State, instanceDesc.State)
					assert.Equal(t, testData.initialInstanceDesc.Tokens, instanceDesc.Tokens)
				} else {
					assert.False(t, instanceExists)
				}

				return testData.registerState, testData.registerTokens
			}

			assert.Equal(t, testInstanceID, lifecycler.GetInstanceID())
			assert.Equal(t, services.New, lifecycler.State())
			assert.Equal(t, PENDING, lifecycler.GetState())
			assert.Empty(t, lifecycler.GetTokens())
			assert.False(t, lifecycler.IsRegistered())
			assert.Equal(t, float64(0), testutil.ToFloat64(lifecycler.metrics.tokensOwned))
			assert.Equal(t, float64(cfg.NumTokens), testutil.ToFloat64(lifecycler.metrics.tokensToOwn))

			require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))

			assert.Equal(t, services.Running, lifecycler.State())
			assert.Equal(t, testData.registerState, lifecycler.GetState())
			assert.Equal(t, testData.registerTokens, lifecycler.GetTokens())
			assert.True(t, lifecycler.IsRegistered())
			assert.Equal(t, float64(cfg.NumTokens), testutil.ToFloat64(lifecycler.metrics.tokensOwned))
			assert.Equal(t, float64(cfg.NumTokens), testutil.ToFloat64(lifecycler.metrics.tokensToOwn))

			// Assert on the instance registered within the ring.
			instanceDesc, ok := getInstanceFromStore(t, store, testInstanceID)
			assert.True(t, ok)
			assert.Equal(t, cfg.Addr, instanceDesc.GetAddr())
			assert.Equal(t, testData.registerState, instanceDesc.GetState())
			assert.Equal(t, testData.registerTokens, Tokens(instanceDesc.GetTokens()))
			assert.Equal(t, cfg.Zone, instanceDesc.GetZone())
		})
	}
}

func TestBasicLifecycler_UnregisterOnStop(t *testing.T) {
	ctx := context.Background()
	cfg := prepareBasicLifecyclerConfig()
	lifecycler, delegate, store, err := prepareBasicLifecycler(cfg)
	require.NoError(t, err)

	delegate.onRegister = func(_ *BasicLifecycler, _ Desc, _ bool, _ string, _ IngesterDesc) (IngesterState, Tokens) {
		return ACTIVE, Tokens{1, 2, 3, 4, 5}
	}
	delegate.onStopping = func(_ *BasicLifecycler) {
		assert.Equal(t, services.Stopping, lifecycler.State())
	}

	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
	assert.Equal(t, ACTIVE, lifecycler.GetState())
	assert.Equal(t, Tokens{1, 2, 3, 4, 5}, lifecycler.GetTokens())
	assert.True(t, lifecycler.IsRegistered())
	assert.Equal(t, float64(cfg.NumTokens), testutil.ToFloat64(lifecycler.metrics.tokensOwned))
	assert.Equal(t, float64(cfg.NumTokens), testutil.ToFloat64(lifecycler.metrics.tokensToOwn))

	require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))
	assert.Equal(t, PENDING, lifecycler.GetState())
	assert.Equal(t, Tokens{}, lifecycler.GetTokens())
	assert.False(t, lifecycler.IsRegistered())
	assert.Equal(t, float64(0), testutil.ToFloat64(lifecycler.metrics.tokensOwned))
	assert.Equal(t, float64(0), testutil.ToFloat64(lifecycler.metrics.tokensToOwn))

	// Assert on the instance removed from the ring.
	_, ok := getInstanceFromStore(t, store, testInstanceID)
	assert.False(t, ok)
}

func TestBasicLifecycler_HeartbeatWhileRunning(t *testing.T) {
	ctx := context.Background()
	cfg := prepareBasicLifecyclerConfig()
	cfg.HeartbeatPeriod = 10 * time.Millisecond

	lifecycler, _, store, err := prepareBasicLifecycler(cfg)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(ctx, lifecycler) //nolint:errcheck
	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))

	// Get the initial timestamp so that we can then assert on the timestamp updated.
	desc, _ := getInstanceFromStore(t, store, testInstanceID)
	initialTimestamp := desc.GetTimestamp()

	test.Poll(t, time.Second, true, func() interface{} {
		desc, _ := getInstanceFromStore(t, store, testInstanceID)
		currTimestamp := desc.GetTimestamp()

		return currTimestamp > initialTimestamp
	})

	assert.Greater(t, testutil.ToFloat64(lifecycler.metrics.heartbeats), float64(0))
}

func TestBasicLifecycler_HeartbeatWhileStopping(t *testing.T) {
	ctx := context.Background()
	cfg := prepareBasicLifecyclerConfig()
	cfg.HeartbeatPeriod = 10 * time.Millisecond

	lifecycler, delegate, store, err := prepareBasicLifecycler(cfg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))

	// Get the initial timestamp so that we can then assert on the timestamp updated.
	desc, _ := getInstanceFromStore(t, store, testInstanceID)
	initialTimestamp := desc.GetTimestamp()
	onStoppingCalled := false

	delegate.onStopping = func(_ *BasicLifecycler) {
		test.Poll(t, time.Second, true, func() interface{} {
			desc, _ := getInstanceFromStore(t, store, testInstanceID)
			currTimestamp := desc.GetTimestamp()

			return currTimestamp > initialTimestamp
		})

		onStoppingCalled = true
	}

	assert.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))
	assert.True(t, onStoppingCalled)
}

func TestBasicLifecycler_HeartbeatAfterBackendRest(t *testing.T) {
	ctx := context.Background()
	cfg := prepareBasicLifecyclerConfig()
	cfg.HeartbeatPeriod = 10 * time.Millisecond

	lifecycler, delegate, store, err := prepareBasicLifecycler(cfg)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(ctx, lifecycler) //nolint:errcheck

	registerTokens := Tokens{1, 2, 3, 4, 5}
	delegate.onRegister = func(_ *BasicLifecycler, _ Desc, _ bool, _ string, _ IngesterDesc) (state IngesterState, tokens Tokens) {
		return ACTIVE, registerTokens
	}

	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))

	// At this point the instance has been registered to the ring. Now we delete it
	// from the ring to simulate a ring storage reset and we expect the next heartbeat
	// will restore it.
	require.NoError(t, store.CAS(ctx, testRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
		return NewDesc(), true, nil
	}))

	test.Poll(t, time.Second, true, func() interface{} {
		desc, ok := getInstanceFromStore(t, store, testInstanceID)
		return ok && desc.GetTimestamp() > 0 && desc.GetState() == ACTIVE && Tokens(desc.GetTokens()).Equals(registerTokens) && desc.GetAddr() == cfg.Addr
	})
}

func TestBasicLifecycler_ChangeState(t *testing.T) {
	ctx := context.Background()
	cfg := prepareBasicLifecyclerConfig()
	lifecycler, delegate, store, err := prepareBasicLifecycler(cfg)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(ctx, lifecycler) //nolint:errcheck

	delegate.onRegister = func(_ *BasicLifecycler, _ Desc, _ bool, _ string, _ IngesterDesc) (IngesterState, Tokens) {
		return JOINING, Tokens{1, 2, 3, 4, 5}
	}

	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
	assert.Equal(t, JOINING, lifecycler.GetState())

	for _, state := range []IngesterState{ACTIVE, LEAVING} {
		assert.NoError(t, lifecycler.ChangeState(ctx, state))
		assert.Equal(t, state, lifecycler.GetState())

		// Assert on the instance state read from the ring.
		desc, ok := getInstanceFromStore(t, store, testInstanceID)
		assert.True(t, ok)
		assert.Equal(t, state, desc.GetState())
	}
}

func TestBasicLifecycler_TokensObservePeriod(t *testing.T) {
	ctx := context.Background()
	cfg := prepareBasicLifecyclerConfig()
	cfg.NumTokens = 5
	cfg.TokensObservePeriod = time.Second

	lifecycler, delegate, store, err := prepareBasicLifecycler(cfg)
	require.NoError(t, err)

	delegate.onRegister = func(_ *BasicLifecycler, _ Desc, _ bool, _ string, _ IngesterDesc) (IngesterState, Tokens) {
		return ACTIVE, Tokens{1, 2, 3, 4, 5}
	}

	require.NoError(t, lifecycler.StartAsync(ctx))

	// While the lifecycler is starting we poll the ring. As soon as the instance
	// is registered, we remove some tokens to simulate how gossip memberlist
	// reconciliation works in case of clashing tokens.
	test.Poll(t, time.Second, true, func() interface{} {
		// Ensure the instance has been registered in the ring.
		desc, ok := getInstanceFromStore(t, store, testInstanceID)
		if !ok {
			return false
		}

		// Remove some tokens.
		return store.CAS(ctx, testRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
			ringDesc := GetOrCreateRingDesc(in)
			ringDesc.AddIngester(testInstanceID, desc.Addr, desc.Zone, Tokens{4, 5}, desc.State)
			return ringDesc, true, nil
		}) == nil
	})

	require.NoError(t, lifecycler.AwaitRunning(ctx))
	assert.Subset(t, lifecycler.GetTokens(), Tokens{4, 5})
	assert.NotContains(t, lifecycler.GetTokens(), uint32(1))
	assert.NotContains(t, lifecycler.GetTokens(), uint32(2))
	assert.NotContains(t, lifecycler.GetTokens(), uint32(3))
}

func TestBasicLifecycler_updateInstance_ShouldAddInstanceToTheRingIfDoesNotExistEvenIfNotChanged(t *testing.T) {
	ctx := context.Background()
	cfg := prepareBasicLifecyclerConfig()
	cfg.HeartbeatPeriod = time.Hour // No heartbeat during the test.

	lifecycler, delegate, store, err := prepareBasicLifecycler(cfg)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(ctx, lifecycler) //nolint:errcheck

	registerTokens := Tokens{1, 2, 3, 4, 5}
	delegate.onRegister = func(_ *BasicLifecycler, _ Desc, _ bool, _ string, _ IngesterDesc) (state IngesterState, tokens Tokens) {
		return ACTIVE, registerTokens
	}

	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))

	// At this point the instance has been registered to the ring. Now we delete it
	// from the ring to simulate a ring storage reset.
	require.NoError(t, store.CAS(ctx, testRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
		return NewDesc(), true, nil
	}))

	// Run a noop update instance, but since the instance is not in the ring we do expect
	// it will added back anyway.
	require.NoError(t, lifecycler.updateInstance(ctx, func(_ Desc, desc *IngesterDesc) bool {
		return false
	}))

	desc, ok := getInstanceFromStore(t, store, testInstanceID)
	require.True(t, ok)
	assert.Equal(t, ACTIVE, desc.GetState())
	assert.Equal(t, registerTokens, Tokens(desc.GetTokens()))
	assert.Equal(t, cfg.Addr, desc.GetAddr())
}

func prepareBasicLifecyclerConfig() BasicLifecyclerConfig {
	return BasicLifecyclerConfig{
		ID:                  testInstanceID,
		Addr:                "127.0.0.1:12345",
		Zone:                "test-zone",
		HeartbeatPeriod:     time.Minute,
		TokensObservePeriod: 0,
		NumTokens:           5,
	}
}

func prepareBasicLifecycler(cfg BasicLifecyclerConfig) (*BasicLifecycler, *mockDelegate, kv.Client, error) {
	delegate := &mockDelegate{}
	lifecycler, store, err := prepareBasicLifecyclerWithDelegate(cfg, delegate)
	return lifecycler, delegate, store, err
}

func prepareBasicLifecyclerWithDelegate(cfg BasicLifecyclerConfig, delegate BasicLifecyclerDelegate) (*BasicLifecycler, kv.Client, error) {
	store := consul.NewInMemoryClient(GetCodec())
	lifecycler, err := NewBasicLifecycler(cfg, testRingName, testRingKey, store, delegate, log.NewNopLogger(), nil)
	return lifecycler, store, err
}

type mockDelegate struct {
	onRegister      func(lifecycler *BasicLifecycler, ringDesc Desc, instanceExists bool, instanceID string, instanceDesc IngesterDesc) (IngesterState, Tokens)
	onTokensChanged func(lifecycler *BasicLifecycler, tokens Tokens)
	onStopping      func(lifecycler *BasicLifecycler)
}

func (m *mockDelegate) OnRingInstanceRegister(lifecycler *BasicLifecycler, ringDesc Desc, instanceExists bool, instanceID string, instanceDesc IngesterDesc) (IngesterState, Tokens) {
	if m.onRegister == nil {
		return PENDING, Tokens{}
	}

	return m.onRegister(lifecycler, ringDesc, instanceExists, instanceID, instanceDesc)
}

func (m *mockDelegate) OnRingInstanceTokens(lifecycler *BasicLifecycler, tokens Tokens) {
	if m.onTokensChanged != nil {
		m.onTokensChanged(lifecycler, tokens)
	}
}

func (m *mockDelegate) OnRingInstanceStopping(lifecycler *BasicLifecycler) {
	if m.onStopping != nil {
		m.onStopping(lifecycler)
	}
}

func getInstanceFromStore(t *testing.T, store kv.Client, instanceID string) (IngesterDesc, bool) {
	out, err := store.Get(context.Background(), testRingKey)
	require.NoError(t, err)

	if out == nil {
		return IngesterDesc{}, false
	}

	ringDesc := out.(*Desc)
	instanceDesc, ok := ringDesc.GetIngesters()[instanceID]

	return instanceDesc, ok
}
