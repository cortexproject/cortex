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
	lifecyclerConfig.HeartbeatPeriod = 100 * time.Millisecond

	return lifecyclerConfig
}

func checkDenormalisedLeaving(d interface{}, id string) bool {
	desc, ok := d.(*Desc)
	return ok &&
		len(desc.Ingesters) == 1 &&
		desc.Ingesters[id].State == LEAVING &&
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

	r, err := New(ringConfig, "ingester", IngesterRingKey)
	require.NoError(t, err)
	defer r.Stop()

	// Add an 'ingester' with denormalised tokens.
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")

	// Since code to insert ingester with denormalised tokens into ring was removed,
	// instead of running lifecycler, we do it manually here.
	token := uint32(0)
	err = r.KVClient.CAS(context.Background(), IngesterRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
		require.Nil(t, in)
		r := NewDesc()
		tks := GenerateTokens(lifecyclerConfig1.NumTokens, nil)
		r.Ingesters[lifecyclerConfig1.ID] = IngesterDesc{
			Addr:      lifecyclerConfig1.Addr,
			Timestamp: time.Now().Unix(),
			State:     LEAVING, // expected by second ingester`
		}
		for _, t := range tks {
			r.Tokens = append(r.Tokens, TokenDesc{
				Token:    t,
				Ingester: lifecyclerConfig1.ID,
			})
		}
		token = tks[0]
		return r, true, nil
	})
	require.NoError(t, err)

	// Check this ingester joined, is active, and has one token.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), IngesterRingKey)
		require.NoError(t, err)
		return checkDenormalisedLeaving(d, "ing1")
	})

	// Add a second ingester with normalised tokens.
	var lifecyclerConfig2 = testLifecyclerConfig(ringConfig, "ing2")
	lifecyclerConfig2.JoinAfter = 100 * time.Second

	l2, err := NewLifecycler(lifecyclerConfig2, &flushTransferer{}, "ingester", IngesterRingKey)
	require.NoError(t, err)
	l2.Start()

	// Since there is nothing that would make l2 to claim tokens from l1 (normally done on transfer)
	// we do it manually.
	require.NoError(t, l2.ClaimTokensFor(context.Background(), "ing1"))
	require.NoError(t, l2.ChangeState(context.Background(), ACTIVE))

	// Check the new ingester joined, has the same token, and is active.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), IngesterRingKey)
		require.NoError(t, err)

		if desc, ok := d.(*Desc); ok {
			// lifecycler for ingester 1 isn't running, so we need to delete it manually
			// (to make checkNormalised happy)
			delete(desc.Ingesters, lifecyclerConfig1.ID)
		}
		return checkNormalised(d, "ing2") &&
			d.(*Desc).Ingesters["ing2"].Tokens[0] == token
	})
}

func TestLifecycler_HealthyInstancesCount(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())

	r, err := New(ringConfig, "ingester", IngesterRingKey)
	require.NoError(t, err)
	defer r.Stop()

	// Add the first ingester to the ring
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")
	lifecyclerConfig1.HeartbeatPeriod = 100 * time.Millisecond
	lifecyclerConfig1.JoinAfter = 100 * time.Millisecond

	lifecycler1, err := NewLifecycler(lifecyclerConfig1, &flushTransferer{}, "ingester", IngesterRingKey)
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

	lifecycler2, err := NewLifecycler(lifecyclerConfig2, &flushTransferer{}, "ingester", IngesterRingKey)
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

func TestLifecycler_NilFlushTransferer(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())
	lifecyclerConfig := testLifecyclerConfig(ringConfig, "ing1")

	// Create a lifecycler with nil FlushTransferer to make sure it operates correctly
	lifecycler, err := NewLifecycler(lifecyclerConfig, nil, "ingester", IngesterRingKey)
	require.NoError(t, err)
	lifecycler.Start()

	// Ensure the lifecycler joined the ring
	test.Poll(t, time.Second, 1, func() interface{} {
		return lifecycler.HealthyInstancesCount()
	})

	lifecycler.Shutdown()
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

	lifecycler1, err := NewLifecycler(lifecyclerConfig1, nil, "service-1", "ring-1")
	require.NoError(t, err)
	lifecycler1.Start()
	defer lifecycler1.Shutdown()

	lifecycler2, err := NewLifecycler(lifecyclerConfig2, nil, "service-2", "ring-2")
	require.NoError(t, err)
	lifecycler2.Start()
	defer lifecycler2.Shutdown()

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

	r, err := New(ringConfig, "ingester", IngesterRingKey)
	require.NoError(t, err)
	defer r.Stop()

	// Add an 'ingester' with normalised tokens.
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")
	l1, err := NewLifecycler(lifecyclerConfig1, &nopFlushTransferer{}, "ingester", IngesterRingKey)
	require.NoError(t, err)
	l1.Start()

	// Check this ingester joined, is active, and has one token.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), IngesterRingKey)
		require.NoError(t, err)
		return checkNormalised(d, "ing1")
	})

	token := l1.tokens[0]

	// Add a second ingester with the same settings, so it will think it has restarted
	l2, err := NewLifecycler(lifecyclerConfig1, &nopFlushTransferer{}, "ingester", IngesterRingKey)
	require.NoError(t, err)
	l2.Start()

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

func (m *MockClient) Stop() {
	// nothing to do
}

// Ensure a check ready returns error when consul returns a nil key and the ingester already holds keys. This happens if the ring key gets deleted
func TestCheckReady(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = &MockClient{}

	r, err := New(ringConfig, "ingester", IngesterRingKey)
	require.NoError(t, err)
	defer r.Stop()
	cfg := testLifecyclerConfig(ringConfig, "ring1")
	cfg.MinReadyDuration = 1 * time.Nanosecond
	l1, err := NewLifecycler(cfg, &nopFlushTransferer{}, "ingester", IngesterRingKey)
	l1.Start()
	require.NoError(t, err)

	l1.setTokens([]uint32{1})

	// Delete the ring key before checking ready
	err = l1.CheckReady(context.Background())
	require.Error(t, err)
}

type noopFlushTransferer struct {
	lifecycler *Lifecycler
}

func (f *noopFlushTransferer) StopIncomingRequests()                 {}
func (f *noopFlushTransferer) Flush()                                {}
func (f *noopFlushTransferer) TransferOut(ctx context.Context) error { return nil }

func TestTokensOnDisk(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())

	r, err := New(ringConfig, "ingester", IngesterRingKey)
	require.NoError(t, err)
	defer r.Stop()

	tokenDir, err := ioutil.TempDir(os.TempDir(), "tokens_on_disk")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(tokenDir))
	}()

	lifecyclerConfig := testLifecyclerConfig(ringConfig, "ing1")
	lifecyclerConfig.NumTokens = 512
	lifecyclerConfig.TokensFilePath = tokenDir + "/tokens"

	// Start first ingester.
	l1, err := NewLifecycler(lifecyclerConfig, &noopFlushTransferer{}, "ingester", IngesterRingKey)
	require.NoError(t, err)
	l1.Start()
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
			len(desc.Ingesters["ing1"].Tokens) == 512 &&
			len(desc.Tokens) == 0
	})

	l1.Shutdown()

	// Start new ingester at same token directory.
	lifecyclerConfig.ID = "ing2"
	l2, err := NewLifecycler(lifecyclerConfig, &noopFlushTransferer{}, "ingester", IngesterRingKey)
	require.NoError(t, err)
	l2.Start()
	defer l2.Shutdown()

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
			len(desc.Ingesters["ing2"].Tokens) == 512 &&
			len(desc.Tokens) == 0
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
	defer r.Stop()

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
			},
			Tokens: []TokenDesc{
				{
					Ingester: "ing2",
					Token:    2,
				},
				{
					Ingester: "ing2",
					Token:    3,
				},
			},
		}

		return r, true, nil
	})
	require.NoError(t, err)

	l1, err := NewLifecycler(cfg, &nopFlushTransferer{}, "ingester", IngesterRingKey)
	l1.Start()
	require.NoError(t, err)

	// Check that the lifecycler was able to join after coming up in LEAVING
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), IngesterRingKey)
		require.NoError(t, err)
		desc, ok := d.(*Desc)
		return ok &&
			len(desc.Ingesters) == 1 &&
			desc.Ingesters["ing1"].State == ACTIVE &&
			len(desc.Ingesters["ing1"].Tokens) == cfg.NumTokens &&
			len(desc.Tokens) == 2
	})
}
