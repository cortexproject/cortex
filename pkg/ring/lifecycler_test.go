package ring

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring/kv"
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
	lifecyclerConfig.TransferFinishDelay = time.Duration(0)

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
	defer r.Stop()

	// Add the first ingester to the ring
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")
	lifecyclerConfig1.HeartbeatPeriod = 100 * time.Millisecond
	lifecyclerConfig1.JoinAfter = 100 * time.Millisecond

	lifecycler1, err := NewLifecycler(lifecyclerConfig1, &flushTransferer{}, nil, "ingester", IngesterRingKey, true)
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

	lifecycler2, err := NewLifecycler(lifecyclerConfig2, &flushTransferer{}, nil, "ingester", IngesterRingKey, true)
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
	lifecycler, err := NewLifecycler(lifecyclerConfig, nil, nil, "ingester", IngesterRingKey, true)
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

	lifecycler1, err := NewLifecycler(lifecyclerConfig1, nil, nil, "service-1", "ring-1", true)
	require.NoError(t, err)
	lifecycler1.Start()
	defer lifecycler1.Shutdown()

	lifecycler2, err := NewLifecycler(lifecyclerConfig2, nil, nil, "service-2", "ring-2", true)
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

type nopFlushTransferer struct {
	allowTransfer bool
}

func (f *nopFlushTransferer) StopIncomingRequests() {}
func (f *nopFlushTransferer) Flush()                {}
func (f *nopFlushTransferer) TransferOut(ctx context.Context) error {
	if !f.allowTransfer {
		panic("should not be called")
	}
	return nil
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
	l1, err := NewLifecycler(lifecyclerConfig1, &nopFlushTransferer{}, nil, "ingester", IngesterRingKey, true)
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
	l2, err := NewLifecycler(lifecyclerConfig1, &nopFlushTransferer{allowTransfer: true}, nil, "ingester", IngesterRingKey, true)
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

func (m *MockClient) MapFunctions(client kv.Client) {
	m.GetFunc = client.Get
	m.CASFunc = client.CAS
	m.WatchKeyFunc = client.WatchKey
	m.WatchPrefixFunc = client.WatchPrefix
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
	defer r.Stop()
	cfg := testLifecyclerConfig(ringConfig, "ring1")
	cfg.MinReadyDuration = 1 * time.Nanosecond
	l1, err := NewLifecycler(cfg, &nopFlushTransferer{}, nil, "ingester", IngesterRingKey, true)
	l1.Start()
	require.NoError(t, err)

	l1.setTokens(Tokens([]uint32{1}))

	// Delete the ring key before checking ready
	err = l1.CheckReady(context.Background())
	require.Error(t, err)
}

type noopFlushTransferer struct{}

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
	l1, err := NewLifecycler(lifecyclerConfig, &noopFlushTransferer{}, nil, "ingester", IngesterRingKey, true)
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
			len(desc.Ingesters["ing1"].Tokens) == 512
	})

	l1.Shutdown()

	// Start new ingester at same token directory.
	lifecyclerConfig.ID = "ing2"
	l2, err := NewLifecycler(lifecyclerConfig, &noopFlushTransferer{}, nil, "ingester", IngesterRingKey, true)
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
				"ing2": {
					Tokens: []uint32{2, 3},
				},
			},
		}

		return r, true, nil
	})
	require.NoError(t, err)

	l1, err := NewLifecycler(cfg, &nopFlushTransferer{}, nil, "ingester", IngesterRingKey, true)
	l1.Start()
	require.NoError(t, err)

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

func TestFindTransferWorkload(t *testing.T) {
	tt := []struct {
		name        string
		ring        string
		token       string
		replication int
		expect      transferWorkload
	}{
		{
			name:        "joining: single new token",
			ring:        "A B C D I+ E F G H",
			token:       "I+",
			replication: 3,
			expect: transferWorkload{
				"E": []TokenRange{{2, 3}}, // transfer BC from E
				"F": []TokenRange{{3, 4}}, // transfer CD from F
				"G": []TokenRange{{4, 5}}, // transfer DI From G
			},
		},

		{
			name:        "joining: single new token around duplicates",
			ring:        "A1 A2 B1 B2 C1 C2 D1 D2 I+ E1 E2 F1 F2 G1 G2 H1 H2",
			token:       "I+",
			replication: 3,
			expect: transferWorkload{
				"E": []TokenRange{{4, 5}, {5, 6}}, // transfer B2C1, C1C2 from E
				"F": []TokenRange{{6, 7}, {7, 8}}, // transfer C2D1, D1D2 from F
				"G": []TokenRange{{8, 9}},         // transfer D2I from G
			},
		},

		{
			name:        "joining: single new token around mixed duplicates",
			ring:        "A1 B1 A2 B2 C1 D1 C2 D2 I+ E1 F1 E2 F2 G1 H1 G2 H2",
			token:       "I+",
			replication: 3,
			expect: transferWorkload{
				"E": []TokenRange{{4, 5}, {5, 6}, {6, 7}}, // transfer B2C1, D1C2, C1D1 from E
				"F": []TokenRange{{7, 8}},                 // transfer C2D2 from F
				"G": []TokenRange{{8, 9}},                 // transfer D2I from G
			},
		},

		{
			name:        "joining: token at start of list",
			ring:        "A+ B C D E F G H",
			token:       "A+",
			replication: 3,
			expect: transferWorkload{
				"B": []TokenRange{{6, 7}}, // transfer FG from B
				"C": []TokenRange{{7, 8}}, // transfer GH from C
				"D": []TokenRange{{8, 1}}, // transfer HA from D
			},
		},

		{
			name:        "joining: token already added on right",
			ring:        "A B C D I1+ I2 E F G H",
			token:       "I1+",
			replication: 3,
			expect:      transferWorkload{}, // we already own everything
		},

		{
			name:        "joining: token already added on left",
			ring:        "A B C D I1 I2+ E F G H",
			token:       "I2+",
			replication: 3,
			expect: transferWorkload{
				"G": []TokenRange{{5, 6}}, // transfer I1I2 from G
			},
		},

		{
			name:        "joining: token already added 2 ingesters over on right",
			ring:        "A B C I1+ D I2 E F G H",
			token:       "I1+",
			replication: 3,
			expect: transferWorkload{
				"D": []TokenRange{{1, 2}}, // transfer AB from D
			},
		},

		{
			name:        "joining: token already added 2 ingesters over on left",
			ring:        "A B C I1 D I2+ E F G H",
			token:       "I2+",
			replication: 3,
			expect: transferWorkload{
				"F": []TokenRange{{4, 5}}, // transfer ID from F
				"G": []TokenRange{{5, 6}}, // transfer DI from G
			},
		},

		{
			name:        "joining: skip over other joining ingesters",
			ring:        "A B C D I+ Z+ Y? X+ W? E F G H",
			token:       "I+",
			replication: 3,
			expect: transferWorkload{
				"E": []TokenRange{{4, 5}}, // transfer DI from E
				"W": []TokenRange{{3, 4}}, // transfer CD from W
				"Y": []TokenRange{{2, 3}}, // transfer BC from y
			},
		},

		{
			name:        "leaving: single leaving token",
			ring:        "A B C D I- E F G H",
			token:       "I-",
			replication: 3,
			expect: transferWorkload{
				"E": []TokenRange{{2, 3}}, // transfer BC to E
				"F": []TokenRange{{3, 4}}, // transfer CD to F
				"G": []TokenRange{{4, 5}}, // transfer DI to G
			},
		},

		{
			name:        "leaving: active token on right",
			ring:        "A B C D I1- I2 E F G H",
			token:       "I1-",
			replication: 3,
			expect:      transferWorkload{}, // we still own everything after I1 leaves
		},

		{
			name:        "leaving: active token on left",
			ring:        "A B C D I1 I2- E F G H",
			token:       "I2-",
			replication: 3,
			expect: transferWorkload{
				"G": []TokenRange{{5, 6}}, // transfer I1I2 to G
			},
		},

		{
			name:        "leaving: token active 2 ingesters over on right",
			ring:        "A B C I1- D I2 E F G H",
			token:       "I1-",
			replication: 3,
			expect: transferWorkload{
				"D": []TokenRange{{1, 2}}, // transfer AB to D
			},
		},

		{
			name:        "leaving: token active added 2 ingesters over on left",
			ring:        "A B C I1 D I2- E F G H",
			token:       "I2-",
			replication: 3,
			expect: transferWorkload{
				"F": []TokenRange{{4, 5}}, // transfer ID to F
				"G": []TokenRange{{5, 6}}, // transfer DI to G
			},
		},

		{
			name:        "leaving: skip over ingesters not joining/active",
			ring:        "A B C D I- Z- Y? X- W? E F G H",
			token:       "I-",
			replication: 3,
			expect: transferWorkload{
				"E": []TokenRange{{2, 3}}, // transfer BC to E
				"F": []TokenRange{{3, 4}}, // transfer CD to F
				"G": []TokenRange{{4, 5}}, // transfer DI to G
			},
		},

		{
			name:        "joining: interleaved tokens",
			ring:        "A1+ B1 A2 B2",
			token:       "A1+",
			replication: 1,
			expect: transferWorkload{
				"B": []TokenRange{{4, 1}}, // Transfer B2 A1+ from B
			},
		},

		{
			name:        "joining: interleaved tokens",
			ring:        "A1 B1+ A2 B2",
			token:       "B1+",
			replication: 1,
			expect: transferWorkload{
				"A": []TokenRange{{1, 2}}, // Transfer A1 B1+ from A
			},
		},

		{
			name:        "joining: interleaved tokens",
			ring:        "A1 B1 A2+ B2",
			token:       "A2+",
			replication: 1,
			expect: transferWorkload{
				"B": []TokenRange{{2, 3}},
			},
		},

		{
			name:        "joining: interleaved tokens",
			ring:        "A1 B1 A2 B2+",
			token:       "B2+",
			replication: 1,
			expect: transferWorkload{
				"A": []TokenRange{{3, 4}},
			},
		},

		{
			name:        "joining: multiple tokens from same ingester",
			ring:        "B1+ B2 A1 A2 B3 A3 B4 A4",
			token:       "B1+",
			replication: 1,
			expect:      transferWorkload{},
		},

		{
			name:        "joining: multiple tokens from same ingester",
			ring:        "B1 B2+ A1 A2 B3 A3 B4 A4",
			token:       "B2+",
			replication: 1,
			expect: transferWorkload{
				"A": []TokenRange{{1, 2}},
			},
		},

		{
			name:        "joining: multiple tokens from same ingester",
			ring:        "B1 B2 A1 A2 B3+ A3 B4 A4",
			token:       "B3+",
			replication: 1,
			expect: transferWorkload{
				"A": []TokenRange{{4, 5}},
			},
		},

		{
			name:        "joining: multiple tokens from same ingester",
			ring:        "B1 B2 A1 A2 B3 A3 B4+ A4",
			token:       "B4+",
			replication: 1,
			expect: transferWorkload{
				"A": []TokenRange{{6, 7}},
			},
		},

		{
			name:        "joining: target can not be same as end range",
			ring:        "A1 B1 C1+ B2 A2 D1 E1",
			token:       "C1+",
			replication: 3,
			expect: transferWorkload{
				"D": []TokenRange{{2, 3}, {1, 2}, {7, 1}},
			},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			d := generateRing(t, tc.ring)
			n := d.Desc.GetNavigator()
			tok := d.TokenDesc(t, tc.token)

			op := Read
			if s, ok := d.tokenStates[tok.Token]; ok && s == LEAVING {
				op = Write
			}
			healthy := d.TokenHealthChecker(op)

			lc := &Lifecycler{
				ID: tok.Ingester,
				cfg: LifecyclerConfig{
					RingConfig: Config{
						ReplicationFactor: tc.replication,
						HeartbeatTimeout:  time.Second * 3600,
					},
				},
			}

			wl, ok := lc.findTransferWorkload(d.Desc, n, tok.Token, healthy)

			require.True(t, ok)
			require.Equal(t, tc.expect, wl)
		})
	}
}

type mockIncrementalJoin struct {
	t *testing.T

	mtx       sync.Mutex
	requested bool
	completed bool

	IncrementalTransferer
}

func (j *mockIncrementalJoin) MemoryStreamTokens() []uint32 { return nil }

func (j *mockIncrementalJoin) RequestComplete(_ context.Context, ranges []TokenRange, addr string) {
	j.mtx.Lock()
	defer j.mtx.Unlock()

	j.completed = true
}

func (j *mockIncrementalJoin) RequestChunkRanges(_ context.Context, ranges []TokenRange, addr string, move bool) error {
	j.mtx.Lock()
	defer j.mtx.Unlock()

	j.requested = true
	return nil
}

func waitIngesterState(t *testing.T, r *Ring, id string, waitTime time.Duration, joined bool) {
	t.Helper()

	test.Poll(t, waitTime, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), "ring")
		require.NoError(t, err)
		if d == nil {
			return false
		}
		i, exist := d.(*Desc).Ingesters[id]
		if joined {
			return exist && i.State == ACTIVE
		}
		return !exist
	})
}

func getLifecyclers(t *testing.T, r *Ring, cfg Config, count int) ([]*Lifecycler, func()) {
	t.Helper()

	ret := []*Lifecycler{}

	for i := 0; i < count; i++ {
		id := fmt.Sprintf("ing-%d", i)

		lcc := testLifecyclerConfig(cfg, id)
		lcc.Addr = id
		lcc.NumTokens = 64
		lc, err := NewLifecycler(lcc, &nopFlushTransferer{allowTransfer: true}, nil, id, "ring", true)
		require.NoError(t, err)
		lc.Start()

		waitIngesterState(t, r, id, time.Millisecond*250, true)
	}

	return ret, func() {
		for _, lc := range ret {
			lc.Shutdown()
		}
	}
}

func testIncrementalRingConfig(t *testing.T) Config {
	t.Helper()

	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	codec := GetCodec()

	inMemory := consul.NewInMemoryClient(codec)
	mockClient := &MockClient{}
	mockClient.MapFunctions(inMemory)
	ringConfig.KVStore.Mock = mockClient

	return ringConfig
}

func TestIncrementalJoin(t *testing.T) {
	ringConfig := testIncrementalRingConfig(t)
	r, err := New(ringConfig, "ingester", "ring")
	require.NoError(t, err)
	defer r.Stop()

	_, stop := getLifecyclers(t, r, ringConfig, 5)
	defer stop()

	mock := mockIncrementalJoin{t: t}

	lcc := testLifecyclerConfig(ringConfig, "joiner")
	lcc.NumTokens = 64
	lcc.JoinIncrementalTransfer = true
	lc, err := NewLifecycler(lcc, &nopFlushTransferer{allowTransfer: true}, &mock, "joiner", "ring", true)
	require.NoError(t, err)
	lc.Start()
	defer lc.Shutdown()

	waitIngesterState(t, r, "joiner", 5000*time.Millisecond, true)

	mock.mtx.Lock()
	defer mock.mtx.Unlock()
	require.Equal(t, mock.requested, true)
	require.Equal(t, mock.completed, true)
}

type mockIncrementalLeave struct {
	t *testing.T

	mtx  sync.Mutex
	sent bool

	IncrementalTransferer
}

func (j *mockIncrementalLeave) MemoryStreamTokens() []uint32 { return nil }

func (j *mockIncrementalLeave) SendChunkRanges(_ context.Context, ranges []TokenRange, addr string) error {
	j.mtx.Lock()
	defer j.mtx.Unlock()

	j.sent = true
	return nil
}

func TestIncrementalLeave(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	codec := GetCodec()

	inMemory := consul.NewInMemoryClient(codec)
	mockClient := &MockClient{}
	mockClient.MapFunctions(inMemory)
	ringConfig.KVStore.Mock = mockClient

	r, err := New(ringConfig, "ingester", "ring")
	require.NoError(t, err)
	defer r.Stop()

	_, stop := getLifecyclers(t, r, ringConfig, 5)
	defer stop()

	mock := mockIncrementalLeave{t: t}

	lcc := testLifecyclerConfig(ringConfig, "leaver")
	lcc.NumTokens = 64
	lcc.LeaveIncrementalTransfer = true
	lc, err := NewLifecycler(lcc, &nopFlushTransferer{allowTransfer: true}, &mock, "leaver", "ring", true)
	require.NoError(t, err)
	lc.Start()

	waitIngesterState(t, r, "leaver", time.Millisecond*250, true)

	lc.Shutdown()
	waitIngesterState(t, r, "leaver", 5000*time.Millisecond, false)

	mock.mtx.Lock()
	defer mock.mtx.Unlock()
	require.Equal(t, mock.sent, true)
}
