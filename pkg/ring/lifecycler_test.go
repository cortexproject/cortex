package ring

import (
	"context"
	"fmt"
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

func testLifecyclerConfig(ringConfig Config, id string) LifecyclerConfig {
	var lifecyclerConfig LifecyclerConfig
	flagext.DefaultValues(&lifecyclerConfig)
	lifecyclerConfig.Addr = "0.0.0.0"
	lifecyclerConfig.Port = 1
	lifecyclerConfig.ListenPort = 0
	lifecyclerConfig.RingConfig = ringConfig
	lifecyclerConfig.NumTokens = 1
	lifecyclerConfig.ID = id
	lifecyclerConfig.Zone = "zone1"
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

	ctx := context.Background()

	// Add the first ingester to the ring
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")
	lifecyclerConfig1.HeartbeatPeriod = 100 * time.Millisecond
	lifecyclerConfig1.JoinAfter = 100 * time.Millisecond

	lifecycler1, err := NewLifecycler(lifecyclerConfig1, NewNoopFlushTransferer(), "ingester", IngesterRingKey, true, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, lifecycler1.HealthyInstancesCount())

	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler1))
	defer services.StopAndAwaitTerminated(ctx, lifecycler1) // nolint:errcheck

	// Assert the first ingester joined the ring
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		return lifecycler1.HealthyInstancesCount() == 1
	})

	// Add the second ingester to the ring
	lifecyclerConfig2 := testLifecyclerConfig(ringConfig, "ing2")
	lifecyclerConfig2.HeartbeatPeriod = 100 * time.Millisecond
	lifecyclerConfig2.JoinAfter = 100 * time.Millisecond

	lifecycler2, err := NewLifecycler(lifecyclerConfig2, NewNoopFlushTransferer(), "ingester", IngesterRingKey, true, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, lifecycler2.HealthyInstancesCount())

	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler2))
	defer services.StopAndAwaitTerminated(ctx, lifecycler2) // nolint:errcheck

	// Assert the second ingester joined the ring
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		return lifecycler2.HealthyInstancesCount() == 2
	})

	// Assert the first ingester count is updated
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		return lifecycler1.HealthyInstancesCount() == 2
	})
}

func TestLifecycler_ZonesCount(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())

	events := []struct {
		zone          string
		expectedZones int
	}{
		{"zone-a", 1},
		{"zone-b", 2},
		{"zone-a", 2},
		{"zone-c", 3},
	}

	for idx, event := range events {
		ctx := context.Background()

		// Register an ingester to the ring.
		cfg := testLifecyclerConfig(ringConfig, fmt.Sprintf("instance-%d", idx))
		cfg.HeartbeatPeriod = 100 * time.Millisecond
		cfg.JoinAfter = 100 * time.Millisecond
		cfg.Zone = event.zone

		lifecycler, err := NewLifecycler(cfg, NewNoopFlushTransferer(), "ingester", IngesterRingKey, true, nil)
		require.NoError(t, err)
		assert.Equal(t, 0, lifecycler.ZonesCount())

		require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
		defer services.StopAndAwaitTerminated(ctx, lifecycler) // nolint:errcheck

		// Wait until joined.
		test.Poll(t, time.Second, idx+1, func() interface{} {
			return lifecycler.HealthyInstancesCount()
		})

		assert.Equal(t, event.expectedZones, lifecycler.ZonesCount())
	}
}

func TestLifecycler_NilFlushTransferer(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())
	lifecyclerConfig := testLifecyclerConfig(ringConfig, "ing1")

	// Create a lifecycler with nil FlushTransferer to make sure it operates correctly
	lifecycler, err := NewLifecycler(lifecyclerConfig, nil, "ingester", IngesterRingKey, true, nil)
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

	lifecycler1, err := NewLifecycler(lifecyclerConfig1, nil, "service-1", "ring-1", true, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), lifecycler1))
	defer services.StopAndAwaitTerminated(context.Background(), lifecycler1) //nolint:errcheck

	lifecycler2, err := NewLifecycler(lifecyclerConfig2, nil, "service-2", "ring-2", true, nil)
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

func TestLifecycler_ShouldHandleInstanceAbruptlyRestarted(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	c := GetCodec()
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(c)

	r, err := New(ringConfig, "ingester", IngesterRingKey, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	// Add an 'ingester' with normalised tokens.
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")
	l1, err := NewLifecycler(lifecyclerConfig1, NewNoopFlushTransferer(), "ingester", IngesterRingKey, true, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), l1))

	// Check this ingester joined, is active, and has one token.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), IngesterRingKey)
		require.NoError(t, err)
		return checkNormalised(d, "ing1")
	})

	expectedTokens := l1.getTokens()
	expectedRegisteredAt := l1.getRegisteredAt()

	// Wait 1 second because the registered timestamp has second precision. Without waiting
	// we wouldn't have the guarantee the previous registered timestamp is preserved.
	time.Sleep(time.Second)

	// Add a second ingester with the same settings, so it will think it has restarted
	l2, err := NewLifecycler(lifecyclerConfig1, NewNoopFlushTransferer(), "ingester", IngesterRingKey, true, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), l2))

	// Check the new ingester picked up the same tokens and registered timestamp.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), IngesterRingKey)
		require.NoError(t, err)

		return checkNormalised(d, "ing1") &&
			expectedTokens.Equals(l2.getTokens()) &&
			expectedRegisteredAt.Unix() == l2.getRegisteredAt().Unix()
	})
}

func TestLifecycler_ShouldHandleDoNotUnregisterOnShutdown(t *testing.T) {
	ctx := context.Background()

	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())

	cfg := testLifecyclerConfig(ringConfig, "ring1")
	cfg.MinReadyDuration = 0
	cfg.JoinAfter = 0
	cfg.ObservePeriod = 0
	cfg.UnregisterOnShutdown = false

	var (
		initialTokens       Tokens
		initialRegisteredAt time.Time
	)

	// Start the lifecycler for the first time and the shutdown.
	{
		l, err := NewLifecycler(cfg, NewNoopFlushTransferer(), "ingester", IngesterRingKey, true, nil)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(ctx, l))

		// Wait until ACTIVE.
		test.Poll(t, time.Second, ACTIVE, func() interface{} {
			return l.GetState()
		})

		initialTokens = l.getTokens()
		initialRegisteredAt = l.getRegisteredAt()

		// Shutdown (will not unregister from ring).
		require.NoError(t, services.StopAndAwaitTerminated(ctx, l))
	}

	// Wait a second because the instance registration timestamp has second precision
	// and we need to wait until the next second to ensure the following assertion works as expected.
	time.Sleep(time.Second)

	// Start the lifecycler for the second time time.
	{
		l, err := NewLifecycler(cfg, NewNoopFlushTransferer(), "ingester", IngesterRingKey, true, nil)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(ctx, l))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, l))
		})

		// Wait until ACTIVE.
		test.Poll(t, time.Second, ACTIVE, func() interface{} {
			return l.GetState()
		})

		// Ensure instance data hasn't changed.
		assert.Equal(t, initialTokens, l.getTokens())
		assert.Equal(t, initialRegisteredAt.Unix(), l.getRegisteredAt().Unix())
	}
}

func TestLifecycler_CheckReady_TheInstanceIsNotInTheRingAtStartup(t *testing.T) {
	ctx := context.Background()

	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())

	cfg := testLifecyclerConfig(ringConfig, "ring1")
	cfg.MinReadyDuration = 0
	cfg.JoinAfter = time.Second
	cfg.ObservePeriod = 0

	l, err := NewLifecycler(cfg, NewNoopFlushTransferer(), "ingester", IngesterRingKey, true, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, l))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, l))
	})

	err = l.CheckReady(ctx)
	require.EqualError(t, err, "this instance owns no tokens")

	// We expect the instance to switch to ACTIVE after JoinAfter period and the readiness check to succeed.
	test.Poll(t, 2*time.Second, nil, func() interface{} {
		return l.CheckReady(ctx)
	})
}

func TestLifecycler_CheckReady_TheInstanceIsAlreadyInTheRingAtStartup(t *testing.T) {
	ctx := context.Background()

	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())

	// Start the lifecycler for the first time, join the ring immediately,
	// shutdown but do not unregister from ring. The instance will be left
	// in the ring with the LEAVING state.
	{
		cfg := testLifecyclerConfig(ringConfig, "ring1")
		cfg.MinReadyDuration = 0
		cfg.JoinAfter = 0
		cfg.ObservePeriod = 0
		cfg.UnregisterOnShutdown = false

		l, err := NewLifecycler(cfg, NewNoopFlushTransferer(), "ingester", IngesterRingKey, true, nil)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(ctx, l))

		// Wait until ready.
		test.Poll(t, time.Second, nil, func() interface{} {
			return l.CheckReady(ctx)
		})

		// Shutdown (will not unregister from ring).
		require.NoError(t, services.StopAndAwaitTerminated(ctx, l))
	}

	// Start the lifecycler for the second time time, join the ring after a short period.
	{
		cfg := testLifecyclerConfig(ringConfig, "ring1")
		cfg.MinReadyDuration = 0
		cfg.JoinAfter = time.Second
		cfg.ObservePeriod = 0

		l, err := NewLifecycler(cfg, NewNoopFlushTransferer(), "ingester", IngesterRingKey, true, nil)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(ctx, l))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, l))
		})

		// Wait until the init ring has been executed (previous tokens has been loaded from ring).
		test.Poll(t, 2*time.Second, true, func() interface{} {
			return len(l.getTokens()) > 0
		})

		err = l.CheckReady(ctx)
		require.EqualError(t, err, "this instance state is PENDING while ACTIVE is expected to be ready")

		// We expect the instance to switch to ACTIVE after JoinAfter period and the readiness check to succeed.
		test.Poll(t, 2*time.Second, nil, func() interface{} {
			return l.CheckReady(ctx)
		})
	}
}

func TestTokensOnDisk(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())

	r, err := New(ringConfig, "ingester", IngesterRingKey, nil)
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
	l1, err := NewLifecycler(lifecyclerConfig, NewNoopFlushTransferer(), "ingester", IngesterRingKey, true, nil)
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
	l2, err := NewLifecycler(lifecyclerConfig, NewNoopFlushTransferer(), "ingester", IngesterRingKey, true, nil)
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
	c := GetCodec()
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(c)

	r, err := New(ringConfig, "ingester", IngesterRingKey, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	cfg := testLifecyclerConfig(ringConfig, "ing1")
	cfg.NumTokens = 2
	cfg.MinReadyDuration = 1 * time.Nanosecond

	// Set state as LEAVING
	err = r.KVClient.CAS(context.Background(), IngesterRingKey, func(in interface{}) (interface{}, bool, error) {
		r := &Desc{
			Ingesters: map[string]InstanceDesc{
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

	l1, err := NewLifecycler(cfg, NewNoopFlushTransferer(), "ingester", IngesterRingKey, true, nil)
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

// JoinInJoiningState ensures that if the lifecycler starts up and the ring already has it in a JOINING state that it still is able to auto join
func TestJoinInJoiningState(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	c := GetCodec()
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(c)

	r, err := New(ringConfig, "ingester", IngesterRingKey, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	cfg := testLifecyclerConfig(ringConfig, "ing1")
	cfg.NumTokens = 2
	cfg.MinReadyDuration = 1 * time.Nanosecond
	instance1RegisteredAt := time.Now().Add(-1 * time.Hour)
	instance2RegisteredAt := time.Now().Add(-2 * time.Hour)

	// Set state as JOINING
	err = r.KVClient.CAS(context.Background(), IngesterRingKey, func(in interface{}) (interface{}, bool, error) {
		r := &Desc{
			Ingesters: map[string]InstanceDesc{
				"ing1": {
					State:               JOINING,
					Tokens:              []uint32{1, 4},
					RegisteredTimestamp: instance1RegisteredAt.Unix(),
				},
				"ing2": {
					Tokens:              []uint32{2, 3},
					RegisteredTimestamp: instance2RegisteredAt.Unix(),
				},
			},
		}

		return r, true, nil
	})
	require.NoError(t, err)

	l1, err := NewLifecycler(cfg, NewNoopFlushTransferer(), "ingester", IngesterRingKey, true, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), l1))

	// Check that the lifecycler was able to join after coming up in JOINING
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), IngesterRingKey)
		require.NoError(t, err)

		desc, ok := d.(*Desc)
		return ok &&
			len(desc.Ingesters) == 2 &&
			desc.Ingesters["ing1"].State == ACTIVE &&
			len(desc.Ingesters["ing1"].Tokens) == cfg.NumTokens &&
			len(desc.Ingesters["ing2"].Tokens) == 2 &&
			desc.Ingesters["ing1"].RegisteredTimestamp == instance1RegisteredAt.Unix() &&
			desc.Ingesters["ing2"].RegisteredTimestamp == instance2RegisteredAt.Unix()
	})
}

func TestRestoreOfZoneWhenOverwritten(t *testing.T) {
	// This test is simulating a case during upgrade of pre 1.0 cortex where
	// older ingesters do not have the zone field in their ring structs
	// so it gets removed. The current version of the lifecylcer should
	// write it back on update during its next heartbeat.

	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	codec := GetCodec()
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(codec)

	r, err := New(ringConfig, "ingester", IngesterRingKey, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	cfg := testLifecyclerConfig(ringConfig, "ing1")

	// Set ing1 to not have a zone
	err = r.KVClient.CAS(context.Background(), IngesterRingKey, func(in interface{}) (interface{}, bool, error) {
		r := &Desc{
			Ingesters: map[string]InstanceDesc{
				"ing1": {
					State:  ACTIVE,
					Addr:   "0.0.0.0",
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

	l1, err := NewLifecycler(cfg, NewNoopFlushTransferer(), "ingester", IngesterRingKey, true, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), l1))

	// Check that the lifecycler was able to reset the zone value to the expected setting
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), IngesterRingKey)
		require.NoError(t, err)
		desc, ok := d.(*Desc)
		return ok &&
			len(desc.Ingesters) == 2 &&
			desc.Ingesters["ing1"].Zone == l1.Zone &&
			desc.Ingesters["ing2"].Zone == ""

	})
}
