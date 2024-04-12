package ruler

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestGetReplicationSetForListRule(t *testing.T) {
	now := time.Now()

	g := ring.NewRandomTokenGenerator()

	tests := map[string]struct {
		ringInstances               map[string]ring.InstanceDesc
		ringHeartbeatTimeout        time.Duration
		ringReplicationFactor       int
		enableAZReplication         bool
		expectedErr                 error
		expectedSet                 []string
		expectedMaxError            int
		expectedMaxUnavailableZones int
		expectedFailedZones         map[string]struct{}
	}{
		"should return error on empty ring": {
			ringInstances:         nil,
			ringHeartbeatTimeout:  time.Minute,
			ringReplicationFactor: 1,
			expectedErr:           ring.ErrEmptyRing,
		},
		"should succeed on all healthy instances and RF=1": {
			ringInstances: map[string]ring.InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", State: ring.ACTIVE, Timestamp: now.Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-1", "", 128, true)},
				"instance-2": {Addr: "127.0.0.2", State: ring.ACTIVE, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-2", "", 128, true)},
				"instance-3": {Addr: "127.0.0.3", State: ring.ACTIVE, Timestamp: now.Add(-20 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-3", "", 128, true)},
			},
			ringHeartbeatTimeout:  time.Minute,
			ringReplicationFactor: 1,
			expectedSet:           []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
		},
		"should fail on 1 unhealthy instance and RF=1": {
			ringInstances: map[string]ring.InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", State: ring.ACTIVE, Timestamp: now.Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-1", "", 128, true)},
				"instance-2": {Addr: "127.0.0.2", State: ring.ACTIVE, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-2", "", 128, true)},
				"instance-3": {Addr: "127.0.0.3", State: ring.ACTIVE, Timestamp: now.Add(-20 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-3", "", 128, true)},
				"instance-4": {Addr: "127.0.0.4", State: ring.ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-4", "", 128, true)},
			},
			ringHeartbeatTimeout:  time.Minute,
			ringReplicationFactor: 1,
			expectedErr:           ring.ErrTooManyUnhealthyInstances,
		},
		"should succeed on 1 unhealthy instances and RF=3": {
			ringInstances: map[string]ring.InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", State: ring.ACTIVE, Timestamp: now.Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-1", "", 128, true)},
				"instance-2": {Addr: "127.0.0.2", State: ring.ACTIVE, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-2", "", 128, true)},
				"instance-3": {Addr: "127.0.0.3", State: ring.ACTIVE, Timestamp: now.Add(-20 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-3", "", 128, true)},
				"instance-4": {Addr: "127.0.0.4", State: ring.ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-4", "", 128, true)},
			},
			ringHeartbeatTimeout:  time.Minute,
			ringReplicationFactor: 3,
			expectedSet:           []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
			expectedMaxError:      1,
		},
		"should succeed on 2 unhealthy instances and RF=3": {
			ringInstances: map[string]ring.InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", State: ring.ACTIVE, Timestamp: now.Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-1", "", 128, true)},
				"instance-2": {Addr: "127.0.0.2", State: ring.ACTIVE, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-2", "", 128, true)},
				"instance-3": {Addr: "127.0.0.3", State: ring.ACTIVE, Timestamp: now.Add(-20 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-3", "", 128, true)},
				"instance-4": {Addr: "127.0.0.4", State: ring.ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-4", "", 128, true)},
				"instance-5": {Addr: "127.0.0.5", State: ring.ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-5", "", 128, true)},
			},
			ringHeartbeatTimeout:  time.Minute,
			ringReplicationFactor: 3,
			expectedSet:           []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
			expectedMaxError:      0,
		},
		"should fail on 3 unhealthy instances and RF=3": {
			ringInstances: map[string]ring.InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", State: ring.ACTIVE, Timestamp: now.Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-1", "", 128, true)},
				"instance-2": {Addr: "127.0.0.2", State: ring.ACTIVE, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-2", "", 128, true)},
				"instance-3": {Addr: "127.0.0.3", State: ring.ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-3", "", 128, true)},
				"instance-4": {Addr: "127.0.0.4", State: ring.ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-4", "", 128, true)},
				"instance-5": {Addr: "127.0.0.5", State: ring.ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-5", "", 128, true)},
			},
			ringHeartbeatTimeout:  time.Minute,
			ringReplicationFactor: 3,
			expectedErr:           ring.ErrTooManyUnhealthyInstances,
		},
		"should succeed on 0 unhealthy instances and RF=3 zone replication enabled": {
			ringInstances: map[string]ring.InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", State: ring.ACTIVE, Timestamp: now.Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-1", "z1", 128, true), Zone: "z1"},
				"instance-2": {Addr: "127.0.0.2", State: ring.ACTIVE, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-2", "z2", 128, true), Zone: "z2"},
				"instance-3": {Addr: "127.0.0.3", State: ring.ACTIVE, Timestamp: now.Add(-20 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-3", "z3", 128, true), Zone: "z3"},
				"instance-4": {Addr: "127.0.0.4", State: ring.ACTIVE, Timestamp: now.Add(-30 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-4", "z1", 128, true), Zone: "z1"},
			},
			ringHeartbeatTimeout:        time.Minute,
			ringReplicationFactor:       3,
			expectedSet:                 []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"},
			enableAZReplication:         true,
			expectedFailedZones:         make(map[string]struct{}),
			expectedMaxUnavailableZones: 2,
		},
		"should succeed on 3 unhealthy instances in 2 zones and RF=3 zone replication enabled": {
			ringInstances: map[string]ring.InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", State: ring.PENDING, Timestamp: now.Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-1", "z1", 128, true), Zone: "z1"},
				"instance-2": {Addr: "127.0.0.2", State: ring.ACTIVE, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-2", "z2", 128, true), Zone: "z2"},
				"instance-3": {Addr: "127.0.0.3", State: ring.ACTIVE, Timestamp: now.Add(-20 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-3", "z3", 128, true), Zone: "z3"},
				"instance-4": {Addr: "127.0.0.4", State: ring.ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-4", "z1", 128, true), Zone: "z1"},
				"instance-5": {Addr: "127.0.0.5", State: ring.PENDING, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-5", "z2", 128, true), Zone: "z2"},
			},
			ringHeartbeatTimeout:  time.Minute,
			ringReplicationFactor: 3,
			expectedSet:           []string{"127.0.0.2", "127.0.0.3"},
			enableAZReplication:   true,
			expectedFailedZones: map[string]struct{}{
				"z1": {},
				"z2": {},
			},
			expectedMaxUnavailableZones: 2,
		},
		"should succeed on 1 unhealthy instances in 1 zone and RF=3 zone replication enabled": {
			ringInstances: map[string]ring.InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", State: ring.ACTIVE, Timestamp: now.Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-1", "z1", 128, true), Zone: "z1"},
				"instance-2": {Addr: "127.0.0.2", State: ring.ACTIVE, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-2", "z2", 128, true), Zone: "z2"},
				"instance-3": {Addr: "127.0.0.3", State: ring.ACTIVE, Timestamp: now.Add(-20 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-3", "z3", 128, true), Zone: "z3"},
				"instance-4": {Addr: "127.0.0.4", State: ring.PENDING, Timestamp: now.Add(-30 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-4", "z1", 128, true), Zone: "z1"},
			},
			ringHeartbeatTimeout:  time.Minute,
			ringReplicationFactor: 3,
			expectedSet:           []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
			enableAZReplication:   true,
			expectedFailedZones: map[string]struct{}{
				"z1": {},
			},
			expectedMaxUnavailableZones: 2,
		},
		"should fail on 3 unhealthy instances in 3 zonez and RF=3 zone replication enabled": {
			ringInstances: map[string]ring.InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", State: ring.ACTIVE, Timestamp: now.Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-1", "z1", 128, true), Zone: "z1"},
				"instance-2": {Addr: "127.0.0.2", State: ring.ACTIVE, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-2", "z2", 128, true), Zone: "z2"},
				"instance-3": {Addr: "127.0.0.3", State: ring.ACTIVE, Timestamp: now.Add(-20 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-3", "z3", 128, true), Zone: "z3"},
				"instance-4": {Addr: "127.0.0.4", State: ring.PENDING, Timestamp: now.Add(-30 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-4", "z1", 128, true), Zone: "z1"},
				"instance-5": {Addr: "127.0.0.5", State: ring.ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-5", "z2", 128, true), Zone: "z2"},
				"instance-6": {Addr: "127.0.0.6", State: ring.ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-6", "z3", 128, true), Zone: "z3"},
			},
			ringHeartbeatTimeout:  time.Minute,
			ringReplicationFactor: 3,
			enableAZReplication:   true,
			expectedErr:           ring.ErrTooManyUnhealthyInstances,
			expectedFailedZones: map[string]struct{}{
				"z1": {},
				"z2": {},
				"z3": {},
			},
		},
		"should fail on 2 unhealthy instances in 2 zones and RF=2 zone replication enabled": {
			ringInstances: map[string]ring.InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", State: ring.ACTIVE, Timestamp: now.Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-1", "z1", 128, true), Zone: "z1"},
				"instance-2": {Addr: "127.0.0.2", State: ring.ACTIVE, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-2", "z2", 128, true), Zone: "z2"},
				"instance-3": {Addr: "127.0.0.3", State: ring.ACTIVE, Timestamp: now.Add(-20 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-3", "z3", 128, true), Zone: "z3"},
				"instance-4": {Addr: "127.0.0.4", State: ring.PENDING, Timestamp: now.Add(-30 * time.Second).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-4", "z1", 128, true), Zone: "z1"},
				"instance-5": {Addr: "127.0.0.5", State: ring.ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: g.GenerateTokens(ring.NewDesc(), "instance-5", "z2", 128, true), Zone: "z2"},
			},
			ringHeartbeatTimeout:  time.Minute,
			ringReplicationFactor: 2,
			enableAZReplication:   true,
			expectedErr:           ring.ErrTooManyUnhealthyInstances,
			expectedFailedZones: map[string]struct{}{
				"z1": {},
				"z2": {},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			store := newMockRuleStore(nil, nil)
			cfg := Config{
				EnableSharding:   true,
				ShardingStrategy: util.ShardingStrategyDefault,
				Ring: RingConfig{
					InstanceID:   "instance-1",
					InstanceAddr: "127.0.0.1",
					InstancePort: 9999,
					KVStore: kv.Config{
						Mock: kvStore,
					},
					HeartbeatTimeout:     1 * time.Minute,
					ReplicationFactor:    testData.ringReplicationFactor,
					ZoneAwarenessEnabled: testData.enableAZReplication,
				},
				FlushCheckPeriod: 0,
			}

			r, _ := buildRuler(t, cfg, nil, store, nil)
			r.limits = ruleLimits{evalDelay: 0}

			rulerRing := r.ring
			// We start ruler's ring, but nothing else (not even lifecycler).
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), rulerRing))
			t.Cleanup(rulerRing.StopAsync)

			err := kvStore.CAS(context.Background(), ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
				d, _ := in.(*ring.Desc)
				if d == nil {
					d = ring.NewDesc()
				}
				d.Ingesters = testData.ringInstances
				return d, true, nil
			})
			require.NoError(t, err)
			// Wait a bit to make sure ruler's ring is updated.
			time.Sleep(100 * time.Millisecond)

			set, failedZones, err := GetReplicationSetForListRule(rulerRing, &cfg.Ring)
			require.Equal(t, testData.expectedErr, err)
			assert.ElementsMatch(t, testData.expectedSet, set.GetAddresses())
			require.Equal(t, testData.expectedMaxError, set.MaxErrors)
			require.Equal(t, testData.expectedMaxUnavailableZones, set.MaxUnavailableZones)
			if testData.enableAZReplication {
				require.Equal(t, len(testData.expectedFailedZones), len(failedZones))
				require.EqualValues(t, testData.expectedFailedZones, failedZones)
			}
		})
	}
}
