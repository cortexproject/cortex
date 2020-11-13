package ring

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

const (
	numTokens = 512
)

func BenchmarkBatch10x100(b *testing.B) {
	benchmarkBatch(b, 10, 100)
}

func BenchmarkBatch100x100(b *testing.B) {
	benchmarkBatch(b, 100, 100)
}

func BenchmarkBatch100x1000(b *testing.B) {
	benchmarkBatch(b, 100, 1000)
}

func benchmarkBatch(b *testing.B, numIngester, numKeys int) {
	// Make a random ring with N ingesters, and M tokens per ingests
	desc := NewDesc()
	takenTokens := []uint32{}
	for i := 0; i < numIngester; i++ {
		tokens := GenerateTokens(numTokens, takenTokens)
		takenTokens = append(takenTokens, tokens...)
		desc.AddIngester(fmt.Sprintf("%d", i), fmt.Sprintf("ingester%d", i), strconv.Itoa(i), tokens, ACTIVE, time.Now())
	}

	cfg := Config{}
	flagext.DefaultValues(&cfg)
	r := Ring{
		cfg:      cfg,
		ringDesc: desc,
		strategy: &DefaultReplicationStrategy{},
	}

	ctx := context.Background()
	callback := func(IngesterDesc, []int) error {
		return nil
	}
	cleanup := func() {
	}
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	keys := make([]uint32, numKeys)
	// Generate a batch of N random keys, and look them up
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		generateKeys(rnd, numKeys, keys)
		err := DoBatch(ctx, &r, keys, callback, cleanup)
		require.NoError(b, err)
	}
}

func generateKeys(r *rand.Rand, numTokens int, dest []uint32) {
	for i := 0; i < numTokens; i++ {
		dest[i] = r.Uint32()
	}
}

func TestDoBatchZeroIngesters(t *testing.T) {
	ctx := context.Background()
	numKeys := 10
	keys := make([]uint32, numKeys)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	generateKeys(rnd, numKeys, keys)
	callback := func(IngesterDesc, []int) error {
		return nil
	}
	cleanup := func() {
	}
	desc := NewDesc()
	r := Ring{
		cfg:      Config{},
		ringDesc: desc,
		strategy: &DefaultReplicationStrategy{},
	}
	require.Error(t, DoBatch(ctx, &r, keys, callback, cleanup))
}

func TestAddIngester(t *testing.T) {
	r := NewDesc()

	const ingName = "ing1"

	now := time.Now()
	ing1Tokens := GenerateTokens(128, nil)

	r.AddIngester(ingName, "addr", "1", ing1Tokens, ACTIVE, now)

	assert.Equal(t, "addr", r.Ingesters[ingName].Addr)
	assert.Equal(t, ing1Tokens, r.Ingesters[ingName].Tokens)
	assert.InDelta(t, time.Now().Unix(), r.Ingesters[ingName].Timestamp, 2)
	assert.Equal(t, now.Unix(), r.Ingesters[ingName].RegisteredTimestamp)
}

func TestAddIngesterReplacesExistingTokens(t *testing.T) {
	r := NewDesc()

	const ing1Name = "ing1"

	// old tokens will be replaced
	r.Ingesters[ing1Name] = IngesterDesc{
		Tokens: []uint32{11111, 22222, 33333},
	}

	newTokens := GenerateTokens(128, nil)

	r.AddIngester(ing1Name, "addr", "1", newTokens, ACTIVE, time.Now())

	require.Equal(t, newTokens, r.Ingesters[ing1Name].Tokens)
}

func TestRing_Get_ZoneAwareness(t *testing.T) {
	// Number of tests to run.
	const testCount = 10000

	tests := map[string]struct {
		numIngesters         int
		numZones             int
		replicationFactor    int
		zoneAwarenessEnabled bool
		expectedErr          string
		expectedIngesters    int
	}{
		"should succeed if there are enough ingesters per zone on RF = 3": {
			numIngesters:         16,
			numZones:             3,
			replicationFactor:    3,
			zoneAwarenessEnabled: true,
			expectedIngesters:    3,
		},
		"should fail if there are ingesters in 1 zone only on RF = 3": {
			numIngesters:         16,
			numZones:             1,
			replicationFactor:    3,
			zoneAwarenessEnabled: true,
			expectedErr:          "at least 2 live replicas required across different availability zones, could only find 1",
		},
		"should succeed if there are ingesters in 2 zones on RF = 3": {
			numIngesters:         16,
			numZones:             2,
			replicationFactor:    3,
			zoneAwarenessEnabled: true,
			expectedIngesters:    2,
		},
		"should succeed if there are ingesters in 1 zone only on RF = 3 but zone-awareness is disabled": {
			numIngesters:         16,
			numZones:             1,
			replicationFactor:    3,
			zoneAwarenessEnabled: false,
			expectedIngesters:    3,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Add ingesters to the ring.
			r := NewDesc()
			var prevTokens []uint32
			for i := 0; i < testData.numIngesters; i++ {
				name := fmt.Sprintf("ing%v", i)
				ingTokens := GenerateTokens(128, prevTokens)

				r.AddIngester(name, fmt.Sprintf("127.0.0.%d", i), fmt.Sprintf("zone-%v", i%testData.numZones), ingTokens, ACTIVE, time.Now())

				prevTokens = append(prevTokens, ingTokens...)
			}

			// Create a ring with the ingesters
			ring := Ring{
				cfg: Config{
					HeartbeatTimeout:     time.Hour,
					ReplicationFactor:    testData.replicationFactor,
					ZoneAwarenessEnabled: testData.zoneAwarenessEnabled,
				},
				ringDesc:         r,
				ringTokens:       r.getTokens(),
				ringTokensByZone: r.getTokensByZone(),
				ringZones:        getZones(r.getTokensByZone()),
				strategy:         &DefaultReplicationStrategy{},
			}

			ingesters := make([]IngesterDesc, 0, len(r.GetIngesters()))
			for _, v := range r.GetIngesters() {
				ingesters = append(ingesters, v)
			}

			// Use the GenerateTokens to get an array of random uint32 values.
			testValues := GenerateTokens(testCount, nil)

			var set ReplicationSet
			var err error
			for i := 0; i < testCount; i++ {
				set, err = ring.Get(testValues[i], Write, ingesters)
				if testData.expectedErr != "" {
					require.EqualError(t, err, testData.expectedErr)
				} else {
					require.NoError(t, err)
				}

				// Skip the rest of the assertions if we were expecting an error.
				if testData.expectedErr != "" {
					continue
				}

				// Check that we have the expected number of ingesters for replication.
				assert.Equal(t, testData.expectedIngesters, len(set.Ingesters))

				// Ensure all ingesters are in a different zone (only if zone-awareness is enabled).
				if testData.zoneAwarenessEnabled {
					zones := make(map[string]struct{})
					for i := 0; i < len(set.Ingesters); i++ {
						if _, ok := zones[set.Ingesters[i].Zone]; ok {
							t.Fatal("found multiple ingesters in the same zone")
						}
						zones[set.Ingesters[i].Zone] = struct{}{}
					}
				}
			}
		})
	}
}

func TestRing_GetAllHealthy(t *testing.T) {
	const heartbeatTimeout = time.Minute
	now := time.Now()

	tests := map[string]struct {
		ringInstances           map[string]IngesterDesc
		expectedErrForRead      error
		expectedSetForRead      []string
		expectedErrForWrite     error
		expectedSetForWrite     []string
		expectedErrForReporting error
		expectedSetForReporting []string
	}{
		"should return error on empty ring": {
			ringInstances:           nil,
			expectedErrForRead:      ErrEmptyRing,
			expectedErrForWrite:     ErrEmptyRing,
			expectedErrForReporting: ErrEmptyRing,
		},
		"should return all healthy instances for the given operation": {
			ringInstances: map[string]IngesterDesc{
				"instance-1": {Addr: "127.0.0.1", State: ACTIVE, Timestamp: now.Unix()},
				"instance-2": {Addr: "127.0.0.2", State: PENDING, Timestamp: now.Add(-10 * time.Second).Unix()},
				"instance-3": {Addr: "127.0.0.3", State: JOINING, Timestamp: now.Add(-20 * time.Second).Unix()},
				"instance-4": {Addr: "127.0.0.4", State: LEAVING, Timestamp: now.Add(-30 * time.Second).Unix()},
				"instance-5": {Addr: "127.0.0.5", State: ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix()},
			},
			expectedSetForRead:      []string{"127.0.0.1", "127.0.0.2", "127.0.0.4"},
			expectedSetForWrite:     []string{"127.0.0.1"},
			expectedSetForReporting: []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Init the ring.
			ringDesc := &Desc{Ingesters: testData.ringInstances}
			for id, instance := range ringDesc.Ingesters {
				ringDesc.Ingesters[id] = instance
			}

			ring := Ring{
				cfg:              Config{HeartbeatTimeout: heartbeatTimeout},
				ringDesc:         ringDesc,
				ringTokens:       ringDesc.getTokens(),
				ringTokensByZone: ringDesc.getTokensByZone(),
				ringZones:        getZones(ringDesc.getTokensByZone()),
				strategy:         &DefaultReplicationStrategy{},
			}

			set, err := ring.GetAllHealthy(Read)
			require.Equal(t, testData.expectedErrForRead, err)
			assert.ElementsMatch(t, testData.expectedSetForRead, set.GetAddresses())

			set, err = ring.GetAllHealthy(Write)
			require.Equal(t, testData.expectedErrForWrite, err)
			assert.ElementsMatch(t, testData.expectedSetForWrite, set.GetAddresses())

			set, err = ring.GetAllHealthy(Reporting)
			require.Equal(t, testData.expectedErrForReporting, err)
			assert.ElementsMatch(t, testData.expectedSetForReporting, set.GetAddresses())
		})
	}
}

func TestRing_GetReplicationSetForOperation(t *testing.T) {
	const heartbeatTimeout = time.Minute
	now := time.Now()

	tests := map[string]struct {
		ringInstances           map[string]IngesterDesc
		ringReplicationFactor   int
		expectedErrForRead      error
		expectedSetForRead      []string
		expectedErrForWrite     error
		expectedSetForWrite     []string
		expectedErrForReporting error
		expectedSetForReporting []string
	}{
		"should return error on empty ring": {
			ringInstances:           nil,
			ringReplicationFactor:   1,
			expectedErrForRead:      ErrEmptyRing,
			expectedErrForWrite:     ErrEmptyRing,
			expectedErrForReporting: ErrEmptyRing,
		},
		"should succeed on all healthy instances and RF=1": {
			ringInstances: map[string]IngesterDesc{
				"instance-1": {Addr: "127.0.0.1", State: ACTIVE, Timestamp: now.Unix(), Tokens: GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", State: ACTIVE, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", State: ACTIVE, Timestamp: now.Add(-20 * time.Second).Unix(), Tokens: GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", State: ACTIVE, Timestamp: now.Add(-30 * time.Second).Unix(), Tokens: GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", State: ACTIVE, Timestamp: now.Add(-40 * time.Second).Unix(), Tokens: GenerateTokens(128, nil)},
			},
			ringReplicationFactor:   1,
			expectedSetForRead:      []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"},
			expectedSetForWrite:     []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"},
			expectedSetForReporting: []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"},
		},
		"should fail on 1 unhealthy instance and RF=1": {
			ringInstances: map[string]IngesterDesc{
				"instance-1": {Addr: "127.0.0.1", State: ACTIVE, Timestamp: now.Unix(), Tokens: GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", State: ACTIVE, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", State: ACTIVE, Timestamp: now.Add(-20 * time.Second).Unix(), Tokens: GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", State: ACTIVE, Timestamp: now.Add(-30 * time.Second).Unix(), Tokens: GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", State: ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: GenerateTokens(128, nil)},
			},
			ringReplicationFactor:   1,
			expectedErrForRead:      ErrTooManyFailedIngesters,
			expectedErrForWrite:     ErrTooManyFailedIngesters,
			expectedErrForReporting: ErrTooManyFailedIngesters,
		},
		"should succeed on 1 unhealthy instances and RF=3": {
			ringInstances: map[string]IngesterDesc{
				"instance-1": {Addr: "127.0.0.1", State: ACTIVE, Timestamp: now.Unix(), Tokens: GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", State: ACTIVE, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", State: ACTIVE, Timestamp: now.Add(-20 * time.Second).Unix(), Tokens: GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", State: ACTIVE, Timestamp: now.Add(-30 * time.Second).Unix(), Tokens: GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", State: ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: GenerateTokens(128, nil)},
			},
			ringReplicationFactor:   3,
			expectedSetForRead:      []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"},
			expectedSetForWrite:     []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"},
			expectedSetForReporting: []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"},
		},
		"should fail on 2 unhealthy instances and RF=3": {
			ringInstances: map[string]IngesterDesc{
				"instance-1": {Addr: "127.0.0.1", State: ACTIVE, Timestamp: now.Unix(), Tokens: GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", State: ACTIVE, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", State: ACTIVE, Timestamp: now.Add(-20 * time.Second).Unix(), Tokens: GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", State: ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", State: ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: GenerateTokens(128, nil)},
			},
			ringReplicationFactor:   3,
			expectedErrForRead:      ErrTooManyFailedIngesters,
			expectedErrForWrite:     ErrTooManyFailedIngesters,
			expectedErrForReporting: ErrTooManyFailedIngesters,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Init the ring.
			ringDesc := &Desc{Ingesters: testData.ringInstances}
			for id, instance := range ringDesc.Ingesters {
				ringDesc.Ingesters[id] = instance
			}

			ring := Ring{
				cfg: Config{
					HeartbeatTimeout:  heartbeatTimeout,
					ReplicationFactor: testData.ringReplicationFactor,
				},
				ringDesc:         ringDesc,
				ringTokens:       ringDesc.getTokens(),
				ringTokensByZone: ringDesc.getTokensByZone(),
				ringZones:        getZones(ringDesc.getTokensByZone()),
				strategy:         &DefaultReplicationStrategy{},
			}

			set, err := ring.GetReplicationSetForOperation(Read)
			require.Equal(t, testData.expectedErrForRead, err)
			assert.ElementsMatch(t, testData.expectedSetForRead, set.GetAddresses())

			set, err = ring.GetReplicationSetForOperation(Write)
			require.Equal(t, testData.expectedErrForWrite, err)
			assert.ElementsMatch(t, testData.expectedSetForWrite, set.GetAddresses())

			set, err = ring.GetReplicationSetForOperation(Reporting)
			require.Equal(t, testData.expectedErrForReporting, err)
			assert.ElementsMatch(t, testData.expectedSetForReporting, set.GetAddresses())
		})
	}
}

func TestRing_ShuffleShard(t *testing.T) {
	tests := map[string]struct {
		ringInstances        map[string]IngesterDesc
		shardSize            int
		zoneAwarenessEnabled bool
		expectedSize         int
		expectedDistribution []int
	}{
		"empty ring": {
			ringInstances:        nil,
			shardSize:            2,
			zoneAwarenessEnabled: true,
			expectedSize:         0,
			expectedDistribution: []int{},
		},
		"single zone, shard size > num instances": {
			ringInstances: map[string]IngesterDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: GenerateTokens(128, nil)},
			},
			shardSize:            3,
			zoneAwarenessEnabled: true,
			expectedSize:         2,
			expectedDistribution: []int{2},
		},
		"single zone, shard size < num instances": {
			ringInstances: map[string]IngesterDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-a", Tokens: GenerateTokens(128, nil)},
			},
			shardSize:            2,
			zoneAwarenessEnabled: true,
			expectedSize:         2,
			expectedDistribution: []int{2},
		},
		"multiple zones, shard size < num zones": {
			ringInstances: map[string]IngesterDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-b", Tokens: GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-c", Tokens: GenerateTokens(128, nil)},
			},
			shardSize:            2,
			zoneAwarenessEnabled: true,
			expectedSize:         3,
			expectedDistribution: []int{1, 1, 1},
		},
		"multiple zones, shard size divisible by num zones": {
			ringInstances: map[string]IngesterDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-b", Tokens: GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", Tokens: GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-c", Tokens: GenerateTokens(128, nil)},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-c", Tokens: GenerateTokens(128, nil)},
			},
			shardSize:            3,
			zoneAwarenessEnabled: true,
			expectedSize:         3,
			expectedDistribution: []int{1, 1, 1},
		},
		"multiple zones, shard size NOT divisible by num zones": {
			ringInstances: map[string]IngesterDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-b", Tokens: GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", Tokens: GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-c", Tokens: GenerateTokens(128, nil)},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-c", Tokens: GenerateTokens(128, nil)},
			},
			shardSize:            4,
			zoneAwarenessEnabled: true,
			expectedSize:         6,
			expectedDistribution: []int{2, 2, 2},
		},
		"multiple zones, shard size NOT divisible by num zones, but zone awareness is disabled": {
			ringInstances: map[string]IngesterDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-b", Tokens: GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", Tokens: GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-c", Tokens: GenerateTokens(128, nil)},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-c", Tokens: GenerateTokens(128, nil)},
			},
			shardSize:            4,
			zoneAwarenessEnabled: false,
			expectedSize:         4,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Init the ring.
			ringDesc := &Desc{Ingesters: testData.ringInstances}
			for id, instance := range ringDesc.Ingesters {
				instance.Timestamp = time.Now().Unix()
				instance.State = ACTIVE
				ringDesc.Ingesters[id] = instance
			}

			ring := Ring{
				cfg: Config{
					HeartbeatTimeout:     time.Hour,
					ZoneAwarenessEnabled: testData.zoneAwarenessEnabled,
				},
				ringDesc:         ringDesc,
				ringTokens:       ringDesc.getTokens(),
				ringTokensByZone: ringDesc.getTokensByZone(),
				ringZones:        getZones(ringDesc.getTokensByZone()),
				strategy:         &DefaultReplicationStrategy{},
			}

			shardRing := ring.ShuffleShard("tenant-id", testData.shardSize)
			assert.Equal(t, testData.expectedSize, shardRing.IngesterCount())

			// Compute the actual distribution of instances across zones.
			if testData.zoneAwarenessEnabled {
				var actualDistribution []int

				if shardRing.IngesterCount() > 0 {
					all, err := shardRing.GetAllHealthy(Read)
					require.NoError(t, err)

					countByZone := map[string]int{}
					for _, instance := range all.Ingesters {
						countByZone[instance.Zone]++
					}

					for _, count := range countByZone {
						actualDistribution = append(actualDistribution, count)
					}
				}

				assert.ElementsMatch(t, testData.expectedDistribution, actualDistribution)
			}
		})
	}
}

// This test asserts on shard stability across multiple invocations and given the same input ring.
func TestRing_ShuffleShard_Stability(t *testing.T) {
	var (
		numTenants     = 100
		numInstances   = 50
		numZones       = 3
		numInvocations = 10
		shardSizes     = []int{3, 6, 9, 12, 15}
	)

	// Initialise the ring.
	ringDesc := &Desc{Ingesters: generateRingInstances(numInstances, numZones)}
	ring := Ring{
		cfg: Config{
			HeartbeatTimeout:     time.Hour,
			ZoneAwarenessEnabled: true,
		},
		ringDesc:         ringDesc,
		ringTokens:       ringDesc.getTokens(),
		ringTokensByZone: ringDesc.getTokensByZone(),
		ringZones:        getZones(ringDesc.getTokensByZone()),
		strategy:         &DefaultReplicationStrategy{},
	}

	for i := 1; i <= numTenants; i++ {
		tenantID := fmt.Sprintf("%d", i)

		for _, size := range shardSizes {
			r := ring.ShuffleShard(tenantID, size)
			expected, err := r.GetAllHealthy(Read)
			require.NoError(t, err)

			// Assert that multiple invocations generate the same exact shard.
			for n := 0; n < numInvocations; n++ {
				r := ring.ShuffleShard(tenantID, size)
				actual, err := r.GetAllHealthy(Read)
				require.NoError(t, err)
				assert.ElementsMatch(t, expected.Ingesters, actual.Ingesters)
			}
		}
	}
}

func TestRing_ShuffleShard_Shuffling(t *testing.T) {
	var (
		numTenants   = 1000
		numInstances = 90
		numZones     = 3
		shardSize    = 3

		// This is the expected theoretical distribution of matching instances
		// between different shards, given the settings above. It has been computed
		// using this spreadsheet:
		// https://docs.google.com/spreadsheets/d/1FXbiWTXi6bdERtamH-IfmpgFq1fNL4GP_KX_yJvbRi4/edit
		theoreticalMatchings = map[int]float64{
			0: 90.2239,
			1: 9.55312,
			2: 0.22217,
			3: 0.00085,
		}
	)

	// Initialise the ring instances. To have stable tests we generate tokens using a linear
	// distribution. Tokens within the same zone are evenly distributed too.
	instances := make(map[string]IngesterDesc, numInstances)
	for i := 0; i < numInstances; i++ {
		id := fmt.Sprintf("instance-%d", i)
		instances[id] = IngesterDesc{
			Addr:                fmt.Sprintf("127.0.0.%d", i),
			Timestamp:           time.Now().Unix(),
			RegisteredTimestamp: time.Now().Unix(),
			State:               ACTIVE,
			Tokens:              generateTokensLinear(i, numInstances, 128),
			Zone:                fmt.Sprintf("zone-%d", i%numZones),
		}
	}

	// Initialise the ring.
	ringDesc := &Desc{Ingesters: instances}
	ring := Ring{
		cfg: Config{
			HeartbeatTimeout:     time.Hour,
			ZoneAwarenessEnabled: true,
		},
		ringDesc:         ringDesc,
		ringTokens:       ringDesc.getTokens(),
		ringTokensByZone: ringDesc.getTokensByZone(),
		ringZones:        getZones(ringDesc.getTokensByZone()),
		strategy:         &DefaultReplicationStrategy{},
	}

	// Compute the shard for each tenant.
	shards := map[string][]string{}

	for i := 1; i <= numTenants; i++ {
		tenantID := fmt.Sprintf("%d", i)
		r := ring.ShuffleShard(tenantID, shardSize)
		set, err := r.GetAllHealthy(Read)
		require.NoError(t, err)

		instances := make([]string, 0, len(set.Ingesters))
		for _, instance := range set.Ingesters {
			instances = append(instances, instance.Addr)
		}

		shards[tenantID] = instances
	}

	// Compute the distribution of matching instances between every combination of shards.
	// The shards comparison is not optimized, but it's fine for a test.
	distribution := map[int]int{}

	for currID, currShard := range shards {
		for otherID, otherShard := range shards {
			if currID == otherID {
				continue
			}

			numMatching := 0
			for _, c := range currShard {
				if util.StringsContain(otherShard, c) {
					numMatching++
				}
			}

			distribution[numMatching]++
		}
	}

	maxCombinations := int(math.Pow(float64(numTenants), 2)) - numTenants
	for numMatching, probability := range theoreticalMatchings {
		// We allow a max deviance of 10% compared to the theoretical probability,
		// clamping it between 1% and 0.2% boundaries.
		maxDeviance := math.Min(1, math.Max(0.2, probability*0.1))

		actual := (float64(distribution[numMatching]) / float64(maxCombinations)) * 100
		assert.InDelta(t, probability, actual, maxDeviance, "numMatching: %d", numMatching)
	}
}

func TestRing_ShuffleShard_Consistency(t *testing.T) {
	type change string

	type scenario struct {
		name         string
		numInstances int
		numZones     int
		shardSize    int
		ringChange   change
	}

	const (
		numTenants = 100
		add        = change("add-instance")
		remove     = change("remove-instance")
	)

	// Generate all test scenarios.
	var scenarios []scenario
	for _, numInstances := range []int{20, 30, 40, 50} {
		for _, shardSize := range []int{3, 6, 9, 12, 15} {
			for _, c := range []change{add, remove} {
				scenarios = append(scenarios, scenario{
					name:         fmt.Sprintf("instances = %d, shard size = %d, ring operation = %s", numInstances, shardSize, c),
					numInstances: numInstances,
					numZones:     3,
					shardSize:    shardSize,
					ringChange:   c,
				})
			}
		}
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			// Initialise the ring.
			ringDesc := &Desc{Ingesters: generateRingInstances(s.numInstances, s.numZones)}
			ring := Ring{
				cfg: Config{
					HeartbeatTimeout:     time.Hour,
					ZoneAwarenessEnabled: true,
				},
				ringDesc:         ringDesc,
				ringTokens:       ringDesc.getTokens(),
				ringTokensByZone: ringDesc.getTokensByZone(),
				ringZones:        getZones(ringDesc.getTokensByZone()),
				strategy:         &DefaultReplicationStrategy{},
			}

			// Compute the initial shard for each tenant.
			initial := map[int]ReplicationSet{}
			for id := 0; id < numTenants; id++ {
				set, err := ring.ShuffleShard(fmt.Sprintf("%d", id), s.shardSize).GetAllHealthy(Read)
				require.NoError(t, err)
				initial[id] = set
			}

			// Update the ring.
			switch s.ringChange {
			case add:
				newID, newDesc := generateRingInstance(s.numInstances+1, 0)
				ringDesc.Ingesters[newID] = newDesc
			case remove:
				// Remove the first one.
				for id := range ringDesc.Ingesters {
					delete(ringDesc.Ingesters, id)
					break
				}
			}

			ring.ringTokens = ringDesc.getTokens()
			ring.ringTokensByZone = ringDesc.getTokensByZone()
			ring.ringZones = getZones(ringDesc.getTokensByZone())

			// Compute the update shard for each tenant and compare it with the initial one.
			// If the "consistency" property is guaranteed, we expect no more then 1 different instance
			// in the updated shard.
			for id := 0; id < numTenants; id++ {
				updated, err := ring.ShuffleShard(fmt.Sprintf("%d", id), s.shardSize).GetAllHealthy(Read)
				require.NoError(t, err)

				added, removed := compareReplicationSets(initial[id], updated)
				assert.LessOrEqual(t, len(added), 1)
				assert.LessOrEqual(t, len(removed), 1)
			}
		})
	}
}

func TestRing_ShuffleShard_ConsistencyOnShardSizeChanged(t *testing.T) {
	// Create 30 instances in 3 zones.
	ringInstances := map[string]IngesterDesc{}
	for i := 0; i < 30; i++ {
		name, desc := generateRingInstance(i, i%3)
		ringInstances[name] = desc
	}

	// Init the ring.
	ringDesc := &Desc{Ingesters: ringInstances}
	ring := Ring{
		cfg: Config{
			HeartbeatTimeout:     time.Hour,
			ZoneAwarenessEnabled: true,
		},
		ringDesc:         ringDesc,
		ringTokens:       ringDesc.getTokens(),
		ringTokensByZone: ringDesc.getTokensByZone(),
		ringZones:        getZones(ringDesc.getTokensByZone()),
		strategy:         &DefaultReplicationStrategy{},
	}

	// Get the replication set with shard size = 3.
	firstShard := ring.ShuffleShard("tenant-id", 3)
	assert.Equal(t, 3, firstShard.IngesterCount())

	firstSet, err := firstShard.GetAllHealthy(Read)
	require.NoError(t, err)

	// Increase shard size to 6.
	secondShard := ring.ShuffleShard("tenant-id", 6)
	assert.Equal(t, 6, secondShard.IngesterCount())

	secondSet, err := secondShard.GetAllHealthy(Read)
	require.NoError(t, err)

	for _, firstInstance := range firstSet.Ingesters {
		assert.True(t, secondSet.Includes(firstInstance.Addr), "new replication set is expected to include previous instance %s", firstInstance.Addr)
	}

	// Increase shard size to 9.
	thirdShard := ring.ShuffleShard("tenant-id", 9)
	assert.Equal(t, 9, thirdShard.IngesterCount())

	thirdSet, err := thirdShard.GetAllHealthy(Read)
	require.NoError(t, err)

	for _, secondInstance := range secondSet.Ingesters {
		assert.True(t, thirdSet.Includes(secondInstance.Addr), "new replication set is expected to include previous instance %s", secondInstance.Addr)
	}

	// Decrease shard size to 6.
	fourthShard := ring.ShuffleShard("tenant-id", 6)
	assert.Equal(t, 6, fourthShard.IngesterCount())

	fourthSet, err := fourthShard.GetAllHealthy(Read)
	require.NoError(t, err)

	// We expect to have the same exact instances we had when the shard size was 6.
	for _, secondInstance := range secondSet.Ingesters {
		assert.True(t, fourthSet.Includes(secondInstance.Addr), "new replication set is expected to include previous instance %s", secondInstance.Addr)
	}

	// Decrease shard size to 3.
	fifthShard := ring.ShuffleShard("tenant-id", 3)
	assert.Equal(t, 3, fifthShard.IngesterCount())

	fifthSet, err := fifthShard.GetAllHealthy(Read)
	require.NoError(t, err)

	// We expect to have the same exact instances we had when the shard size was 3.
	for _, firstInstance := range firstSet.Ingesters {
		assert.True(t, fifthSet.Includes(firstInstance.Addr), "new replication set is expected to include previous instance %s", firstInstance.Addr)
	}
}

func TestRing_ShuffleShard_ConsistencyOnZonesChanged(t *testing.T) {
	// Create 20 instances in 2 zones.
	ringInstances := map[string]IngesterDesc{}
	for i := 0; i < 20; i++ {
		name, desc := generateRingInstance(i, i%2)
		ringInstances[name] = desc
	}

	// Init the ring.
	ringDesc := &Desc{Ingesters: ringInstances}
	ring := Ring{
		cfg: Config{
			HeartbeatTimeout:     time.Hour,
			ZoneAwarenessEnabled: true,
		},
		ringDesc:         ringDesc,
		ringTokens:       ringDesc.getTokens(),
		ringTokensByZone: ringDesc.getTokensByZone(),
		ringZones:        getZones(ringDesc.getTokensByZone()),
		strategy:         &DefaultReplicationStrategy{},
	}

	// Get the replication set with shard size = 2.
	firstShard := ring.ShuffleShard("tenant-id", 2)
	assert.Equal(t, 2, firstShard.IngesterCount())

	firstSet, err := firstShard.GetAllHealthy(Read)
	require.NoError(t, err)

	// Increase shard size to 4.
	secondShard := ring.ShuffleShard("tenant-id", 4)
	assert.Equal(t, 4, secondShard.IngesterCount())

	secondSet, err := secondShard.GetAllHealthy(Read)
	require.NoError(t, err)

	for _, firstInstance := range firstSet.Ingesters {
		assert.True(t, secondSet.Includes(firstInstance.Addr), "new replication set is expected to include previous instance %s", firstInstance.Addr)
	}

	// Scale up cluster, adding 10 instances in 1 new zone.
	for i := 20; i < 30; i++ {
		name, desc := generateRingInstance(i, 2)
		ringInstances[name] = desc
	}

	ring.ringDesc.Ingesters = ringInstances
	ring.ringTokens = ringDesc.getTokens()
	ring.ringTokensByZone = ringDesc.getTokensByZone()
	ring.ringZones = getZones(ringDesc.getTokensByZone())

	// Increase shard size to 6.
	thirdShard := ring.ShuffleShard("tenant-id", 6)
	assert.Equal(t, 6, thirdShard.IngesterCount())

	thirdSet, err := thirdShard.GetAllHealthy(Read)
	require.NoError(t, err)

	for _, secondInstance := range secondSet.Ingesters {
		assert.True(t, thirdSet.Includes(secondInstance.Addr), "new replication set is expected to include previous instance %s", secondInstance.Addr)
	}

	// Increase shard size to 9.
	fourthShard := ring.ShuffleShard("tenant-id", 9)
	assert.Equal(t, 9, fourthShard.IngesterCount())

	fourthSet, err := fourthShard.GetAllHealthy(Read)
	require.NoError(t, err)

	for _, thirdInstance := range thirdSet.Ingesters {
		assert.True(t, fourthSet.Includes(thirdInstance.Addr), "new replication set is expected to include previous instance %s", thirdInstance.Addr)
	}
}

func TestRing_ShuffleShardWithLookback(t *testing.T) {
	type eventType int

	const (
		add eventType = iota
		remove
		test

		lookbackPeriod = time.Hour
		userID         = "user-1"
	)

	var (
		now = time.Now()
	)

	type event struct {
		what         eventType
		instanceID   string
		instanceDesc IngesterDesc
		shardSize    int
		expected     []string
	}

	tests := map[string]struct {
		timeline []event
	}{
		"single zone, shard size = 1, recently bootstrapped cluster": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-time.Minute))},
				{what: test, shardSize: 1, expected: []string{"instance-1", "instance-2", "instance-3"}},
			},
		},
		"single zone, shard size = 1, instances scale up": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 3}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 1, expected: []string{"instance-1"}},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 2}, now.Add(-10*time.Minute))},
				{what: test, shardSize: 1, expected: []string{"instance-4" /* lookback: */, "instance-1"}},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-5*time.Minute))},
				{what: test, shardSize: 1, expected: []string{"instance-5" /* lookback: */, "instance-4", "instance-1"}},
			},
		},
		"single zone, shard size = 1, instances scale down": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 1, expected: []string{"instance-1"}},
				{what: remove, instanceID: "instance-3"},
				{what: test, shardSize: 1, expected: []string{"instance-1"}},
				{what: remove, instanceID: "instance-1"},
				{what: test, shardSize: 1, expected: []string{"instance-2"}},
			},
		},
		"single zone, shard size = 1, rollout with instances unregistered (removed and re-added one by one)": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 1, expected: []string{"instance-1"}},
				// Rollout instance-3.
				{what: remove, instanceID: "instance-3"},
				{what: test, shardSize: 1, expected: []string{"instance-1"}},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now)},
				{what: test, shardSize: 1, expected: []string{"instance-1"}},
				// Rollout instance-2.
				{what: remove, instanceID: "instance-2"},
				{what: test, shardSize: 1, expected: []string{"instance-1"}},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now)},
				{what: test, shardSize: 1, expected: []string{"instance-1"}},
				// Rollout instance-1.
				{what: remove, instanceID: "instance-1"},
				{what: test, shardSize: 1, expected: []string{"instance-2" /* side effect: */, "instance-3"}},
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now)},
				{what: test, shardSize: 1, expected: []string{"instance-1" /* lookback: */, "instance-2" /* side effect: */, "instance-3"}},
			},
		},
		"single zone, shard size = 2, rollout with instances unregistered (removed and re-added one by one)": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-a", []uint32{userToken(userID, "zone-a", 3) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2"}},
				// Rollout instance-4.
				{what: remove, instanceID: "instance-4"},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2"}},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-a", []uint32{userToken(userID, "zone-a", 3) + 1}, now)},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2"}},
				// Rollout instance-3.
				{what: remove, instanceID: "instance-3"},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2"}},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now)},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2"}},
				// Rollout instance-2.
				{what: remove, instanceID: "instance-2"},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-3" /* side effect:*/, "instance-4"}},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now)},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2" /* lookback: */, "instance-3" /* side effect:*/, "instance-4"}},
				// Rollout instance-1.
				{what: remove, instanceID: "instance-1"},
				{what: test, shardSize: 2, expected: []string{"instance-2" /* lookback: */, "instance-3" /* side effect:*/, "instance-4"}},
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now)},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2" /* lookback: */, "instance-3" /* side effect:*/, "instance-4"}},
			},
		},
		"single zone, increase shard size": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 1, expected: []string{"instance-1"}},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2"}},
				{what: test, shardSize: 3, expected: []string{"instance-1", "instance-2", "instance-3"}},
				{what: test, shardSize: 4, expected: []string{"instance-1", "instance-2", "instance-3"}},
			},
		},
		"multi zone, shard size = 3, recently bootstrapped cluster": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-a", []uint32{userToken(userID, "zone-a", 3) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-b", []uint32{userToken(userID, "zone-b", 4) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-6", instanceDesc: generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 5) + 1}, now.Add(-time.Minute))},
				{what: test, shardSize: 3, expected: []string{"instance-1", "instance-2", "instance-3", "instance-4", "instance-5", "instance-6"}},
			},
		},
		"multi zone, shard size = 3, instances scale up": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 2}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 2}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 2}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 3}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 3}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-6", instanceDesc: generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 3}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 3, expected: []string{"instance-1", "instance-2", "instance-3"}},
				// Scale up.
				{what: add, instanceID: "instance-7", instanceDesc: generateRingInstanceWithInfo("instance-7", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now)},
				{what: test, shardSize: 3, expected: []string{"instance-7", "instance-2", "instance-3" /* lookback: */, "instance-1"}},
				{what: add, instanceID: "instance-8", instanceDesc: generateRingInstanceWithInfo("instance-8", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now)},
				{what: test, shardSize: 3, expected: []string{"instance-7", "instance-8", "instance-3" /* lookback: */, "instance-1", "instance-2"}},
				{what: add, instanceID: "instance-9", instanceDesc: generateRingInstanceWithInfo("instance-9", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 1}, now)},
				{what: test, shardSize: 3, expected: []string{"instance-7", "instance-8", "instance-9" /* lookback: */, "instance-1", "instance-2", "instance-3"}},
			},
		},
		"multi zone, shard size = 3, instances scale down": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 3}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 3}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-6", instanceDesc: generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 3}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-7", instanceDesc: generateRingInstanceWithInfo("instance-7", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 2}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-8", instanceDesc: generateRingInstanceWithInfo("instance-8", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 2}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-9", instanceDesc: generateRingInstanceWithInfo("instance-9", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 2}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 3, expected: []string{"instance-1", "instance-2", "instance-3"}},
				// Scale down.
				{what: remove, instanceID: "instance-1"},
				{what: test, shardSize: 3, expected: []string{"instance-7", "instance-2", "instance-3"}},
				{what: remove, instanceID: "instance-2"},
				{what: test, shardSize: 3, expected: []string{"instance-7", "instance-8", "instance-3"}},
				{what: remove, instanceID: "instance-3"},
				{what: test, shardSize: 3, expected: []string{"instance-7", "instance-8", "instance-9"}},
			},
		},
		"multi zone, increase shard size": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 3}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 3}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-6", instanceDesc: generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 3}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-7", instanceDesc: generateRingInstanceWithInfo("instance-7", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 2}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-8", instanceDesc: generateRingInstanceWithInfo("instance-8", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 2}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-9", instanceDesc: generateRingInstanceWithInfo("instance-9", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 2}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 3, expected: []string{"instance-1", "instance-2", "instance-3"}},
				{what: test, shardSize: 6, expected: []string{"instance-1", "instance-2", "instance-3", "instance-7", "instance-8", "instance-9"}},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Initialise the ring.
			ringDesc := &Desc{Ingesters: map[string]IngesterDesc{}}
			ring := Ring{
				cfg: Config{
					HeartbeatTimeout:     time.Hour,
					ZoneAwarenessEnabled: true,
				},
				ringDesc:         ringDesc,
				ringTokens:       ringDesc.getTokens(),
				ringTokensByZone: ringDesc.getTokensByZone(),
				ringZones:        getZones(ringDesc.getTokensByZone()),
				strategy:         &DefaultReplicationStrategy{},
			}

			// Replay the events on the timeline.
			for _, event := range testData.timeline {
				switch event.what {
				case add:
					ringDesc.Ingesters[event.instanceID] = event.instanceDesc

					ring.ringTokens = ringDesc.getTokens()
					ring.ringTokensByZone = ringDesc.getTokensByZone()
					ring.ringZones = getZones(ringDesc.getTokensByZone())
				case remove:
					delete(ringDesc.Ingesters, event.instanceID)

					ring.ringTokens = ringDesc.getTokens()
					ring.ringTokensByZone = ringDesc.getTokensByZone()
					ring.ringZones = getZones(ringDesc.getTokensByZone())
				case test:
					rs, err := ring.ShuffleShardWithLookback(userID, event.shardSize, lookbackPeriod, time.Now()).GetAllHealthy(Read)
					require.NoError(t, err)
					assert.ElementsMatch(t, event.expected, rs.GetAddresses())
				}
			}
		})
	}
}

func TestRing_ShuffleShardWithLookback_CorrectnessWithFuzzy(t *testing.T) {
	// The goal of this test is NOT to ensure that the minimum required number of instances
	// are returned at any given time, BUT at least all required instances are returned.
	var (
		numInitialInstances = []int{9, 30, 60, 90}
		numInitialZones     = []int{1, 3}
		numEvents           = 100
		lookbackPeriod      = time.Hour
		delayBetweenEvents  = 5 * time.Minute // 12 events / hour
		userID              = "user-1"
	)

	for _, numInstances := range numInitialInstances {
		for _, numZones := range numInitialZones {
			testName := fmt.Sprintf("num instances = %d, num zones = %d", numInstances, numZones)

			t.Run(testName, func(t *testing.T) {
				// Randomise the seed but log it in case we need to reproduce the test on failure.
				seed := time.Now().UnixNano()
				rand.Seed(seed)
				t.Log("random generator seed:", seed)

				// Initialise the ring.
				ringDesc := &Desc{Ingesters: generateRingInstances(numInstances, numZones)}
				ring := Ring{
					cfg: Config{
						HeartbeatTimeout:     time.Hour,
						ZoneAwarenessEnabled: true,
					},
					ringDesc:         ringDesc,
					ringTokens:       ringDesc.getTokens(),
					ringTokensByZone: ringDesc.getTokensByZone(),
					ringZones:        getZones(ringDesc.getTokensByZone()),
					strategy:         &DefaultReplicationStrategy{},
				}

				// The simulation starts with the minimum shard size. Random events can later increase it.
				shardSize := numZones

				// The simulation assumes the initial ring contains instances registered
				// since more than the lookback period.
				currTime := time.Now().Add(lookbackPeriod).Add(time.Minute)

				// Add the initial shard to the history.
				rs, err := ring.shuffleShard(userID, shardSize, 0, time.Now()).GetReplicationSetForOperation(Read)
				require.NoError(t, err)

				history := map[time.Time]ReplicationSet{
					currTime: rs,
				}

				// Simulate a progression of random events over the time and, at each iteration of the simuation,
				// make sure the subring includes all non-removed instances picked from previous versions of the
				// ring up until the lookback period.
				nextIngesterID := len(ringDesc.Ingesters) + 1

				for i := 1; i <= numEvents; i++ {
					currTime = currTime.Add(delayBetweenEvents)

					switch r := rand.Intn(100); {
					case r < 80:
						// Scale up instances by 1.
						instanceID := fmt.Sprintf("instance-%d", nextIngesterID)
						zoneID := fmt.Sprintf("zone-%d", nextIngesterID%numZones)
						nextIngesterID++

						ringDesc.Ingesters[instanceID] = generateRingInstanceWithInfo(instanceID, zoneID, GenerateTokens(128, nil), currTime)

						ring.ringTokens = ringDesc.getTokens()
						ring.ringTokensByZone = ringDesc.getTokensByZone()
						ring.ringZones = getZones(ringDesc.getTokensByZone())
					case r < 90:
						// Scale down instances by 1. To make tests reproducible we get the instance IDs, sort them
						// and then get a random index (using the random generator initialized with a constant seed).
						ingesterIDs := make([]string, 0, len(ringDesc.Ingesters))
						for id := range ringDesc.Ingesters {
							ingesterIDs = append(ingesterIDs, id)
						}

						sort.Strings(ingesterIDs)

						idxToRemove := rand.Intn(len(ingesterIDs))
						idToRemove := ingesterIDs[idxToRemove]
						delete(ringDesc.Ingesters, idToRemove)

						ring.ringTokens = ringDesc.getTokens()
						ring.ringTokensByZone = ringDesc.getTokensByZone()
						ring.ringZones = getZones(ringDesc.getTokensByZone())

						// Remove the terminated instance from the history.
						for ringTime, ringState := range history {
							for idx, desc := range ringState.Ingesters {
								// In this simulation instance ID == instance address.
								if desc.Addr != idToRemove {
									continue
								}

								ringState.Ingesters = append(ringState.Ingesters[:idx], ringState.Ingesters[idx+1:]...)
								history[ringTime] = ringState
								break
							}
						}
					default:
						// Scale up shard size (keeping the per-zone balance).
						shardSize += numZones
					}

					// Add the current shard to the history.
					rs, err = ring.shuffleShard(userID, shardSize, 0, time.Now()).GetReplicationSetForOperation(Read)
					require.NoError(t, err)
					history[currTime] = rs

					// Ensure the shard with lookback includes all instances from previous states of the ring.
					rsWithLookback, err := ring.ShuffleShardWithLookback(userID, shardSize, lookbackPeriod, currTime).GetReplicationSetForOperation(Read)
					require.NoError(t, err)

					for ringTime, ringState := range history {
						if ringTime.Before(currTime.Add(-lookbackPeriod)) {
							// This entry from the history is obsolete, we can remove it.
							delete(history, ringTime)
							continue
						}

						for _, expectedAddr := range ringState.GetAddresses() {
							if !rsWithLookback.Includes(expectedAddr) {
								t.Fatalf(
									"subring generated after event %d is expected to include instance %s from ring state at time %s but it's missing (actual instances are: %s)",
									i, expectedAddr, ringTime.String(), strings.Join(rsWithLookback.GetAddresses(), ", "))
							}
						}
					}
				}
			})
		}
	}
}

func BenchmarkRing_ShuffleShard(b *testing.B) {
	for _, numInstances := range []int{50, 100, 1000} {
		for _, numZones := range []int{1, 3} {
			for _, shardSize := range []int{3, 10, 30} {
				b.Run(fmt.Sprintf("num instances = %d, num zones = %d, shard size = %d", numInstances, numZones, shardSize), func(b *testing.B) {
					benchmarkShuffleSharding(b, numInstances, numZones, shardSize, false)
				})
			}
		}
	}
}

func BenchmarkRing_ShuffleShardCached(b *testing.B) {
	for _, numInstances := range []int{50, 100, 1000} {
		for _, numZones := range []int{1, 3} {
			for _, shardSize := range []int{3, 10, 30} {
				b.Run(fmt.Sprintf("num instances = %d, num zones = %d, shard size = %d", numInstances, numZones, shardSize), func(b *testing.B) {
					benchmarkShuffleSharding(b, numInstances, numZones, shardSize, true)
				})
			}
		}
	}
}

func benchmarkShuffleSharding(b *testing.B, numInstances, numZones, shardSize int, cache bool) {
	// Initialise the ring.
	ringDesc := &Desc{Ingesters: generateRingInstances(numInstances, numZones)}
	ring := Ring{
		cfg:                Config{HeartbeatTimeout: time.Hour, ZoneAwarenessEnabled: true},
		ringDesc:           ringDesc,
		ringTokens:         ringDesc.getTokens(),
		ringTokensByZone:   ringDesc.getTokensByZone(),
		ringZones:          getZones(ringDesc.getTokensByZone()),
		strategy:           &DefaultReplicationStrategy{},
		lastTopologyChange: time.Now(),
	}

	if cache {
		ring.shuffledSubringCache = map[subringCacheKey]*Ring{}
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		ring.ShuffleShard("tenant-1", shardSize)
	}
}

// generateTokensLinear returns tokens with a linear distribution.
func generateTokensLinear(instanceID, numInstances, numTokens int) []uint32 {
	tokens := make([]uint32, 0, numTokens)
	step := math.MaxUint32 / numTokens
	offset := (step / numInstances) * instanceID

	for t := offset; t <= math.MaxUint32; t += step {
		tokens = append(tokens, uint32(t))
	}

	return tokens
}

func generateRingInstances(numInstances, numZones int) map[string]IngesterDesc {
	instances := make(map[string]IngesterDesc, numInstances)

	for i := 1; i <= numInstances; i++ {
		id, desc := generateRingInstance(i, i%numZones)
		instances[id] = desc
	}

	return instances
}

func generateRingInstance(id, zone int) (string, IngesterDesc) {
	instanceID := fmt.Sprintf("instance-%d", id)
	zoneID := fmt.Sprintf("zone-%d", zone)

	return instanceID, generateRingInstanceWithInfo(instanceID, zoneID, GenerateTokens(128, nil), time.Now())
}

func generateRingInstanceWithInfo(addr, zone string, tokens []uint32, registeredAt time.Time) IngesterDesc {
	return IngesterDesc{
		Addr:                addr,
		Timestamp:           time.Now().Unix(),
		RegisteredTimestamp: registeredAt.Unix(),
		State:               ACTIVE,
		Tokens:              tokens,
		Zone:                zone,
	}
}

// compareReplicationSets returns the list of instance addresses which differ between the two sets.
func compareReplicationSets(first, second ReplicationSet) (added, removed []string) {
	for _, instance := range first.Ingesters {
		if !second.Includes(instance.Addr) {
			added = append(added, instance.Addr)
		}
	}

	for _, instance := range second.Ingesters {
		if !first.Includes(instance.Addr) {
			removed = append(removed, instance.Addr)
		}
	}

	return
}

// This test verifies that ring is getting updates, even after extending check in the loop method.
func TestRingUpdates(t *testing.T) {
	inmem := consul.NewInMemoryClient(GetCodec())

	cfg := Config{
		KVStore:           kv.Config{Mock: inmem},
		HeartbeatTimeout:  1 * time.Minute,
		ReplicationFactor: 3,
	}

	ring, err := New(cfg, "test", "test", nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ring))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), ring)
	})

	require.Equal(t, 0, ring.IngesterCount())

	lc1 := startLifecycler(t, cfg, 100*time.Millisecond, 1, 3)
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return ring.IngesterCount()
	})

	lc2 := startLifecycler(t, cfg, 100*time.Millisecond, 2, 3)
	test.Poll(t, 1*time.Second, 2, func() interface{} {
		return ring.IngesterCount()
	})

	lc3 := startLifecycler(t, cfg, 100*time.Millisecond, 3, 3)
	test.Poll(t, 1*time.Second, 3, func() interface{} {
		return ring.IngesterCount()
	})

	// Sleep for a few seconds (ring timestamp resolution is 1 second, so to verify that ring is updated in the background,
	// sleep for 2 seconds)
	time.Sleep(2 * time.Second)

	rs, err := ring.GetAllHealthy(Read)
	require.NoError(t, err)

	now := time.Now()
	for _, ing := range rs.Ingesters {
		require.InDelta(t, now.UnixNano(), time.Unix(ing.Timestamp, 0).UnixNano(), float64(1500*time.Millisecond.Nanoseconds()))
	}

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), lc2))
	test.Poll(t, 1*time.Second, 2, func() interface{} {
		return ring.IngesterCount()
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), lc1))
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return ring.IngesterCount()
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), lc3))
	test.Poll(t, 1*time.Second, 0, func() interface{} {
		return ring.IngesterCount()
	})
}

func startLifecycler(t *testing.T, cfg Config, heartbeat time.Duration, lifecyclerID int, zones int) *Lifecycler {
	lcCfg := LifecyclerConfig{
		RingConfig:      cfg,
		NumTokens:       16,
		HeartbeatPeriod: heartbeat,
		ObservePeriod:   0,
		JoinAfter:       0,
		Zone:            fmt.Sprintf("zone-%d", lifecyclerID%zones),
		Addr:            fmt.Sprintf("addr-%d", lifecyclerID),
		ID:              fmt.Sprintf("ingester-%d", lifecyclerID),
	}

	lc, err := NewLifecycler(lcCfg, &noopFlushTransferer{}, "test", "test", false, nil)
	require.NoError(t, err)

	lc.AddListener(services.NewListener(nil, nil, nil, nil, func(from services.State, failure error) {
		t.Log("lifecycler", lifecyclerID, "failed:", failure)
		t.Fail()
	}))

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), lc))

	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), lc)
	})

	return lc
}

// This test checks if shuffle-sharded ring can be reused, and whether it receives
// updates from "main" ring.
func TestShuffleShardWithCaching(t *testing.T) {
	inmem := consul.NewInMemoryClientWithConfig(GetCodec(), consul.Config{
		MaxCasRetries: 20,
		CasRetryDelay: 500 * time.Millisecond,
	})

	cfg := Config{
		KVStore:              kv.Config{Mock: inmem},
		HeartbeatTimeout:     1 * time.Minute,
		ReplicationFactor:    3,
		ZoneAwarenessEnabled: true,
	}

	ring, err := New(cfg, "test", "test", nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ring))
	t.Cleanup(func() {
		_ = services.StartAndAwaitRunning(context.Background(), ring)
	})

	// We will stop <number of zones> ingesters later, to see that subring is recomputed.
	const numLifecyclers = 6
	const zones = 3

	lcs := []*Lifecycler(nil)
	for i := 0; i < numLifecyclers; i++ {
		lc := startLifecycler(t, cfg, 500*time.Millisecond, i, zones)

		lcs = append(lcs, lc)
	}

	// Wait until all instances in the ring are ACTIVE.
	test.Poll(t, 5*time.Second, numLifecyclers, func() interface{} {
		active := 0
		rs, _ := ring.GetReplicationSetForOperation(Read)
		for _, ing := range rs.Ingesters {
			if ing.State == ACTIVE {
				active++
			}
		}
		return active
	})

	// Use shardSize = zones, to get one ingester from each zone.
	const shardSize = zones
	const user = "user"

	// This subring should be cached, and reused.
	subring := ring.ShuffleShard(user, shardSize)

	// Do 100 iterations over two seconds. Make sure we get the same subring.
	const iters = 100
	sleep := (2 * time.Second) / iters
	for i := 0; i < iters; i++ {
		newSubring := ring.ShuffleShard(user, shardSize)
		require.True(t, subring == newSubring, "cached subring reused")
		require.Equal(t, shardSize, subring.IngesterCount())
		time.Sleep(sleep)
	}

	// Make sure subring has up-to-date timestamps.
	{
		rs, err := subring.GetReplicationSetForOperation(Read)
		require.NoError(t, err)

		now := time.Now()
		for _, ing := range rs.Ingesters {
			// Lifecyclers use 500ms refresh, but timestamps use 1s resolution, so we better give it some extra buffer.
			assert.InDelta(t, now.UnixNano(), time.Unix(ing.Timestamp, 0).UnixNano(), float64(2*time.Second.Nanoseconds()))
		}
	}

	// Now stop one lifecycler from each zone. Subring needs to be recomputed.
	for i := 0; i < zones; i++ {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), lcs[i]))
	}

	test.Poll(t, 5*time.Second, numLifecyclers-zones, func() interface{} {
		return ring.IngesterCount()
	})

	// Change of ingesters -> new subring needed.
	newSubring := ring.ShuffleShard("user", zones)
	require.False(t, subring == newSubring)
	require.Equal(t, zones, subring.IngesterCount())

	// Change of shard size -> new subring needed.
	subring = newSubring
	newSubring = ring.ShuffleShard("user", 1)
	require.False(t, subring == newSubring)
	// Zone-aware shuffle-shard gives all zones the same number of ingesters (at least one).
	require.Equal(t, zones, newSubring.IngesterCount())
}

// User shuffle shard token.
func userToken(user, zone string, skip int) uint32 {
	r := rand.New(rand.NewSource(util.ShuffleShardSeed(user, zone)))

	for ; skip > 0; skip-- {
		_ = r.Uint32()
	}
	return r.Uint32()
}
