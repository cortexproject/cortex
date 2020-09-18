package ring

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	strconv "strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
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
		desc.AddIngester(fmt.Sprintf("%d", i), fmt.Sprintf("ingester%d", i), strconv.Itoa(i), tokens, ACTIVE)
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

	ing1Tokens := GenerateTokens(128, nil)

	r.AddIngester(ingName, "addr", "1", ing1Tokens, ACTIVE)

	require.Equal(t, "addr", r.Ingesters[ingName].Addr)
	require.Equal(t, ing1Tokens, r.Ingesters[ingName].Tokens)
}

func TestAddIngesterReplacesExistingTokens(t *testing.T) {
	r := NewDesc()

	const ing1Name = "ing1"

	// old tokens will be replaced
	r.Ingesters[ing1Name] = IngesterDesc{
		Tokens: []uint32{11111, 22222, 33333},
	}

	newTokens := GenerateTokens(128, nil)

	r.AddIngester(ing1Name, "addr", "1", newTokens, ACTIVE)

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

				r.AddIngester(name, fmt.Sprintf("127.0.0.%d", i), fmt.Sprintf("zone-%v", i%testData.numZones), ingTokens, ACTIVE)

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
			// Init the ring description.
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
					all, err := shardRing.GetAll(Read)
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
			expected, err := r.GetAll(Read)
			require.NoError(t, err)

			// Assert that multiple invocations generate the same exact shard.
			for n := 0; n < numInvocations; n++ {
				r := ring.ShuffleShard(tenantID, size)
				actual, err := r.GetAll(Read)
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
			Addr:      fmt.Sprintf("127.0.0.%d", i),
			Timestamp: time.Now().Unix(),
			State:     ACTIVE,
			Tokens:    generateTokensLinear(i, numInstances, 128),
			Zone:      fmt.Sprintf("zone-%d", i%numZones),
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
		set, err := r.GetAll(Read)
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
		// clamping it between 1% and 0.1% boundaries.
		maxDeviance := math.Min(1, math.Max(0.1, probability*0.1))

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
				set, err := ring.ShuffleShard(fmt.Sprintf("%d", id), s.shardSize).GetAll(Read)
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
				updated, err := ring.ShuffleShard(fmt.Sprintf("%d", id), s.shardSize).GetAll(Read)
				require.NoError(t, err)

				added, removed := compareReplicationSets(initial[id], updated)
				assert.LessOrEqual(t, len(added), 1)
				assert.LessOrEqual(t, len(removed), 1)
			}
		})
	}
}

func BenchmarkRing_ShuffleShard(b *testing.B) {
	for _, numInstances := range []int{50, 100, 1000} {
		for _, numZones := range []int{1, 3} {
			for _, shardSize := range []int{3, 10, 30} {
				b.Run(fmt.Sprintf("num instances = %d, num zones = %d, shard size = %d", numInstances, numZones, shardSize), func(b *testing.B) {
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

					b.ResetTimer()

					for n := 0; n < b.N; n++ {
						ring.ShuffleShard("tenant-1", shardSize)
					}
				})
			}
		}
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
	return fmt.Sprintf("instance-%d", id), IngesterDesc{
		Addr:      fmt.Sprintf("127.0.0.%d", id),
		Timestamp: time.Now().Unix(),
		State:     ACTIVE,
		Tokens:    GenerateTokens(128, nil),
		Zone:      fmt.Sprintf("zone-%d", zone),
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
