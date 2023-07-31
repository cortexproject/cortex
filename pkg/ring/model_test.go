package ring

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInstanceDesc_IsHealthy_ForIngesterOperations(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		ingester       *InstanceDesc
		timeout        time.Duration
		writeExpected  bool
		readExpected   bool
		reportExpected bool
	}{
		"ACTIVE ingester with last keepalive newer than timeout": {
			ingester:       &InstanceDesc{State: ACTIVE, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:        time.Minute,
			writeExpected:  true,
			readExpected:   true,
			reportExpected: true,
		},
		"ACTIVE ingester with last keepalive older than timeout": {
			ingester:       &InstanceDesc{State: ACTIVE, Timestamp: time.Now().Add(-90 * time.Second).Unix()},
			timeout:        time.Minute,
			writeExpected:  false,
			readExpected:   false,
			reportExpected: false,
		},
		"JOINING ingester with last keepalive newer than timeout": {
			ingester:       &InstanceDesc{State: JOINING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:        time.Minute,
			writeExpected:  false,
			readExpected:   true,
			reportExpected: true,
		},
		"LEAVING ingester with last keepalive newer than timeout": {
			ingester:       &InstanceDesc{State: LEAVING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:        time.Minute,
			writeExpected:  false,
			readExpected:   true,
			reportExpected: true,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			actual := testData.ingester.IsHealthy(Write, testData.timeout, time.Now())
			assert.Equal(t, testData.writeExpected, actual)

			actual = testData.ingester.IsHealthy(Read, testData.timeout, time.Now())
			assert.Equal(t, testData.readExpected, actual)

			actual = testData.ingester.IsHealthy(Reporting, testData.timeout, time.Now())
			assert.Equal(t, testData.reportExpected, actual)
		})
	}
}

func TestInstanceDesc_GetRegisteredAt(t *testing.T) {
	tests := map[string]struct {
		desc     *InstanceDesc
		expected time.Time
	}{
		"should return zero value on nil desc": {
			desc:     nil,
			expected: time.Time{},
		},
		"should return zero value registered timestamp = 0": {
			desc: &InstanceDesc{
				RegisteredTimestamp: 0,
			},
			expected: time.Time{},
		},
		"should return timestamp parsed from desc": {
			desc: &InstanceDesc{
				RegisteredTimestamp: time.Unix(10000000, 0).Unix(),
			},
			expected: time.Unix(10000000, 0),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.True(t, testData.desc.GetRegisteredAt().Equal(testData.expected))
		})
	}
}

func TestHasInstanceDescsChanged_TokensOrZone(t *testing.T) {
	tests := map[string]struct {
		before     map[string]InstanceDesc
		after      map[string]InstanceDesc
		hasChanged bool
	}{
		"should return false if same": {
			before: map[string]InstanceDesc{
				"instance-1": {Tokens: []uint32{1, 5, 7}, Zone: "zone-1"},
				"instance-2": {Tokens: []uint32{2, 3, 8}, Zone: "zone-2"},
			},
			after: map[string]InstanceDesc{
				"instance-1": {Tokens: []uint32{1, 5, 7}, Zone: "zone-1"},
				"instance-2": {Tokens: []uint32{2, 3, 8}, Zone: "zone-2"},
			},
			hasChanged: false,
		},
		"should return true if zone is different": {
			before: map[string]InstanceDesc{
				"instance-1": {Tokens: []uint32{1, 5, 7}, Zone: "zone-1"},
				"instance-2": {Tokens: []uint32{2, 3, 8}, Zone: "zone-2"},
			},
			after: map[string]InstanceDesc{
				"instance-1": {Tokens: []uint32{1, 5, 7}, Zone: "zone-1"},
				"instance-2": {Tokens: []uint32{2, 3, 8}, Zone: "zone-3"},
			},
			hasChanged: true,
		},
		"should return true if token is different": {
			before: map[string]InstanceDesc{
				"instance-1": {Tokens: []uint32{1, 5, 7}, Zone: "zone-1"},
				"instance-2": {Tokens: []uint32{2, 3, 8}, Zone: "zone-2"},
			},
			after: map[string]InstanceDesc{
				"instance-1": {Tokens: []uint32{1, 5, 7}, Zone: "zone-1"},
				"instance-2": {Tokens: []uint32{2, 3, 9}, Zone: "zone-2"},
			},
			hasChanged: true,
		},
		"should return true if token is added": {
			before: map[string]InstanceDesc{
				"instance-1": {Tokens: []uint32{1, 5, 7}, Zone: "zone-1"},
				"instance-2": {Tokens: []uint32{2, 3, 8}, Zone: "zone-2"},
			},
			after: map[string]InstanceDesc{
				"instance-1": {Tokens: []uint32{1, 5, 7}, Zone: "zone-1"},
				"instance-2": {Tokens: []uint32{2, 3, 8, 9}, Zone: "zone-2"},
			},
			hasChanged: true,
		},
		"should return true if token is removed": {
			before: map[string]InstanceDesc{
				"instance-1": {Tokens: []uint32{1, 5, 7}, Zone: "zone-1"},
				"instance-2": {Tokens: []uint32{2, 3, 8}, Zone: "zone-2"},
			},
			after: map[string]InstanceDesc{
				"instance-1": {Tokens: []uint32{1, 5, 7}, Zone: "zone-1"},
				"instance-2": {Tokens: []uint32{2, 3}, Zone: "zone-2"},
			},
			hasChanged: true,
		},
		"should return true if tokens are swapped": {
			before: map[string]InstanceDesc{
				"instance-1": {Tokens: []uint32{1, 5, 7}, Zone: "zone-1"},
				"instance-2": {Tokens: []uint32{2, 3, 8}, Zone: "zone-2"},
			},
			after: map[string]InstanceDesc{
				"instance-1": {Tokens: []uint32{2, 3, 8}, Zone: "zone-1"},
				"instance-2": {Tokens: []uint32{1, 5, 7}, Zone: "zone-2"},
			},
			hasChanged: true,
		},
		"should return true if instance is added": {
			before: map[string]InstanceDesc{
				"instance-1": {Tokens: []uint32{1, 5, 7}, Zone: "zone-1"},
				"instance-2": {Tokens: []uint32{2, 3, 8}, Zone: "zone-2"},
			},
			after: map[string]InstanceDesc{
				"instance-1": {Tokens: []uint32{1, 5, 7}, Zone: "zone-1"},
				"instance-2": {Tokens: []uint32{2, 3, 8}, Zone: "zone-2"},
				"instance-3": {Tokens: []uint32{10, 11}, Zone: "zone-3"},
			},
			hasChanged: true,
		},
		"should return true if instance is removed": {
			before: map[string]InstanceDesc{
				"instance-1": {Tokens: []uint32{1, 5, 7}, Zone: "zone-1"},
				"instance-2": {Tokens: []uint32{2, 3, 8}, Zone: "zone-2"},
			},
			after: map[string]InstanceDesc{
				"instance-1": {Tokens: []uint32{1, 5, 7}, Zone: "zone-1"},
			},
			hasChanged: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			hasInstanceDescsChanged := HasInstanceDescsChanged(testData.before, testData.after, func(a, b InstanceDesc) bool {
				return HasTokensChanged(b, a) || HasZoneChanged(b, a)
			})
			assert.Equal(t, testData.hasChanged, hasInstanceDescsChanged)
		})
	}
}

func normalizedSource() *Desc {
	r := NewDesc()
	r.Ingesters["first"] = InstanceDesc{
		Tokens: []uint32{100, 200, 300},
	}
	r.Ingesters["second"] = InstanceDesc{}
	return r
}

func normalizedOutput() *Desc {
	return &Desc{
		Ingesters: map[string]InstanceDesc{
			"first":  {},
			"second": {Tokens: []uint32{100, 200, 300}},
		},
	}
}

func TestClaimTokensFromNormalizedToNormalized(t *testing.T) {
	r := normalizedSource()
	result := r.ClaimTokens("first", "second")

	assert.Equal(t, Tokens{100, 200, 300}, result)
	assert.Equal(t, normalizedOutput(), r)
}

func TestDesc_Ready(t *testing.T) {
	now := time.Now()

	r := &Desc{
		Ingesters: map[string]InstanceDesc{
			"ing1": {
				Tokens:    []uint32{100, 200, 300},
				State:     ACTIVE,
				Timestamp: now.Unix(),
			},
		},
	}

	if err := r.IsReady(now, 10*time.Second); err != nil {
		t.Fatal("expected ready, got", err)
	}

	if err := r.IsReady(now, 0); err != nil {
		t.Fatal("expected ready, got", err)
	}

	if err := r.IsReady(now.Add(5*time.Minute), 10*time.Second); err == nil {
		t.Fatal("expected !ready (no heartbeat from active ingester), but got no error")
	}

	if err := r.IsReady(now.Add(5*time.Minute), 0); err != nil {
		t.Fatal("expected ready (no heartbeat but timeout disabled), got", err)
	}

	r = &Desc{
		Ingesters: map[string]InstanceDesc{
			"ing1": {
				State:     ACTIVE,
				Timestamp: now.Unix(),
			},
		},
	}

	if err := r.IsReady(now, 10*time.Second); err == nil {
		t.Fatal("expected !ready (no tokens), but got no error")
	}

	r.Ingesters["some ingester"] = InstanceDesc{
		Tokens:    []uint32{12345},
		Timestamp: now.Unix(),
	}

	if err := r.IsReady(now, 10*time.Second); err != nil {
		t.Fatal("expected ready, got", err)
	}
}

func TestDesc_getTokensByZone(t *testing.T) {
	tests := map[string]struct {
		desc     *Desc
		expected map[string][]uint32
	}{
		"empty ring": {
			desc:     &Desc{Ingesters: map[string]InstanceDesc{}},
			expected: map[string][]uint32{},
		},
		"single zone": {
			desc: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Tokens: []uint32{1, 5}, Zone: ""},
				"instance-2": {Addr: "127.0.0.1", Tokens: []uint32{2, 4}, Zone: ""},
				"instance-3": {Addr: "127.0.0.1", Tokens: []uint32{3, 6}, Zone: ""},
			}},
			expected: map[string][]uint32{
				"": {1, 2, 3, 4, 5, 6},
			},
		},
		"multiple zones": {
			desc: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Tokens: []uint32{1, 5}, Zone: "zone-1"},
				"instance-2": {Addr: "127.0.0.1", Tokens: []uint32{2, 4}, Zone: "zone-1"},
				"instance-3": {Addr: "127.0.0.1", Tokens: []uint32{3, 6}, Zone: "zone-2"},
			}},
			expected: map[string][]uint32{
				"zone-1": {1, 2, 4, 5},
				"zone-2": {3, 6},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.desc.getTokensByZone())
		})
	}
}

func TestDesc_TokensFor(t *testing.T) {
	tests := map[string]struct {
		desc         *Desc
		expectedMine Tokens
		expectedAll  Tokens
	}{
		"empty ring": {
			desc:         &Desc{Ingesters: map[string]InstanceDesc{}},
			expectedMine: Tokens(nil),
			expectedAll:  Tokens{},
		},
		"single zone": {
			desc: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Tokens: []uint32{1, 5}, Zone: ""},
				"instance-2": {Addr: "127.0.0.1", Tokens: []uint32{2, 4}, Zone: ""},
				"instance-3": {Addr: "127.0.0.1", Tokens: []uint32{3, 6}, Zone: ""},
			}},
			expectedMine: Tokens{1, 5},
			expectedAll:  Tokens{1, 2, 3, 4, 5, 6},
		},
		"multiple zones": {
			desc: &Desc{Ingesters: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Tokens: []uint32{1, 5}, Zone: "zone-1"},
				"instance-2": {Addr: "127.0.0.1", Tokens: []uint32{2, 4}, Zone: "zone-1"},
				"instance-3": {Addr: "127.0.0.1", Tokens: []uint32{3, 6}, Zone: "zone-2"},
			}},
			expectedMine: Tokens{1, 5},
			expectedAll:  Tokens{1, 2, 3, 4, 5, 6},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actualMine, actualAll := testData.desc.TokensFor("instance-1")
			assert.Equal(t, testData.expectedMine, actualMine)
			assert.Equal(t, testData.expectedAll, actualAll)
		})
	}
}

func TestDesc_RingsCompare(t *testing.T) {
	tests := map[string]struct {
		r1, r2   *Desc
		expected CompareResult
	}{
		"nil rings": {
			r1:       nil,
			r2:       nil,
			expected: Equal,
		},
		"one nil, one empty ring": {
			r1:       nil,
			r2:       &Desc{Ingesters: map[string]InstanceDesc{}},
			expected: Equal,
		},
		"two empty rings": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{}},
			expected: Equal,
		},
		"same single instance": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1"}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1"}}},
			expected: Equal,
		},
		"same single instance, different timestamp": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Timestamp: 123456}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Timestamp: 789012}}},
			expected: EqualButStatesAndTimestamps,
		},
		"same single instance, different state": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", State: ACTIVE}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", State: JOINING}}},
			expected: EqualButStatesAndTimestamps,
		},
		"same single instance, different registered timestamp": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", State: ACTIVE, RegisteredTimestamp: 1}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", State: ACTIVE, RegisteredTimestamp: 2}}},
			expected: Different,
		},
		"instance in different zone": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Zone: "one"}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Zone: "two"}}},
			expected: Different,
		},
		"same instance, different address": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1"}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr2"}}},
			expected: Different,
		},
		"more instances in one ring": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1"}, "ing2": {Addr: "ing2"}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1"}}},
			expected: Different,
		},
		"different tokens": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Tokens: []uint32{1, 2, 3}}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1"}}},
			expected: Different,
		},
		"different tokens 2": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Tokens: []uint32{1, 2, 3}}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Tokens: []uint32{1, 2, 4}}}},
			expected: Different,
		},
		"same number of instances, using different IDs": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Tokens: []uint32{1, 2, 3}}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing2": {Addr: "addr1", Tokens: []uint32{1, 2, 3}}}},
			expected: Different,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.r1.RingCompare(testData.r2))
			assert.Equal(t, testData.expected, testData.r2.RingCompare(testData.r1))
		})
	}
}

func TestMergeTokens(t *testing.T) {
	tests := map[string]struct {
		input    [][]uint32
		expected []uint32
	}{
		"empty input": {
			input:    nil,
			expected: []uint32{},
		},
		"single instance in input": {
			input: [][]uint32{
				{1, 3, 4, 8},
			},
			expected: []uint32{1, 3, 4, 8},
		},
		"multiple instances in input": {
			input: [][]uint32{
				{1, 3, 4, 8},
				{0, 2, 6, 9},
				{5, 7, 10, 11},
			},
			expected: []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
		},
		"some instances have no tokens": {
			input: [][]uint32{
				{1, 3, 4, 8},
				{},
				{0, 2, 6, 9},
				{},
				{5, 7, 10, 11},
			},
			expected: []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, MergeTokens(testData.input))
		})
	}
}

func TestMergeTokensByZone(t *testing.T) {
	tests := map[string]struct {
		input    map[string][][]uint32
		expected map[string][]uint32
	}{
		"empty input": {
			input:    nil,
			expected: map[string][]uint32{},
		},
		"single zone": {
			input: map[string][][]uint32{
				"zone-1": {
					{1, 3, 4, 8},
					{2, 5, 6, 7},
				},
			},
			expected: map[string][]uint32{
				"zone-1": {1, 2, 3, 4, 5, 6, 7, 8},
			},
		},
		"multiple zones": {
			input: map[string][][]uint32{
				"zone-1": {
					{1, 3, 4, 8},
					{2, 5, 6, 7},
				},
				"zone-2": {
					{3, 5},
					{2, 4},
				},
			},
			expected: map[string][]uint32{
				"zone-1": {1, 2, 3, 4, 5, 6, 7, 8},
				"zone-2": {2, 3, 4, 5},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, MergeTokensByZone(testData.input))
		})
	}
}

func TestDesc_SplitById_JoinIds(t *testing.T) {
	tests := map[string]struct {
		ring  *Desc
		split map[string]interface{}
	}{
		"empty ring": {
			ring:  &Desc{Ingesters: map[string]InstanceDesc{}},
			split: map[string]interface{}{},
		},
		"single instance": {
			ring:  &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Tokens: []uint32{1, 2, 3}, Timestamp: 123456, State: JOINING, Zone: "zone1", RegisteredTimestamp: 123}}},
			split: map[string]interface{}{"ing1": &InstanceDesc{Addr: "addr1", Tokens: []uint32{1, 2, 3}, Timestamp: 123456, State: JOINING, Zone: "zone1", RegisteredTimestamp: 123}},
		},
		"two instances": {
			ring: &Desc{Ingesters: map[string]InstanceDesc{
				"ing1": {Addr: "addr1", Tokens: []uint32{1, 2, 3}, Timestamp: 123456, State: JOINING, Zone: "zone1", RegisteredTimestamp: 123},
				"ing2": {Addr: "addr2", Tokens: []uint32{3, 4, 5}, Timestamp: 5678, State: ACTIVE, Zone: "zone2", RegisteredTimestamp: 567},
			}},
			split: map[string]interface{}{
				"ing1": &InstanceDesc{Addr: "addr1", Tokens: []uint32{1, 2, 3}, Timestamp: 123456, State: JOINING, Zone: "zone1", RegisteredTimestamp: 123},
				"ing2": &InstanceDesc{Addr: "addr2", Tokens: []uint32{3, 4, 5}, Timestamp: 5678, State: ACTIVE, Zone: "zone2", RegisteredTimestamp: 567},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.split, testData.ring.SplitByID(), "Error on SplitById")

			r := NewDesc()
			r.JoinIds(testData.split)
			assert.Equal(t, testData.ring, r, "Error on JoinIds")
		})
	}
}

func TestDesc_EncodingMultikey(t *testing.T) {
	codec := GetCodec()
	ring := &Desc{Ingesters: map[string]InstanceDesc{
		"ing1": {Addr: "addr1", Timestamp: 123456},
		"ing2": {Addr: "addr2", Timestamp: 5678},
		"ing3": {},
	}}
	encoded, err := codec.EncodeMultiKey(ring)
	assert.NoError(t, err)
	decoded, err := codec.DecodeMultiKey(encoded)
	assert.NoError(t, err)

	assert.Equal(t, ring, decoded)
}

func TestDesc_FindDifference(t *testing.T) {
	tests := map[string]struct {
		r1       *Desc
		r2       *Desc
		toUpdate interface{}
		toDelete interface{}
	}{
		"nil rings": {
			r1:       nil,
			r2:       nil,
			toUpdate: NewDesc(),
			toDelete: []string{},
		},
		"one nil, one empty ring": {
			r1:       nil,
			r2:       &Desc{Ingesters: map[string]InstanceDesc{}},
			toUpdate: NewDesc(),
			toDelete: []string{},
		},
		"one empty, one nil": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{}},
			r2:       nil,
			toUpdate: NewDesc(),
			toDelete: []string{},
		},
		"two empty rings": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{}},
			toUpdate: NewDesc(),
			toDelete: []string{},
		},
		"same single instance": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1"}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1"}}},
			toUpdate: NewDesc(),
			toDelete: []string{},
		},
		"one nil, single instance": {
			r1:       nil,
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1"}}},
			toUpdate: &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1"}}},
			toDelete: []string{},
		},
		"one instance, nil": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1"}}},
			r2:       nil,
			toUpdate: NewDesc(),
			toDelete: []string{"ing1"},
		},
		"same single instance, different timestamp": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Timestamp: 123456}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Timestamp: 789012}}},
			toUpdate: &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Timestamp: 789012}}},
			toDelete: []string{},
		},
		"same single instance, different state": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", State: ACTIVE, Timestamp: 1000}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", State: JOINING, Timestamp: 1100}}},
			toUpdate: &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", State: JOINING, Timestamp: 1100}}},
			toDelete: []string{},
		},
		"same single instance, different registered timestamp": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", State: ACTIVE, RegisteredTimestamp: 1, Timestamp: 1000}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", State: ACTIVE, RegisteredTimestamp: 2, Timestamp: 1100}}},
			toUpdate: &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", State: ACTIVE, RegisteredTimestamp: 2, Timestamp: 1100}}},
			toDelete: []string{},
		},
		"instance in different zone": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Zone: "one", Timestamp: 1000}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Zone: "two", Timestamp: 1100}}},
			toUpdate: &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Zone: "two", Timestamp: 1100}}},
			toDelete: []string{},
		},
		"same instance, different address": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Timestamp: 1000}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr2", Timestamp: 1100}}},
			toUpdate: &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr2", Timestamp: 1100}}},
			toDelete: []string{},
		},
		"more instances in one ring": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1"}, "ing2": {Addr: "ing2"}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1"}}},
			toUpdate: NewDesc(),
			toDelete: []string{"ing2"},
		},
		"more instances in second ring": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1"}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1"}, "ing2": {Addr: "ing2"}}},
			toUpdate: &Desc{Ingesters: map[string]InstanceDesc{"ing2": {Addr: "ing2"}}},
			toDelete: []string{},
		},
		"different tokens": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Tokens: []uint32{1, 2, 3}, Timestamp: 1000}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Timestamp: 1100}}},
			toUpdate: &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Timestamp: 1100}}},
			toDelete: []string{},
		},
		"different tokens 2": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Tokens: []uint32{1, 2, 3}, Timestamp: 1000}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Tokens: []uint32{1, 2, 4}, Timestamp: 1100}}},
			toUpdate: &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Tokens: []uint32{1, 2, 4}, Timestamp: 1100}}},
			toDelete: []string{},
		},
		"different instances, conflictTokens new lose": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Tokens: []uint32{1, 2, 3}}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Tokens: []uint32{1, 2, 3}}, "ing2": {Addr: "addr1", Tokens: []uint32{1, 2, 4}}}},
			toUpdate: &Desc{Ingesters: map[string]InstanceDesc{"ing2": {Addr: "addr1", Tokens: []uint32{4}}}},
			toDelete: []string{},
		},
		"different instances, conflictTokens new win": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing5": {Addr: "addr1", Tokens: []uint32{1, 2, 3}, Timestamp: 1000}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing5": {Addr: "addr1", Tokens: []uint32{1, 2, 3}, Timestamp: 1100}, "ing2": {Addr: "addr1", Tokens: []uint32{1, 2, 4}, Timestamp: 1100}}},
			toUpdate: &Desc{Ingesters: map[string]InstanceDesc{"ing2": {Addr: "addr1", Tokens: []uint32{1, 2, 4}, Timestamp: 1100}, "ing5": {Addr: "addr1", Tokens: []uint32{3}, Timestamp: 1100}}},
			toDelete: []string{},
		},
		"same number of instances, using different IDs": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr1", Tokens: []uint32{1, 2, 3}}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing2": {Addr: "addr1", Tokens: []uint32{1, 2, 3}}}},
			toUpdate: &Desc{Ingesters: map[string]InstanceDesc{"ing2": {Addr: "addr1", Tokens: []uint32{1, 2, 3}}}},
			toDelete: []string{"ing1"},
		},
		"old instance desc should not update newer instance desc": {
			r1:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr_new", Tokens: []uint32{1, 2, 3}, Timestamp: int64(1000)}}},
			r2:       &Desc{Ingesters: map[string]InstanceDesc{"ing1": {Addr: "addr_old", Tokens: []uint32{1, 2, 3}, Timestamp: int64(900)}}},
			toUpdate: NewDesc(),
			toDelete: []string{},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			toUpdate, toDelete, err := testData.r1.FindDifference(testData.r2)
			assert.Equal(t, testData.toUpdate, toUpdate)
			assert.Equal(t, testData.toDelete, toDelete)
			assert.NoError(t, err)
		})
	}
}

func Test_resolveConflicts(t *testing.T) {
	tests := []struct {
		name string
		args map[string]InstanceDesc
		want map[string]InstanceDesc
	}{
		{
			name: "Empty input",
			args: map[string]InstanceDesc{},
			want: map[string]InstanceDesc{},
		},
		{
			name: "No conflicts",
			args: map[string]InstanceDesc{
				"ing1": {State: ACTIVE, Tokens: []uint32{1, 2, 3}},
				"ing2": {State: ACTIVE, Tokens: []uint32{4, 5, 6}},
			},
			want: map[string]InstanceDesc{
				"ing1": {State: ACTIVE, Tokens: []uint32{1, 2, 3}},
				"ing2": {State: ACTIVE, Tokens: []uint32{4, 5, 6}},
			},
		},
		{
			name: "Conflict resolution with LEFT state",
			args: map[string]InstanceDesc{
				"ing1": {State: ACTIVE, Tokens: []uint32{1, 2, 3}},
				"ing2": {State: LEFT, Tokens: []uint32{1, 2, 3, 4}},
			},
			want: map[string]InstanceDesc{
				"ing1": {State: ACTIVE, Tokens: []uint32{1, 2, 3}},
				"ing2": {State: LEFT, Tokens: nil},
			},
		},
		{
			name: "Conflict resolution with LEAVING state",
			args: map[string]InstanceDesc{
				"ing1": {State: LEAVING, Tokens: []uint32{1, 2, 3}},
				"ing2": {State: PENDING, Tokens: []uint32{1, 2, 3, 4}},
			},
			want: map[string]InstanceDesc{
				"ing1": {State: LEAVING, Tokens: nil},
				"ing2": {State: PENDING, Tokens: []uint32{1, 2, 3, 4}},
			},
		},
		{
			name: "Conflict resolution with JOINING state",
			args: map[string]InstanceDesc{
				"ing1": {State: ACTIVE, Tokens: []uint32{1, 2, 3}},
				"ing2": {State: JOINING, Tokens: []uint32{1, 2, 3, 4}},
			},
			want: map[string]InstanceDesc{
				"ing1": {State: ACTIVE, Tokens: []uint32{1, 2, 3}},
				"ing2": {State: JOINING, Tokens: []uint32{4}},
			},
		},
		{
			name: "Conflict resolution with same state",
			args: map[string]InstanceDesc{
				"ing1": {State: ACTIVE, Tokens: []uint32{1, 2, 3}},
				"ing2": {State: ACTIVE, Tokens: []uint32{1, 2, 3, 4}},
			},
			want: map[string]InstanceDesc{
				"ing1": {State: ACTIVE, Tokens: []uint32{1, 2, 3}},
				"ing2": {State: ACTIVE, Tokens: []uint32{4}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolveConflicts(tt.args)
			for key, actualInstance := range tt.args {
				assert.Equal(t, actualInstance, tt.want[key])
			}
		})
	}
}
