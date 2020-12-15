package ring

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIngesterDesc_IsHealthy_ForIngesterOperations(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		ingester       *IngesterDesc
		timeout        time.Duration
		writeExpected  bool
		readExpected   bool
		reportExpected bool
	}{
		"ACTIVE ingester with last keepalive newer than timeout": {
			ingester:       &IngesterDesc{State: ACTIVE, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:        time.Minute,
			writeExpected:  true,
			readExpected:   true,
			reportExpected: true,
		},
		"ACTIVE ingester with last keepalive older than timeout": {
			ingester:       &IngesterDesc{State: ACTIVE, Timestamp: time.Now().Add(-90 * time.Second).Unix()},
			timeout:        time.Minute,
			writeExpected:  false,
			readExpected:   false,
			reportExpected: false,
		},
		"JOINING ingester with last keepalive newer than timeout": {
			ingester:       &IngesterDesc{State: JOINING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:        time.Minute,
			writeExpected:  false,
			readExpected:   false,
			reportExpected: true,
		},
		"LEAVING ingester with last keepalive newer than timeout": {
			ingester:       &IngesterDesc{State: LEAVING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
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

func TestIngesterDesc_IsHealthy_ForStoreGatewayOperations(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		instance      *IngesterDesc
		timeout       time.Duration
		syncExpected  bool
		queryExpected bool
	}{
		"ACTIVE instance with last keepalive newer than timeout": {
			instance:      &IngesterDesc{State: ACTIVE, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:       time.Minute,
			syncExpected:  true,
			queryExpected: true,
		},
		"ACTIVE instance with last keepalive older than timeout": {
			instance:      &IngesterDesc{State: ACTIVE, Timestamp: time.Now().Add(-90 * time.Second).Unix()},
			timeout:       time.Minute,
			syncExpected:  false,
			queryExpected: false,
		},
		"JOINING instance with last keepalive newer than timeout": {
			instance:      &IngesterDesc{State: JOINING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:       time.Minute,
			syncExpected:  true,
			queryExpected: false,
		},
		"LEAVING instance with last keepalive newer than timeout": {
			instance:      &IngesterDesc{State: LEAVING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:       time.Minute,
			syncExpected:  true,
			queryExpected: false,
		},
		"PENDING instance with last keepalive newer than timeout": {
			instance:      &IngesterDesc{State: PENDING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:       time.Minute,
			syncExpected:  false,
			queryExpected: false,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			actual := testData.instance.IsHealthy(BlocksSync, testData.timeout, time.Now())
			assert.Equal(t, testData.syncExpected, actual)

			actual = testData.instance.IsHealthy(BlocksRead, testData.timeout, time.Now())
			assert.Equal(t, testData.queryExpected, actual)
		})
	}
}

func TestIngesterDesc_GetRegisteredAt(t *testing.T) {
	tests := map[string]struct {
		desc     *IngesterDesc
		expected time.Time
	}{
		"should return zero value on nil desc": {
			desc:     nil,
			expected: time.Time{},
		},
		"should return zero value registered timestamp = 0": {
			desc: &IngesterDesc{
				RegisteredTimestamp: 0,
			},
			expected: time.Time{},
		},
		"should return timestamp parsed from desc": {
			desc: &IngesterDesc{
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

func normalizedSource() *Desc {
	r := NewDesc()
	r.Ingesters["first"] = IngesterDesc{
		Tokens: []uint32{100, 200, 300},
	}
	r.Ingesters["second"] = IngesterDesc{}
	return r
}

func normalizedOutput() *Desc {
	return &Desc{
		Ingesters: map[string]IngesterDesc{
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
		Ingesters: map[string]IngesterDesc{
			"ing1": {
				Tokens:    []uint32{100, 200, 300},
				State:     ACTIVE,
				Timestamp: now.Unix(),
			},
		},
	}

	if err := r.Ready(now, 10*time.Second); err != nil {
		t.Fatal("expected ready, got", err)
	}

	if err := r.Ready(now.Add(5*time.Minute), 10*time.Second); err == nil {
		t.Fatal("expected !ready (no heartbeat from active ingester), but got no error")
	}

	r = &Desc{
		Ingesters: map[string]IngesterDesc{
			"ing1": {
				State:     ACTIVE,
				Timestamp: now.Unix(),
			},
		},
	}

	if err := r.Ready(now, 10*time.Second); err == nil {
		t.Fatal("expected !ready (no tokens), but got no error")
	}

	r.Ingesters["some ingester"] = IngesterDesc{
		Tokens:    []uint32{12345},
		Timestamp: now.Unix(),
	}

	if err := r.Ready(now, 10*time.Second); err != nil {
		t.Fatal("expected ready, got", err)
	}
}

func TestDesc_getTokensByZone(t *testing.T) {
	tests := map[string]struct {
		desc     *Desc
		expected map[string][]uint32
	}{
		"empty ring": {
			desc:     &Desc{Ingesters: map[string]IngesterDesc{}},
			expected: map[string][]uint32{},
		},
		"single zone": {
			desc: &Desc{Ingesters: map[string]IngesterDesc{
				"instance-1": {Addr: "127.0.0.1", Tokens: []uint32{1, 5}, Zone: ""},
				"instance-2": {Addr: "127.0.0.1", Tokens: []uint32{2, 4}, Zone: ""},
				"instance-3": {Addr: "127.0.0.1", Tokens: []uint32{3, 6}, Zone: ""},
			}},
			expected: map[string][]uint32{
				"": {1, 2, 3, 4, 5, 6},
			},
		},
		"multiple zones": {
			desc: &Desc{Ingesters: map[string]IngesterDesc{
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
			desc:         &Desc{Ingesters: map[string]IngesterDesc{}},
			expectedMine: Tokens(nil),
			expectedAll:  Tokens{},
		},
		"single zone": {
			desc: &Desc{Ingesters: map[string]IngesterDesc{
				"instance-1": {Addr: "127.0.0.1", Tokens: []uint32{1, 5}, Zone: ""},
				"instance-2": {Addr: "127.0.0.1", Tokens: []uint32{2, 4}, Zone: ""},
				"instance-3": {Addr: "127.0.0.1", Tokens: []uint32{3, 6}, Zone: ""},
			}},
			expectedMine: Tokens{1, 5},
			expectedAll:  Tokens{1, 2, 3, 4, 5, 6},
		},
		"multiple zones": {
			desc: &Desc{Ingesters: map[string]IngesterDesc{
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
			r2:       &Desc{Ingesters: map[string]IngesterDesc{}},
			expected: Equal,
		},
		"two empty rings": {
			r1:       &Desc{Ingesters: map[string]IngesterDesc{}},
			r2:       &Desc{Ingesters: map[string]IngesterDesc{}},
			expected: Equal,
		},
		"same single instance": {
			r1:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr1"}}},
			r2:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr1"}}},
			expected: Equal,
		},
		"same single instance, different timestamp": {
			r1:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr1", Timestamp: 123456}}},
			r2:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr1", Timestamp: 789012}}},
			expected: EqualButStatesAndTimestamps,
		},
		"same single instance, different state": {
			r1:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr1", State: ACTIVE}}},
			r2:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr1", State: JOINING}}},
			expected: EqualButStatesAndTimestamps,
		},
		"same single instance, different registered timestamp": {
			r1:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr1", State: ACTIVE, RegisteredTimestamp: 1}}},
			r2:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr1", State: ACTIVE, RegisteredTimestamp: 2}}},
			expected: Different,
		},
		"instance in different zone": {
			r1:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr1", Zone: "one"}}},
			r2:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr1", Zone: "two"}}},
			expected: Different,
		},
		"same instance, different address": {
			r1:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr1"}}},
			r2:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr2"}}},
			expected: Different,
		},
		"more instances in one ring": {
			r1:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr1"}, "ing2": {Addr: "ing2"}}},
			r2:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr1"}}},
			expected: Different,
		},
		"different tokens": {
			r1:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr1", Tokens: []uint32{1, 2, 3}}}},
			r2:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr1"}}},
			expected: Different,
		},
		"different tokens 2": {
			r1:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr1", Tokens: []uint32{1, 2, 3}}}},
			r2:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr1", Tokens: []uint32{1, 2, 4}}}},
			expected: Different,
		},
		"same number of instances, using different IDs": {
			r1:       &Desc{Ingesters: map[string]IngesterDesc{"ing1": {Addr: "addr1", Tokens: []uint32{1, 2, 3}}}},
			r2:       &Desc{Ingesters: map[string]IngesterDesc{"ing2": {Addr: "addr1", Tokens: []uint32{1, 2, 3}}}},
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
