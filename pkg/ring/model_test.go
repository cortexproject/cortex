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
			actual := testData.ingester.IsHealthy(Write, testData.timeout)
			assert.Equal(t, testData.writeExpected, actual)

			actual = testData.ingester.IsHealthy(Read, testData.timeout)
			assert.Equal(t, testData.readExpected, actual)

			actual = testData.ingester.IsHealthy(Reporting, testData.timeout)
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
			actual := testData.instance.IsHealthy(BlocksSync, testData.timeout)
			assert.Equal(t, testData.syncExpected, actual)

			actual = testData.instance.IsHealthy(BlocksRead, testData.timeout)
			assert.Equal(t, testData.queryExpected, actual)
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
		expected map[string][]TokenDesc
	}{
		"empty ring": {
			desc:     &Desc{Ingesters: map[string]IngesterDesc{}},
			expected: map[string][]TokenDesc{},
		},
		"single zone": {
			desc: &Desc{Ingesters: map[string]IngesterDesc{
				"instance-1": {Addr: "127.0.0.1", Tokens: []uint32{1, 5}, Zone: ""},
				"instance-2": {Addr: "127.0.0.1", Tokens: []uint32{2, 4}, Zone: ""},
				"instance-3": {Addr: "127.0.0.1", Tokens: []uint32{3, 6}, Zone: ""},
			}},
			expected: map[string][]TokenDesc{
				"": {
					{Token: 1, Ingester: "instance-1", Zone: ""},
					{Token: 2, Ingester: "instance-2", Zone: ""},
					{Token: 3, Ingester: "instance-3", Zone: ""},
					{Token: 4, Ingester: "instance-2", Zone: ""},
					{Token: 5, Ingester: "instance-1", Zone: ""},
					{Token: 6, Ingester: "instance-3", Zone: ""},
				},
			},
		},
		"multiple zones": {
			desc: &Desc{Ingesters: map[string]IngesterDesc{
				"instance-1": {Addr: "127.0.0.1", Tokens: []uint32{1, 5}, Zone: "zone-1"},
				"instance-2": {Addr: "127.0.0.1", Tokens: []uint32{2, 4}, Zone: "zone-1"},
				"instance-3": {Addr: "127.0.0.1", Tokens: []uint32{3, 6}, Zone: "zone-2"},
			}},
			expected: map[string][]TokenDesc{
				"zone-1": {
					{Token: 1, Ingester: "instance-1", Zone: "zone-1"},
					{Token: 2, Ingester: "instance-2", Zone: "zone-1"},
					{Token: 4, Ingester: "instance-2", Zone: "zone-1"},
					{Token: 5, Ingester: "instance-1", Zone: "zone-1"},
				},
				"zone-2": {
					{Token: 3, Ingester: "instance-3", Zone: "zone-2"},
					{Token: 6, Ingester: "instance-3", Zone: "zone-2"},
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.desc.getTokensByZone())
		})
	}
}
