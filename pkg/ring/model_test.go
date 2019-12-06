package ring

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIngesterDesc_IsHealthy(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		ingester       *IngesterDesc
		timeout        time.Duration
		writeExpected  bool
		readExpected   bool
		reportExpected bool
	}{
		"ALIVE ingester with last keepalive newer than timeout": {
			ingester:       &IngesterDesc{State: ACTIVE, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:        time.Minute,
			writeExpected:  true,
			readExpected:   true,
			reportExpected: true,
		},
		"ALIVE ingester with last keepalive older than timeout": {
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

func normalizedSource() *Desc {
	r := NewDesc()
	r.Ingesters["first"] = IngesterDesc{
		Tokens: []uint32{100, 200, 300},
	}
	r.Ingesters["second"] = IngesterDesc{}
	return r
}

func unnormalizedSource() *Desc {
	r := NewDesc()
	r.Ingesters["first"] = IngesterDesc{}
	r.Ingesters["second"] = IngesterDesc{}
	r.Tokens = []TokenDesc{
		{Token: 100, Ingester: "first"},
		{Token: 200, Ingester: "first"},
		{Token: 300, Ingester: "first"},
	}
	return r
}

func normalizedOutput() *Desc {
	return &Desc{
		Ingesters: map[string]IngesterDesc{
			"first":  {},
			"second": {Tokens: []uint32{100, 200, 300}},
		},
		Tokens: nil,
	}
}

func unnormalizedOutput() *Desc {
	return &Desc{
		Ingesters: map[string]IngesterDesc{
			"first":  {},
			"second": {},
		},
		Tokens: []TokenDesc{
			{Token: 100, Ingester: "second"},
			{Token: 200, Ingester: "second"},
			{Token: 300, Ingester: "second"},
		},
	}
}

func TestClaimTokensFromNormalizedToNormalized(t *testing.T) {
	r := normalizedSource()
	result := r.ClaimTokens("first", "second", true)

	assert.Equal(t, []uint32{100, 200, 300}, result)
	assert.Equal(t, normalizedOutput(), r)
}

func TestClaimTokensFromNormalizedToUnnormalized(t *testing.T) {
	r := normalizedSource()
	result := r.ClaimTokens("first", "second", false)

	assert.Equal(t, []uint32{100, 200, 300}, result)
	assert.Equal(t, unnormalizedOutput(), r)
}

func TestClaimTokensFromUnnormalizedToUnnormalized(t *testing.T) {
	r := unnormalizedSource()
	result := r.ClaimTokens("first", "second", false)

	assert.Equal(t, []uint32{100, 200, 300}, result)
	assert.Equal(t, unnormalizedOutput(), r)
}

func TestClaimTokensFromUnnormalizedToNormalized(t *testing.T) {
	r := unnormalizedSource()

	result := r.ClaimTokens("first", "second", true)

	assert.Equal(t, []uint32{100, 200, 300}, result)
	assert.Equal(t, normalizedOutput(), r)
}

func TestReady(t *testing.T) {
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

	r.Tokens = []TokenDesc{
		{Token: 12345, Ingester: "some ingester"},
	}

	if err := r.Ready(now, 10*time.Second); err != nil {
		t.Fatal("expected ready, got", err)
	}
}
