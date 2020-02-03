package ring

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNormalizationAndConflictResolution(t *testing.T) {
	now := time.Now().Unix()

	first := &Desc{
		Ingesters: map[string]IngesterDesc{
			"Ing 1":   {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{50, 40, 40, 30}},
			"Ing 2":   {Addr: "addr2", Timestamp: 123456, State: LEAVING, Tokens: []uint32{100, 5, 5, 100, 100, 200, 20, 10}},
			"Ing 3":   {Addr: "addr3", Timestamp: now, State: LEFT, Tokens: []uint32{100, 200, 300}},
			"Ing 4":   {Addr: "addr4", Timestamp: now, State: LEAVING, Tokens: []uint32{30, 40, 50}},
			"Unknown": {Tokens: []uint32{100}},
		},
	}

	second := &Desc{
		Ingesters: map[string]IngesterDesc{
			"Unknown": {
				Timestamp: now + 10,
				Tokens:    []uint32{1000, 2000},
			},
		},
	}

	change, err := first.Merge(second, false)
	if err != nil {
		t.Fatal(err)
	}
	changeRing := (*Desc)(nil)
	if change != nil {
		changeRing = change.(*Desc)
	}

	assert.Equal(t, &Desc{
		Ingesters: map[string]IngesterDesc{
			"Ing 1":   {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
			"Ing 2":   {Addr: "addr2", Timestamp: 123456, State: LEAVING, Tokens: []uint32{5, 10, 20, 100, 200}},
			"Ing 3":   {Addr: "addr3", Timestamp: now, State: LEFT},
			"Ing 4":   {Addr: "addr4", Timestamp: now, State: LEAVING},
			"Unknown": {Timestamp: now + 10, Tokens: []uint32{1000, 2000}},
		},
	}, first)

	assert.Equal(t, &Desc{
		// change ring is always normalized, "Unknown" ingester has lost two tokens: 100 from first ring (because of second ring), and 1000 (conflict resolution)
		Ingesters: map[string]IngesterDesc{
			"Unknown": {Timestamp: now + 10, Tokens: []uint32{1000, 2000}},
		},
	}, changeRing)
}

func merge(ring1, ring2 *Desc) (*Desc, *Desc) {
	change, err := ring1.Merge(ring2, false)
	if err != nil {
		panic(err)
	}

	if change == nil {
		return ring1, nil
	}

	changeRing := change.(*Desc)
	return ring1, changeRing
}

func TestMerge(t *testing.T) {
	now := time.Now().Unix()

	firstRing := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now, State: JOINING, Tokens: []uint32{5, 10, 20, 100, 200}},
			},
		}
	}

	secondRing := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 3": {Addr: "addr3", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{150, 250, 350}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
			},
		}
	}

	thirdRing := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: LEAVING, Tokens: []uint32{30, 40, 50}},
				"Ing 3": {Addr: "addr3", Timestamp: now + 10, State: ACTIVE, Tokens: []uint32{150, 250, 350}},
			},
		}
	}

	expectedFirstSecondMerge := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
				"Ing 3": {Addr: "addr3", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{150, 250, 350}},
			},
		}
	}

	expectedFirstSecondThirdMerge := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: LEAVING, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
				"Ing 3": {Addr: "addr3", Timestamp: now + 10, State: ACTIVE, Tokens: []uint32{150, 250, 350}},
			},
		}
	}

	fourthRing := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: LEFT, Tokens: []uint32{30, 40, 50}},
			},
		}
	}

	expectedFirstSecondThirdFourthMerge := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: LEFT, Tokens: nil},
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
				"Ing 3": {Addr: "addr3", Timestamp: now + 10, State: ACTIVE, Tokens: []uint32{150, 250, 350}},
			},
		}
	}

	{
		our, ch := merge(firstRing(), secondRing())
		assert.Equal(t, expectedFirstSecondMerge(), our)
		assert.Equal(t, secondRing(), ch) // entire second ring is new
	}

	{ // idempotency: (no change after applying same ring again)
		our, ch := merge(expectedFirstSecondMerge(), secondRing())
		assert.Equal(t, expectedFirstSecondMerge(), our)
		assert.Equal(t, (*Desc)(nil), ch)
	}

	{ // commutativity: Merge(first, second) == Merge(second, first)
		our, ch := merge(secondRing(), firstRing())
		assert.Equal(t, expectedFirstSecondMerge(), our)
		// when merging first into second ring, only "Ing 1" is new
		assert.Equal(t, &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
			},
		}, ch)
	}

	{ // associativity: Merge(Merge(first, second), third) == Merge(first, Merge(second, third))
		our1, _ := merge(firstRing(), secondRing())
		our1, _ = merge(our1, thirdRing())
		assert.Equal(t, expectedFirstSecondThirdMerge(), our1)

		our2, _ := merge(secondRing(), thirdRing())
		our2, _ = merge(our2, firstRing())
		assert.Equal(t, expectedFirstSecondThirdMerge(), our2)
	}

	{
		out, ch := merge(expectedFirstSecondThirdMerge(), fourthRing())
		assert.Equal(t, expectedFirstSecondThirdFourthMerge(), out)
		// entire fourth ring is the update -- but without tokens
		assert.Equal(t, &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: LEFT, Tokens: nil},
			},
		}, ch)
	}
}

func TestTokensTakeover(t *testing.T) {
	now := time.Now().Unix()

	first := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now, State: JOINING, Tokens: []uint32{5, 10, 20}}, // partially migrated from Ing 3
			},
		}
	}

	second := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20}},
				"Ing 3": {Addr: "addr3", Timestamp: now + 5, State: LEAVING, Tokens: []uint32{5, 10, 20, 100, 200}},
			},
		}
	}

	merged := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20}},
				"Ing 3": {Addr: "addr3", Timestamp: now + 5, State: LEAVING, Tokens: []uint32{100, 200}},
			},
		}
	}

	{
		our, ch := merge(first(), second())
		assert.Equal(t, merged(), our)
		assert.Equal(t, &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20}},
				"Ing 3": {Addr: "addr3", Timestamp: now + 5, State: LEAVING, Tokens: []uint32{100, 200}}, // change doesn't contain conflicted tokens
			},
		}, ch)
	}

	{ // idempotency: (no change after applying same ring again)
		our, ch := merge(merged(), second())
		assert.Equal(t, merged(), our)
		assert.Equal(t, (*Desc)(nil), ch)
	}

	{ // commutativity: (Merge(first, second) == Merge(second, first)
		our, ch := merge(second(), first())
		assert.Equal(t, merged(), our)

		// change is different though
		assert.Equal(t, &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
			},
		}, ch)
	}
}

func TestMergeLeft(t *testing.T) {
	now := time.Now().Unix()

	firstRing := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now, State: JOINING, Tokens: []uint32{5, 10, 20, 100, 200}},
			},
		}
	}

	secondRing := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 2": {Addr: "addr2", Timestamp: now, State: LEFT, Tokens: []uint32{5, 10, 20, 100, 200}},
			},
		}
	}

	expectedFirstSecondMerge := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now, State: LEFT},
			},
		}
	}

	thirdRing := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: LEAVING, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now, State: JOINING, Tokens: []uint32{5, 10, 20, 100, 200}}, // from firstRing
			},
		}
	}

	expectedFirstSecondThirdMerge := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: LEAVING, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now, State: LEFT},
			},
		}
	}

	{
		our, ch := merge(firstRing(), secondRing())
		assert.Equal(t, expectedFirstSecondMerge(), our)
		assert.Equal(t, &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 2": {Addr: "addr2", Timestamp: now, State: LEFT},
			},
		}, ch)
	}

	{ // idempotency: (no change after applying same ring again)
		our, ch := merge(expectedFirstSecondMerge(), secondRing())
		assert.Equal(t, expectedFirstSecondMerge(), our)
		assert.Equal(t, (*Desc)(nil), ch)
	}

	{ // commutativity: Merge(first, second) == Merge(second, first)
		our, ch := merge(secondRing(), firstRing())
		assert.Equal(t, expectedFirstSecondMerge(), our)
		// when merging first into second ring, only "Ing 1" is new
		assert.Equal(t, &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
			},
		}, ch)
	}

	{ // associativity: Merge(Merge(first, second), third) == Merge(first, Merge(second, third))
		our1, _ := merge(firstRing(), secondRing())
		our1, _ = merge(our1, thirdRing())
		assert.Equal(t, expectedFirstSecondThirdMerge(), our1)

		our2, _ := merge(secondRing(), thirdRing())
		our2, _ = merge(our2, firstRing())
		assert.Equal(t, expectedFirstSecondThirdMerge(), our2)
	}
}

func TestMergeRemoveMissing(t *testing.T) {
	now := time.Now().Unix()

	firstRing := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now, State: JOINING, Tokens: []uint32{5, 10, 20, 100, 200}},
				"Ing 3": {Addr: "addr3", Timestamp: now, State: LEAVING, Tokens: []uint32{5, 10, 20, 100, 200}},
			},
		}
	}

	secondRing := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
			},
		}
	}

	expectedFirstSecondMerge := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
				"Ing 3": {Addr: "addr3", Timestamp: now, State: LEFT},
			},
		}
	}

	{
		our, ch := mergeLocalCAS(firstRing(), secondRing())
		assert.Equal(t, expectedFirstSecondMerge(), our)
		assert.Equal(t, &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
				"Ing 3": {Addr: "addr3", Timestamp: now, State: LEFT},
			},
		}, ch) // entire second ring is new
	}

	{ // idempotency: (no change after applying same ring again)
		our, ch := mergeLocalCAS(expectedFirstSecondMerge(), secondRing())
		assert.Equal(t, expectedFirstSecondMerge(), our)
		assert.Equal(t, (*Desc)(nil), ch)
	}

	{ // commutativity is broken when deleting missing entries. But let's make sure we get reasonable results at least.
		our, ch := mergeLocalCAS(secondRing(), firstRing())
		assert.Equal(t, &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
				"Ing 3": {Addr: "addr3", Timestamp: now, State: LEAVING},
			},
		}, our)

		assert.Equal(t, &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 3": {Addr: "addr3", Timestamp: now, State: LEAVING},
			},
		}, ch)
	}
}

func TestMergeMissingIntoLeft(t *testing.T) {
	now := time.Now().Unix()

	ring1 := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
				"Ing 3": {Addr: "addr3", Timestamp: now, State: LEFT},
			},
		}
	}

	ring2 := func() *Desc {
		return &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 10, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
			},
		}
	}

	{
		our, ch := mergeLocalCAS(ring1(), ring2())
		assert.Equal(t, &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 10, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
				"Ing 3": {Addr: "addr3", Timestamp: now, State: LEFT},
			},
		}, our)

		assert.Equal(t, &Desc{
			Ingesters: map[string]IngesterDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 10, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
				// Ing 3 is not changed, it was already LEFT
			},
		}, ch)
	}
}

func mergeLocalCAS(ring1, ring2 *Desc) (*Desc, *Desc) {
	change, err := ring1.Merge(ring2, true)
	if err != nil {
		panic(err)
	}

	if change == nil {
		return ring1, nil
	}

	changeRing := change.(*Desc)
	return ring1, changeRing
}
