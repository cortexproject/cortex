package ring

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGenerateTokens(t *testing.T) {
	testCase := map[string]struct {
		tg TokenGenerator
	}{
		"random": {
			tg: NewRandomTokenGenerator(),
		},
		"minimizeSpread": {
			tg: NewMinimizeSpreadTokenGenerator(),
		},
	}

	for name, tc := range testCase {
		t.Run(name, func(t *testing.T) {
			tokens := tc.tg.GenerateTokens(NewDesc(), "", "", 1000000, true)

			dups := make(map[uint32]int)

			for ix, v := range tokens {
				if ox, ok := dups[v]; ok {
					t.Errorf("Found duplicate token %d, tokens[%d]=%d, tokens[%d]=%d", v, ix, tokens[ix], ox, tokens[ox])
				} else {
					dups[v] = ix
				}
			}
		})
	}
}

func TestGenerateTokens_IgnoresOldTokens(t *testing.T) {
	testCase := map[string]struct {
		tg TokenGenerator
	}{
		"random": {
			tg: NewRandomTokenGenerator(),
		},
		"minimizeSpread": {
			tg: NewMinimizeSpreadTokenGenerator(),
		},
	}

	for name, c := range testCase {
		tc := c
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			d := NewDesc()
			dups := make(map[uint32]bool)

			for i := 0; i < 500; i++ {
				id := strconv.Itoa(i)
				zone := strconv.Itoa(i % 3)
				tokens := tc.tg.GenerateTokens(d, id, zone, 500, true)
				d.AddIngester(id, id, zone, tokens, ACTIVE, time.Now())
				for _, v := range tokens {
					if dups[v] {
						t.Fatal("GenerateTokens returned old token")
					}
					dups[v] = true
				}
			}
		})
	}
}

func TestMinimizeSpreadTokenGenerator(t *testing.T) {
	rindDesc := NewDesc()
	zones := []string{"zone1", "zone2", "zone3"}

	mTokenGenerator := newMockedTokenGenerator(len(zones))
	minimizeTokenGenerator := &MinimizeSpreadTokenGenerator{
		innerGenerator: mTokenGenerator,
	}

	dups := map[uint32]bool{}

	// First time we should generate tokens using the inner generator
	generateTokensForIngesters(t, rindDesc, "initial", zones, minimizeTokenGenerator, dups)
	require.Equal(t, mTokenGenerator.called, len(zones))

	// Should Generate tokens based on the ring state
	for i := 0; i < 50; i++ {
		generateTokensForIngesters(t, rindDesc, fmt.Sprintf("minimize-%v", i), zones, minimizeTokenGenerator, dups)
		assertDistancePerIngester(t, rindDesc, 0.01)
	}
	require.Equal(t, mTokenGenerator.called, len(zones))

	mTokenGenerator.called = 0
	// Should fallback to random generator when more than 1 ingester does not have tokens and force flag is set
	rindDesc.AddIngester("pendingIngester-1", "pendingIngester-1", zones[0], []uint32{}, PENDING, time.Now())
	rindDesc.AddIngester("pendingIngester-2", "pendingIngester-2", zones[0], []uint32{}, PENDING, time.Now().Add(-10*time.Minute))
	tokens := minimizeTokenGenerator.GenerateTokens(rindDesc, "pendingIngester-1", zones[0], 512, true)
	require.Len(t, tokens, 512)
	require.Equal(t, mTokenGenerator.called, 1)

	// Should generate for the ingester with with the smaller registered
	tokens = minimizeTokenGenerator.GenerateTokens(rindDesc, "pendingIngester-2", zones[0], 512, false)
	require.Len(t, tokens, 512)
	require.Equal(t, mTokenGenerator.called, 1)

	// Should generate tokens on other AZs
	rindDesc.AddIngester("pendingIngester-1-az-2", "pendingIngester-1-az-2", zones[0], []uint32{}, PENDING, time.Now())
	tokens = minimizeTokenGenerator.GenerateTokens(rindDesc, "pendingIngester-1-az-2", zones[1], 512, false)
	require.Len(t, tokens, 512)
	require.Equal(t, mTokenGenerator.called, 1)

	// Should generate tokens only for the ingesters with the smaller registered time when multiples
	// ingesters does not have tokens
	tokens = minimizeTokenGenerator.GenerateTokens(rindDesc, "pendingIngester-1", zones[0], 512, false)
	require.Len(t, tokens, 0)
	tokens = minimizeTokenGenerator.GenerateTokens(rindDesc, "pendingIngester-2", zones[0], 512, false)
	require.Len(t, tokens, 512)
}

func generateTokensForIngesters(t *testing.T, rindDesc *Desc, prefix string, zones []string, minimizeTokenGenerator *MinimizeSpreadTokenGenerator, dups map[uint32]bool) {
	for _, zone := range zones {
		id := fmt.Sprintf("%v-%v", prefix, zone)
		tokens := minimizeTokenGenerator.GenerateTokens(rindDesc, id, zone, 512, true)
		for _, token := range tokens {
			if dups[token] {
				t.Fatal("GenerateTokens returned duplicated tokens")
			}
			dups[token] = true
		}
		require.Len(t, tokens, 512)
		rindDesc.AddIngester(id, id, zone, tokens, ACTIVE, time.Now())
	}
}

type mockedTokenGenerator struct {
	totalZones int
	zones      map[string]int

	called int
	RandomTokenGenerator
}

func (m *mockedTokenGenerator) GenerateTokens(d *Desc, id, zone string, numTokens int, force bool) []uint32 {
	m.called++
	return m.RandomTokenGenerator.GenerateTokens(d, id, zone, numTokens, force)
}

func newMockedTokenGenerator(totalZones int) *mockedTokenGenerator {
	return &mockedTokenGenerator{totalZones: totalZones, zones: make(map[string]int)}
}

func assertDistancePerIngester(t testing.TB, d *Desc, tolerance float64) {
	r := make(map[string]int64, len(d.Ingesters))
	tokensPerAz := d.getTokensByZone()
	numberOfIngesterPerAz := map[string]int{}

	for s, desc := range d.Ingesters {
		numberOfIngesterPerAz[desc.Zone]++
		for _, token := range desc.Tokens {
			index, ok := slices.BinarySearch(tokensPerAz[desc.Zone], token)
			if !ok {
				t.Fatal("token not found")
			}
			prev := index - 1
			if prev < 0 {
				prev = len(tokensPerAz[desc.Zone]) - 1
			}
			r[s] += tokenDistance(tokensPerAz[desc.Zone][prev], tokensPerAz[desc.Zone][index])
		}
	}

	for s, desc := range d.Ingesters {
		expectedDistance := float64(maxTokenValue / numberOfIngesterPerAz[desc.Zone])
		realDistance := float64(r[s])
		require.Condition(t, func() (success bool) {
			return (1 - math.Abs(expectedDistance/realDistance)) < tolerance
		}, "[%v] expected and real distance error is greater than %v -> %v[%v/%v]", s, tolerance, 1-math.Abs(expectedDistance/realDistance), expectedDistance, realDistance)
	}
}
