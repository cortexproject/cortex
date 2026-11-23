package ring

import (
	"cmp"
	"fmt"
	"math"
	"math/rand"
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

			for i := range 500 {
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
	for i := range 50 {
		generateTokensForIngesters(t, rindDesc, fmt.Sprintf("minimize-%v", i), zones, minimizeTokenGenerator, dups)
		assertDistancePerIngester(t, rindDesc, 0.02)
	}
	require.Equal(t, mTokenGenerator.called, len(zones))

	// Test generating tokens for an ingester that already have tokens in the ring
	rg := NewRandomTokenGenerator()
	rTokens := rg.GenerateTokens(rindDesc, "partial", zones[0], 256, true)
	rindDesc.AddIngester("partial", "partial", zones[0], rTokens, ACTIVE, time.Now())
	nTokens := minimizeTokenGenerator.GenerateTokens(rindDesc, "partial", zones[0], 256, true)
	rindDesc.AddIngester("partial", "partial", zones[0], append(rTokens, nTokens...), ACTIVE, time.Now())
	assertDistancePerIngester(t, rindDesc, 0.02)

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
	// Lets remove this pending ingester to no interfere with the next tests
	rindDesc.RemoveIngester("pendingIngester-1-az-2")

	// Should generate tokens only for the ingesters with the smaller registered time when multiples
	// ingesters does not have tokens
	tokens = minimizeTokenGenerator.GenerateTokens(rindDesc, "pendingIngester-1", zones[0], 512, false)
	require.Len(t, tokens, 0)
	tokens = minimizeTokenGenerator.GenerateTokens(rindDesc, "pendingIngester-2", zones[0], 512, false)
	require.Len(t, tokens, 512)

	// Should generate tokens only for the ingesters with the smaller Id when multiples ingesters does not have tokens
	// and have the same registered time
	now := time.Now()
	rindDesc.AddIngester("pendingIngester-1", "pendingIngester-1", zones[0], []uint32{}, PENDING, now)
	rindDesc.AddIngester("pendingIngester-2", "pendingIngester-2", zones[0], []uint32{}, PENDING, now)
	tokens = minimizeTokenGenerator.GenerateTokens(rindDesc, "pendingIngester-1", zones[0], 512, false)
	require.Len(t, tokens, 512)
	tokens = minimizeTokenGenerator.GenerateTokens(rindDesc, "pendingIngester-2", zones[0], 512, false)
	require.Len(t, tokens, 0)
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

func TestMinimizeSpreadTokenGenerator_IgnoresReadOnlyIngesters(t *testing.T) {
	const (
		zone              = "zone1"
		numActive         = 20
		numReadOnly       = 10
		tokensPerIngester = 128
		tokensToRenew     = tokensPerIngester / 10
	)

	rnd := rand.New(rand.NewSource(1))
	seeded := &seededTokenGenerator{rnd: rnd}
	generator := &MinimizeSpreadTokenGenerator{innerGenerator: seeded}
	registeredAt := time.Unix(1700000000, 0)

	// Build an unbalanced ring from random tokens, then mark the last ingesters READONLY as if scaling down.
	ringDesc := NewDesc()
	for i := range numActive + numReadOnly {
		id := fmt.Sprintf("ingester-%02d", i)
		state := ACTIVE
		if i >= numActive {
			state = READONLY
		}
		ringDesc.AddIngester(id, id, zone, seeded.GenerateTokens(ringDesc, id, zone, tokensPerIngester, true), state, registeredAt)
	}

	readOnlyTokensBefore := map[string][]uint32{}
	for id, ing := range ringDesc.Ingesters {
		if ing.State == READONLY {
			readOnlyTokensBefore[id] = slices.Clone(ing.Tokens)
		}
	}

	// The random ring must start unbalanced, or the assertion below proves nothing.
	require.Greater(t, maxWriteOwnershipError(ringDesc), 0.05)

	// Renew 10% of the tokens of each ACTIVE ingester, the same way Lifecycler.RenewTokens does.
	for i := range numActive {
		id := fmt.Sprintf("ingester-%02d", i)
		ing := ringDesc.Ingesters[id]

		kept := slices.Clone(ing.Tokens)
		for range tokensToRenew {
			idx := rnd.Intn(len(kept))
			kept = append(kept[:idx], kept[idx+1:]...)
		}
		ringDesc.AddIngester(id, id, zone, kept, ACTIVE, registeredAt)

		newTokens := generator.GenerateTokens(ringDesc, id, zone, tokensToRenew, true)
		require.Len(t, newTokens, tokensToRenew)

		tokens := append(kept, newTokens...)
		slices.Sort(tokens)
		require.Len(t, slices.Compact(slices.Clone(tokens)), tokensPerIngester, "duplicated tokens for %s", id)
		ringDesc.AddIngester(id, id, zone, tokens, ACTIVE, registeredAt)
	}

	// READONLY ingesters keep their tokens, and no token is shared between ingesters.
	for id, tokens := range readOnlyTokensBefore {
		require.Equal(t, tokens, ringDesc.Ingesters[id].Tokens)
	}
	allTokens := ringDesc.GetTokens()
	require.Len(t, slices.Compact(slices.Clone(allTokens)), len(allTokens))

	// Writes skip READONLY ingesters, so the write ownership of the ACTIVE ingesters must be balanced.
	require.Less(t, maxWriteOwnershipError(ringDesc), 0.02)
}

// maxWriteOwnershipError returns the largest relative difference between an ACTIVE ingester's write
// ownership and its expected share. Write ownership is computed from ACTIVE tokens only, because a
// READONLY ingester's ranges are written to the next ACTIVE ingester on the ring.
func maxWriteOwnershipError(d *Desc) float64 {
	type tokenOwner struct {
		token uint32
		id    string
	}
	var tokens []tokenOwner
	for id, ing := range d.Ingesters {
		if ing.State != ACTIVE {
			continue
		}
		for _, token := range ing.Tokens {
			tokens = append(tokens, tokenOwner{token: token, id: id})
		}
	}
	slices.SortFunc(tokens, func(a, b tokenOwner) int { return cmp.Compare(a.token, b.token) })

	owned := map[string]int64{}
	for i := 1; i <= len(tokens); i++ {
		curr := tokens[i%len(tokens)]
		owned[curr.id] += tokenDistance(tokens[i-1].token, curr.token)
	}

	expected := float64(maxTokenValue+1) / float64(len(owned))
	maxErr := 0.0
	for _, o := range owned {
		maxErr = math.Max(maxErr, math.Abs(float64(o)-expected)/expected)
	}
	return maxErr
}

// seededTokenGenerator generates random tokens from a fixed source so tests are deterministic.
type seededTokenGenerator struct {
	rnd *rand.Rand
}

func (g *seededTokenGenerator) GenerateTokens(d *Desc, _, _ string, numTokens int, _ bool) []uint32 {
	used := map[uint32]bool{}
	for _, token := range d.GetTokens() {
		used[token] = true
	}

	tokens := make([]uint32, 0, numTokens)
	for len(tokens) < numTokens {
		candidate := g.rnd.Uint32()
		if used[candidate] {
			continue
		}
		used[candidate] = true
		tokens = append(tokens, candidate)
	}
	slices.Sort(tokens)
	return tokens
}

func TestMinimizeSpreadTokenGenerator_TokenlessDonorAndCollision(t *testing.T) {
	const zone = "zone1"
	registeredAt := time.Unix(1700000000, 0)

	// With one token, the ACTIVE ingester owns the whole ring, and the first split of its range lands at
	// 1000 + expectedOwnershipDistance. Expected ownership is 1/3: the ACTIVE ingester, the ingester joining
	// concurrently, and the one generating tokens. A READONLY ingester holds exactly that token.
	expectedOwnershipDistance := int64(float64(1) / 3 * maxTokenValue)
	readOnlyToken := uint32(1000 + expectedOwnershipDistance)

	ringDesc := NewDesc()
	ringDesc.AddIngester("active", "active", zone, []uint32{1000}, ACTIVE, registeredAt)
	ringDesc.AddIngester("joining", "joining", zone, []uint32{}, PENDING, registeredAt)
	// Joins concurrently and registered later, so it's in the donor set without any tokens.
	ringDesc.AddIngester("concurrent", "concurrent", zone, []uint32{}, PENDING, registeredAt.Add(time.Second))
	ringDesc.AddIngester("readonly", "readonly", zone, []uint32{readOnlyToken}, READONLY, registeredAt)

	inner := newMockedTokenGenerator(1)
	generator := &MinimizeSpreadTokenGenerator{innerGenerator: inner}

	var tokens []uint32
	require.NotPanics(t, func() {
		tokens = generator.GenerateTokens(ringDesc, "joining", zone, 1, true)
	})
	require.Len(t, tokens, 1)
	// The collision must not discard the range: the token still comes from splitting it, not from the random fallback.
	require.Equal(t, 0, inner.called)
	// The token moves to the next free position instead of skipping the range up to the READONLY token.
	require.Equal(t, readOnlyToken+1, tokens[0])

	// Writes skip the READONLY ingester, so the joining ingester will own the range from the ACTIVE token to its new
	// token, which must be about its expected 1/3 of the ring.
	ownership := float64(tokenDistance(1000, tokens[0])) / float64(maxTokenValue+1)
	require.InDelta(t, 1.0/3, ownership, 0.001)
}
