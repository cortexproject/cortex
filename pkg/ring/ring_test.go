package ring

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
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
		desc.AddIngester(fmt.Sprintf("%d", i), fmt.Sprintf("ingester%d", i), tokens, ACTIVE, false)
	}

	cfg := Config{}
	flagext.DefaultValues(&cfg)
	r := Ring{
		name:     "ingester",
		cfg:      cfg,
		ringDesc: desc,
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
		name:     "ingester",
		cfg:      Config{},
		ringDesc: desc,
	}
	require.Error(t, DoBatch(ctx, &r, keys, callback, cleanup))
}

func TestAddIngester(t *testing.T) {
	r := NewDesc()

	const ingName = "ing1"

	ing1Tokens := GenerateTokens(128, nil)

	r.AddIngester(ingName, "addr", ing1Tokens, ACTIVE, false)

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

	r.AddIngester(ing1Name, "addr", newTokens, ACTIVE, false)

	require.Equal(t, newTokens, r.Ingesters[ing1Name].Tokens)
}

func TestSubring(t *testing.T) {
	r := NewDesc()

	n := 16 // number of ingesters in ring
	var prevTokens []uint32
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("ing%v", i)
		ingTokens := GenerateTokens(128, prevTokens)

		r.AddIngester(name, fmt.Sprintf("addr%v", i), ingTokens, ACTIVE, false)

		prevTokens = append(prevTokens, ingTokens...)
	}

	// Create a ring with the ingesters
	ring := Ring{
		name: "main ring",
		cfg: Config{
			HeartbeatTimeout: time.Hour,
		},
		ringDesc:   r,
		ringTokens: r.GetNavigator(),
	}

	// Subring of 0 invalid
	_, err := ring.Subring(0, 0)
	require.Error(t, err)

	// Generate a sub ring for all possible valid ranges
	for i := 1; i < n+2; i++ {
		subr, err := ring.Subring(rand.Uint32(), i)
		require.NoError(t, err)
		subringSize := i
		if i > n {
			subringSize = n
		}
		require.Equal(t, subringSize, len(subr.(*Ring).ringDesc.Ingesters))
		require.Equal(t, subringSize*128, len(subr.(*Ring).ringTokens))
		require.True(t, sort.SliceIsSorted(subr.(*Ring).ringTokens, func(i, j int) bool {
			return subr.(*Ring).ringTokens[i].Token < subr.(*Ring).ringTokens[j].Token
		}))

		// Obtain a replication slice
		size := i - 1
		if size <= 0 {
			size = 1
		}
		subr.(*Ring).cfg.ReplicationFactor = size
		set, err := subr.Get(rand.Uint32(), Write, nil)
		require.NoError(t, err)
		require.Equal(t, size, len(set.Ingesters))
	}
}

func TestStableSubring(t *testing.T) {
	r := NewDesc()

	n := 16 // number of ingesters in ring
	var prevTokens []uint32
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("ing%v", i)
		ingTokens := GenerateTokens(128, prevTokens)

		r.AddIngester(name, fmt.Sprintf("addr%v", i), ingTokens, ACTIVE, false)

		prevTokens = append(prevTokens, ingTokens...)
	}

	// Create a ring with the ingesters
	ring := Ring{
		name: "main ring",
		cfg: Config{
			HeartbeatTimeout: time.Hour,
		},
		ringDesc:   r,
		ringTokens: r.GetNavigator(),
	}

	// Generate the same subring multiple times
	var subrings [][]TokenDesc
	key := rand.Uint32()
	subringsize := 4
	for i := 1; i < 4; i++ {
		subr, err := ring.Subring(key, subringsize)
		require.NoError(t, err)
		require.Equal(t, subringsize, len(subr.(*Ring).ringDesc.Ingesters))
		require.Equal(t, subringsize*128, len(subr.(*Ring).ringTokens))
		require.True(t, sort.SliceIsSorted(subr.(*Ring).ringTokens, func(i, j int) bool {
			return subr.(*Ring).ringTokens[i].Token < subr.(*Ring).ringTokens[j].Token
		}))

		subrings = append(subrings, subr.(*Ring).ringTokens)
	}

	// Validate that the same subring is produced each time from the same ring
	for i := 0; i < len(subrings); i++ {
		next := i + 1
		if next >= len(subrings) {
			next = 0
		}
		require.Equal(t, subrings[i], subrings[next])
	}
}

type namedRing struct {
	*Desc
	namedTokens map[string]uint32
	tokenStates map[uint32]IngesterState
}

func (r *namedRing) FindTokensByState(s IngesterState) []uint32 {
	var ret []uint32
	for t, state := range r.tokenStates {
		if state == s {
			ret = append(ret, t)
		}
	}
	return ret
}

func (r *namedRing) TokenHealthChecker(op Operation) HealthCheckFunc {
	return func(t TokenDesc) bool {
		state := ACTIVE
		if s, ok := r.tokenStates[t.Token]; ok {
			state = s
		}

		if op == Write && state != ACTIVE {
			return false
		} else if op == Read && state == JOINING {
			return false
		}

		return true
	}
}

// Name gets a token's name by its value
func (r *namedRing) TokenName(t *testing.T, value uint32) string {
	t.Helper()
	for n, v := range r.namedTokens {
		if v == value {
			return n
		}
	}
	t.Fatalf("could not find %d in ring", value)
	return ""
}

// Token gets a token by its name.
func (r *namedRing) Token(t *testing.T, name string) uint32 {
	t.Helper()
	v, ok := r.namedTokens[name]
	if !ok {
		t.Fatalf("no token named %s in ring", name)
	}
	return v
}

// Token gets a TokenDesc by its name.
func (r *namedRing) TokenDesc(t *testing.T, name string) TokenDesc {
	t.Helper()
	v := r.Token(t, name)
	for name, ing := range r.Desc.Ingesters {
		for _, tok := range ing.Tokens {
			if tok == v {
				return TokenDesc{Token: tok, Ingester: name}
			}
		}
	}
	t.Fatalf("could not find %s in ring", name)
	return TokenDesc{}
}

// BindStates binds token states to the ingesters.
func (r *namedRing) BindStates(t *testing.T) {
	for name, ing := range r.Desc.Ingesters {
		for _, tok := range ing.Tokens {
			state := ACTIVE
			for t, s := range r.tokenStates {
				if tok == t {
					state = s
					break
				}
			}

			ing := r.Desc.Ingesters[name]
			ing.State = state
			r.Desc.Ingesters[name] = ing
		}
	}
}

// generateRing generates a namedRing given a schema describing tokens in the
// ring. The schema is a space-delimited list of letters followed by an
// optional number and a state suffix. If a letter is specified more than once,
// each instance must have a number to differentiate between the two:
//
// A B C D1 E D2 F G
//
// This example creates a ring of 7 ingesters, where ingester with id "D" has
// two tokens, D1 and D2. Tokens are assigned values of the previous token's
// value plus one.
//
// Each token can be suffixed with a state marker to affect the token's state:
//
// .: ACTIVE
// ?: PENDING
// +: JOINING
// -: LEAVING
//
// If no suffix is specified, the default state will be ACTIVE.
func generateRing(t *testing.T, desc string) *namedRing {
	t.Helper()

	regex, err := regexp.Compile(
		`(?P<ingester>[A-Z])(?P<token>\d*)(?P<state>\+|\-|\?|\.)?`,
	)
	if err != nil {
		t.Fatalf("unexpected regex err %v", err)
	}

	r := &namedRing{
		Desc:        &Desc{},
		namedTokens: make(map[string]uint32),
		tokenStates: make(map[uint32]IngesterState),
	}
	r.Ingesters = make(map[string]IngesterDesc)

	tokens := strings.Split(desc, " ")
	var nextToken uint32 = 1

	for _, tokDesc := range tokens {
		if tokDesc == "" {
			continue
		}

		submatches := regex.FindStringSubmatch(tokDesc)
		if submatches == nil {
			t.Fatalf("invalid token desc %s", tokDesc)
			continue
		}

		ingester := submatches[1]
		tokenIndex := 1
		state := ACTIVE

		if submatches[2] != "" {
			tokenIndex, err = strconv.Atoi(submatches[2])
			if err != nil {
				t.Fatalf("invalid token index %s in %s", submatches[2], tokDesc)
			}
		}
		if submatches[3] != "" {
			switch stateStr := submatches[3]; stateStr {
			case ".":
				state = ACTIVE
			case "?":
				state = PENDING
			case "+":
				state = JOINING
			case "-":
				state = LEAVING
			default:
				t.Fatalf("invalid token state operator %s in %s", stateStr, tokDesc)
			}
		}

		ing, ok := r.Ingesters[ingester]
		if !ok {
			ing = IngesterDesc{
				Addr:      ingester,
				State:     ACTIVE,
				Timestamp: time.Now().Unix(),
			}
		}

		if tokenIndex != len(ing.Tokens)+1 {
			t.Fatalf("invalid token index %d in %s, should be %d", tokenIndex, tokDesc, len(ing.Tokens)+1)
		}

		ing.Tokens = append(ing.Tokens, nextToken)
		r.tokenStates[nextToken] = state
		r.namedTokens[tokDesc] = nextToken
		r.Ingesters[ingester] = ing
		nextToken++
	}

	for id, ing := range r.Ingesters {
		r.Ingesters[id] = ing
	}

	return r
}

func TestGenerateRing(t *testing.T) {
	tt := []struct {
		desc      string
		ingesters int
		active    int
		pending   int
		leaving   int
		joining   int
	}{
		{"A B C", 3, 3, 0, 0, 0},
		{"A1 B A2 C", 3, 4, 0, 0, 0},
		{"A1. B? A2+ C-", 3, 1, 1, 1, 1},
	}

	for _, tc := range tt {
		t.Run(tc.desc, func(t *testing.T) {
			r := generateRing(t, tc.desc)

			require.Equal(t, tc.active, len(r.FindTokensByState(ACTIVE)))
			require.Equal(t, tc.pending, len(r.FindTokensByState(PENDING)))
			require.Equal(t, tc.leaving, len(r.FindTokensByState(LEAVING)))
			require.Equal(t, tc.joining, len(r.FindTokensByState(JOINING)))
			require.Equal(t, tc.ingesters, len(r.Desc.Ingesters))
		})
	}
}

func TestRingGet(t *testing.T) {
	tt := []struct {
		name   string
		desc   string
		key    uint32
		op     Operation
		expect []string
	}{
		{"all active", "A B C", 0, Read, []string{"A", "B", "C"}},
		{"wrap around", "A B C", 2, Read, []string{"C", "A", "B"}},
		{"skip joining on read", "A B+ C+ D E", 0, Read, []string{"A", "D", "E"}},
		{"skip joining on write", "A B+ C+ D E", 0, Write, []string{"A", "D", "E"}},
		{"skip leaving on write", "A B- C- D E", 0, Write, []string{"A", "D", "E"}},
		{"don't skip leaving on read", "A B- C- D E", 0, Read, []string{"A", "B", "C"}},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			var ringConfig Config
			flagext.DefaultValues(&ringConfig)
			ringConfig.ReplicationFactor = 3
			ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())

			r, err := New(ringConfig, "ingester", "ring")
			require.NoError(t, err)
			// Stop is slow, run it in a goroutine and don't wait for it
			defer func() { go r.Stop() }()

			nr := generateRing(t, tc.desc)
			nr.BindStates(t)

			r.ringDesc = nr.Desc
			r.ringTokens = r.ringDesc.GetNavigator()
			rs, err := r.Get(tc.key, tc.op, nil)
			require.NoError(t, err)

			names := []string{}
			for _, ing := range rs.Ingesters {
				names = append(names, ing.Addr)
			}

			require.Equal(t, tc.expect, names)
		})
	}
}
