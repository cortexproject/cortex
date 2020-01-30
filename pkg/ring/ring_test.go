package ring

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
		desc.AddIngester(fmt.Sprintf("%d", i), fmt.Sprintf("ingester%d", i), tokens, ACTIVE)
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

	r.AddIngester(ingName, "addr", ing1Tokens, ACTIVE)

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

	r.AddIngester(ing1Name, "addr", newTokens, ACTIVE)

	require.Equal(t, newTokens, r.Ingesters[ing1Name].Tokens)
}

func TestSubring(t *testing.T) {
	r := NewDesc()

	n := 16 // number of ingesters in ring
	var prevTokens []uint32
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("ing%v", i)
		ingTokens := GenerateTokens(128, prevTokens)

		r.AddIngester(name, fmt.Sprintf("addr%v", i), ingTokens, ACTIVE)

		prevTokens = append(prevTokens, ingTokens...)
	}

	// Create a ring with the ingesters
	ring := Ring{
		name: "main ring",
		cfg: Config{
			HeartbeatTimeout: time.Hour,
		},
		ringDesc:   r,
		ringTokens: r.getTokens(),
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

		r.AddIngester(name, fmt.Sprintf("addr%v", i), ingTokens, ACTIVE)

		prevTokens = append(prevTokens, ingTokens...)
	}

	// Create a ring with the ingesters
	ring := Ring{
		name: "main ring",
		cfg: Config{
			HeartbeatTimeout: time.Hour,
		},
		ringDesc:   r,
		ringTokens: r.getTokens(),
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
