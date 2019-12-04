package ring

import (
	"context"
	"fmt"
	"math/rand"
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

	const (
		ing1Name = "ing1"
		ing2Name = "ing2"
	)

	ing1Tokens := GenerateTokens(128, nil)
	ing2Tokens := GenerateTokens(128, ing1Tokens)

	// store tokens to r.Tokens
	for _, t := range ing1Tokens {
		r.Tokens = append(r.Tokens, TokenDesc{Token: t, Ingester: ing1Name})
	}

	for _, t := range ing2Tokens {
		r.Tokens = append(r.Tokens, TokenDesc{Token: t, Ingester: ing2Name})
	}

	r.AddIngester(ing1Name, "addr", ing1Tokens, ACTIVE)

	require.Equal(t, "addr", r.Ingesters[ing1Name].Addr)
	require.Equal(t, ing1Tokens, r.Ingesters[ing1Name].Tokens)

	require.Equal(t, len(ing2Tokens), len(r.Tokens))
	for _, tok := range r.Tokens {
		require.NotEqual(t, "test", tok.Ingester)
	}
}

func TestAddIngesterReplacesExistingTokens(t *testing.T) {
	r := NewDesc()

	const (
		ing1Name = "ing1"
	)

	newTokens := GenerateTokens(128, nil)

	// previous tokens for ingester1 will be replaced
	r.Tokens = append(r.Tokens, TokenDesc{Token: 11111, Ingester: ing1Name})
	r.Tokens = append(r.Tokens, TokenDesc{Token: 22222, Ingester: ing1Name})
	r.Tokens = append(r.Tokens, TokenDesc{Token: 33333, Ingester: ing1Name})

	r.AddIngester(ing1Name, "addr", newTokens, ACTIVE)

	require.Equal(t, newTokens, r.Ingesters[ing1Name].Tokens)
	require.Equal(t, 0, len(r.Tokens)) // all previous tokens were removed
}
