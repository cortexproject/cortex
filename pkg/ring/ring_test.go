package ring

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/util/flagext"
)

const (
	numIngester = 100
	numTokens   = 512
	numKeys     = 100
)

func BenchmarkRing(b *testing.B) {
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

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	keys := make([]uint32, numKeys)
	// Generate a batch of N random keys, and look them up
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		generateKeys(rnd, numKeys, keys)
		r.BatchGet(keys, Write)
	}
}

func generateKeys(r *rand.Rand, numTokens int, dest []uint32) {
	for i := 0; i < numTokens; i++ {
		dest[i] = r.Uint32()
	}
}
