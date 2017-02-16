package ring

import (
	"fmt"
	"testing"
)

const (
	numIngester = 100
	numTokens   = 512
)

func BenchmarkRing(b *testing.B) {
	// Make a random ring with N ingesters, and M tokens per ingests
	desc := newDesc()
	takenTokens := []uint32{}
	for i := 0; i < numIngester; i++ {
		tokens := generateTokens(numTokens, takenTokens)
		takenTokens = append(takenTokens, tokens...)
		desc.addIngester(fmt.Sprintf("%d", i), fmt.Sprintf("ingester%d", i), tokens, ACTIVE)
	}

	consul := newMockConsulClient()
	ringBytes, err := ProtoCodec{}.Encode(desc)
	if err != nil {
		b.Fatal(err)
	}
	consul.PutBytes(consulKey, ringBytes)

	r, err := New(Config{
		ConsulConfig: ConsulConfig{
			mock: consul,
		},
	})
	if err != nil {
		b.Fatal(err)
	}

	// Generate a batch of N random keys, and look them up
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keys := generateTokens(100, nil)
		r.BatchGet(keys, 3, Write)
	}
}
