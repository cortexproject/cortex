package jump

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	keys    = 1000
	buckets = 100
)

func TestHash(t *testing.T) {
	counts := map[int32]int{}
	r := rand.NewSource(1985)

	for i := 0; i <= 100; i++ {
		key := fmt.Sprintf("%064x", r.Int63())
		j := Hash(string(key), 10)
		counts[j]++
	}

	for i := int32(0); i < 10; i++ {
		require.InEpsilon(t, keys/buckets, counts[i], 0.5)
	}
}
