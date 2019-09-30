package ring

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func makeSequentialTokenGenerator() TokenGeneratorFunc {
	n := 0

	return func(num int, taken []uint32) []uint32 {
		start := n
		end := start + num

		ret := make([]uint32, num)
		for i := n; i < end; i++ {
			ret[i-start] = uint32(i + 1)
		}

		n = end
		return ret
	}
}

type mockTokenCheckerTransfer struct {
	IncrementalTransferer
	tokens []uint32
}

func (t *mockTokenCheckerTransfer) MemoryStreamTokens() []uint32 {
	return t.tokens
}

func TestTokenChecker(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.ReplicationFactor = 1
	codec := GetCodec()

	inMemory := consul.NewInMemoryClient(codec)
	mockClient := &MockClient{}
	mockClient.MapFunctions(inMemory)
	ringConfig.KVStore.Mock = mockClient

	r, err := New(ringConfig, "ring", "ring")
	require.NoError(t, err)
	defer r.Stop()

	transfer := &mockTokenCheckerTransfer{}
	generator := makeSequentialTokenGenerator()

	var lifecyclers []*Lifecycler
	for i := 0; i < 2; i++ {
		id := fmt.Sprintf("lc-%d", i)

		lcc := testLifecyclerConfig(ringConfig, id)
		lcc.Addr = id
		lcc.NumTokens = 32
		lcc.GenerateTokens = generator

		lc, err := NewLifecycler(lcc, &nopFlushTransferer{true}, transfer, id, "ring", true)
		require.NoError(t, err)
		lc.Start()
		defer lc.Shutdown()

		test.Poll(t, 500*time.Millisecond, true, func() interface{} {
			d, err := r.KVClient.Get(context.Background(), "ring")
			require.NoError(t, err)
			desc, ok := d.(*Desc)
			if !ok {
				return false
			}
			i, exist := desc.Ingesters[id]
			return exist && i.State == ACTIVE
		})

		lifecyclers = append(lifecyclers, lc)
	}

	// Update consul for each lifecycler twice: doing it twice makes sure
	// that each lifecycler sees the other ones.
	for i := 0; i < 2; i++ {
		for _, lc := range lifecyclers {
			err := lc.updateConsul(context.Background())
			require.NoError(t, err)
		}
	}

	// Populate transfer with all tokens in lifecycler
	for _, tok := range lifecyclers[0].getTokens() {
		transfer.tokens = append(transfer.tokens, tok-1)
	}

	calledHandler := atomic.NewBool(false)
	streamsHandler := func(l []uint32) {
		calledHandler.Store(true)
	}

	tc := NewTokenChecker(TokenCheckerConfig{
		CheckOnInterval: time.Duration(50 * time.Millisecond),
	}, lifecyclers[0].cfg.RingConfig, lifecyclers[0], streamsHandler)
	defer tc.Shutdown()

	// Make sure ring is updated
	tc.syncRing()

	// Make sure CheckToken with token == 1 returns true
	test.Poll(t, time.Millisecond*500, true, func() interface{} {
		return tc.TokenExpected(1)
	})

	// Make sure CheckToken with a token out of range returns false.
	require.False(t, tc.TokenExpected(
		transfer.tokens[len(transfer.tokens)-1]+1,
	))

	// Make sure all tokens are valid.
	test.Poll(t, time.Millisecond*500, true, func() interface{} {
		return tc.CheckAllStreams()
	})

	require.True(t, calledHandler.Load())
}
