package bench

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/ring/kv/memberlist"
)

type dnsProviderMock struct {
	resolved []string
}

func (p *dnsProviderMock) Resolve(ctx context.Context, addrs []string) error {
	p.resolved = addrs
	return nil
}

func (p dnsProviderMock) Addresses() []string {
	return p.resolved
}

func encodeMessage(b *testing.B, key string, d *ring.Desc) []byte {
	c := ring.GetCodec()
	val, err := c.Encode(d)
	require.NoError(b, err)

	kvPair := memberlist.KeyValuePair{
		Key:   key,
		Value: val,
		Codec: c.CodecID(),
	}

	ser, err := kvPair.Marshal()
	require.NoError(b, err)
	return ser
}

func generateUniqueTokens(ingester, numTokens int) []uint32 {
	// Generate unique tokens without using ring.GenerateTokens in order to not
	// rely on random number generation. Also, because generating unique tokens
	// with GenerateTokens can be quite expensive, it pollutes the CPU profile
	// to the point of being useless.
	tokens := make([]uint32, numTokens)
	for i := range tokens {
		tokens[i] = uint32((ingester * 100000) + (i * 10))
	}
	return tokens
}

// Benchmark the memberlist receive path when it is being used as the ring backing store.
func BenchmarkMemberlistReceiveWithRingDesc(b *testing.B) {
	c := ring.GetCodec()

	var cfg memberlist.KVConfig
	flagext.DefaultValues(&cfg)
	cfg.TCPTransport = memberlist.TCPTransportConfig{
		BindAddrs: []string{"localhost"},
	}
	cfg.Codecs = []codec.Codec{c}

	mkv := memberlist.NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), mkv))
	defer services.StopAndAwaitTerminated(context.Background(), mkv) //nolint:errcheck

	// Build the initial ring state:
	// - The ring isn't actually in use, so the fields such as address/zone are not important.
	// - The number of keys in the store has no impact for this test, so simulate a single ring.
	// - The number of instances in the ring does have a big impact.
	const numInstances = 600
	const numTokens = 128
	initialDesc := ring.NewDesc()
	{
		for i := 0; i < numInstances; i++ {
			tokens := generateUniqueTokens(i, numTokens)
			initialDesc.AddIngester(fmt.Sprintf("instance-%d", i), "127.0.0.1", "zone", tokens, ring.ACTIVE, time.Now())
		}
		// Send a single update to populate the store.
		msg := encodeMessage(b, "ring", initialDesc)
		mkv.NotifyMsg(msg)
	}

	// Ensure that each received message updates the ring.
	testMsgs := make([][]byte, b.N)
	for i := range testMsgs {
		instance := initialDesc.Ingesters["instance-0"]
		instance.Timestamp = initialDesc.Ingesters["instance-0"].RegisteredTimestamp + int64(i)

		testDesc := ring.NewDesc()
		testDesc.Ingesters["instance-0"] = instance
		testMsgs[i] = encodeMessage(b, "ring", testDesc)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mkv.NotifyMsg(testMsgs[i])
	}
}
