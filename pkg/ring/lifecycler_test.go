package ring

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/test"
)

type flushTransferer struct {
	lifecycler *Lifecycler
}

func (f *flushTransferer) StopIncomingRequests() {}
func (f *flushTransferer) Flush()                {}
func (f *flushTransferer) TransferOut(ctx context.Context) error {
	if err := f.lifecycler.ClaimTokensFor(ctx, "ing1"); err != nil {
		return err
	}
	return f.lifecycler.ChangeState(ctx, ACTIVE)
}

func testLifecyclerConfig(ringConfig Config, id string) LifecyclerConfig {
	var lifecyclerConfig LifecyclerConfig
	flagext.DefaultValues(&lifecyclerConfig)
	lifecyclerConfig.Addr = "0.0.0.0"
	lifecyclerConfig.Port = 1
	lifecyclerConfig.RingConfig = ringConfig
	lifecyclerConfig.NumTokens = 1
	lifecyclerConfig.ClaimOnRollout = true
	lifecyclerConfig.ID = id
	lifecyclerConfig.FinalSleep = 0
	return lifecyclerConfig
}

func checkDenormalised(d interface{}, id string) bool {
	desc, ok := d.(*Desc)
	return ok &&
		len(desc.Ingesters) == 1 &&
		desc.Ingesters[id].State == ACTIVE &&
		len(desc.Ingesters[id].Tokens) == 0 &&
		len(desc.Tokens) == 1
}

func checkNormalised(d interface{}, id string) bool {
	desc, ok := d.(*Desc)
	return ok &&
		len(desc.Ingesters) == 1 &&
		desc.Ingesters[id].State == ACTIVE &&
		len(desc.Ingesters[id].Tokens) == 1 &&
		len(desc.Tokens) == 0
}

func TestRingNormaliseMigration(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	codec := ProtoCodec{Factory: ProtoDescFactory}
	ringConfig.KVStore.Mock = NewInMemoryKVClient(codec)

	r, err := New(ringConfig, "ingester")
	require.NoError(t, err)
	defer r.Stop()

	// Add an 'ingester' with denormalised tokens.
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")

	ft := &flushTransferer{}
	l1, err := NewLifecycler(lifecyclerConfig1, ft, "ingester")
	require.NoError(t, err)

	// Check this ingester joined, is active, and has one token.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)
		return checkDenormalised(d, "ing1")
	})

	token := l1.tokens[0]

	// Add a second ingester with normalised tokens.
	var lifecyclerConfig2 = testLifecyclerConfig(ringConfig, "ing2")
	lifecyclerConfig2.JoinAfter = 100 * time.Second
	lifecyclerConfig2.NormaliseTokens = true

	l2, err := NewLifecycler(lifecyclerConfig2, &flushTransferer{}, "ingester")
	require.NoError(t, err)

	// This will block until l1 has successfully left the ring.
	ft.lifecycler = l2 // When l1 shutsdown, call l2.ClaimTokensFor("ing1")
	l1.Shutdown()

	// Check the new ingester joined, has the same token, and is active.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)
		return checkNormalised(d, "ing2") &&
			d.(*Desc).Ingesters["ing2"].Tokens[0] == token
	})
}

type nopFlushTransferer struct{}

func (f *nopFlushTransferer) StopIncomingRequests() {}
func (f *nopFlushTransferer) Flush()                {}
func (f *nopFlushTransferer) TransferOut(ctx context.Context) error {
	panic("should not be called")
}

func TestRingRestart(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	codec := ProtoCodec{Factory: ProtoDescFactory}
	ringConfig.KVStore.Mock = NewInMemoryKVClient(codec)

	r, err := New(ringConfig, "ingester")
	require.NoError(t, err)
	defer r.Stop()

	// Add an 'ingester' with normalised tokens.
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")
	lifecyclerConfig1.NormaliseTokens = true
	l1, err := NewLifecycler(lifecyclerConfig1, &nopFlushTransferer{}, "ingester")
	require.NoError(t, err)

	// Check this ingester joined, is active, and has one token.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)
		return checkNormalised(d, "ing1")
	})

	token := l1.tokens[0]

	// Add a second ingester with the same settings, so it will think it has restarted
	l2, err := NewLifecycler(lifecyclerConfig1, &nopFlushTransferer{}, "ingester")
	require.NoError(t, err)

	// Check the new ingester picked up the same token
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)
		l2Tokens := l2.getTokens()
		return checkNormalised(d, "ing1") &&
			len(l2Tokens) == 1 &&
			l2Tokens[0] == token
	})
}
