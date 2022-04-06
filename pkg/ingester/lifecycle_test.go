package ingester

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const userID = "1"

func defaultIngesterTestConfig(t testing.TB) Config {
	t.Helper()

	consul, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := Config{}
	flagext.DefaultValues(&cfg)
	flagext.DefaultValues(&cfg.BlocksStorageConfig)
	cfg.FlushCheckPeriod = 99999 * time.Hour
	cfg.MaxChunkIdle = 99999 * time.Hour
	cfg.ConcurrentFlushes = 1
	cfg.LifecyclerConfig.RingConfig.KVStore.Mock = consul
	cfg.LifecyclerConfig.NumTokens = 1
	cfg.LifecyclerConfig.ListenPort = 0
	cfg.LifecyclerConfig.Addr = "localhost"
	cfg.LifecyclerConfig.ID = "localhost"
	cfg.LifecyclerConfig.FinalSleep = 0
	cfg.ActiveSeriesMetricsEnabled = true
	return cfg
}

func defaultClientTestConfig() client.Config {
	clientConfig := client.Config{}
	flagext.DefaultValues(&clientConfig)
	return clientConfig
}

func defaultLimitsTestConfig() validation.Limits {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	return limits
}

// TestIngesterRestart tests a restarting ingester doesn't keep adding more tokens.
func TestIngesterRestart(t *testing.T) {
	config := defaultIngesterTestConfig(t)
	config.LifecyclerConfig.UnregisterOnShutdown = false

	{
		ingester, err := prepareIngesterWithBlocksStorage(t, config, prometheus.NewRegistry())
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))
		time.Sleep(100 * time.Millisecond)
		// Doesn't actually unregister due to UnregisterFromRing: false.
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester))
	}

	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return numTokens(config.LifecyclerConfig.RingConfig.KVStore.Mock, "localhost", RingKey)
	})

	{
		ingester, err := prepareIngesterWithBlocksStorage(t, config, prometheus.NewRegistry())
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))
		time.Sleep(100 * time.Millisecond)
		// Doesn't actually unregister due to UnregisterFromRing: false.
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester))
	}

	time.Sleep(200 * time.Millisecond)

	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return numTokens(config.LifecyclerConfig.RingConfig.KVStore.Mock, "localhost", RingKey)
	})
}

func TestIngester_ShutdownHandler(t *testing.T) {
	for _, unregister := range []bool{false, true} {
		t.Run(fmt.Sprintf("unregister=%t", unregister), func(t *testing.T) {
			registry := prometheus.NewRegistry()
			config := defaultIngesterTestConfig(t)
			config.LifecyclerConfig.UnregisterOnShutdown = unregister
			ingester, err := prepareIngesterWithBlocksStorage(t, config, registry)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))

			// Make sure the ingester has been added to the ring.
			test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
				return numTokens(config.LifecyclerConfig.RingConfig.KVStore.Mock, "localhost", RingKey)
			})

			recorder := httptest.NewRecorder()
			ingester.ShutdownHandler(recorder, nil)
			require.Equal(t, http.StatusNoContent, recorder.Result().StatusCode)

			// Make sure the ingester has been removed from the ring even when UnregisterFromRing is false.
			test.Poll(t, 100*time.Millisecond, 0, func() interface{} {
				return numTokens(config.LifecyclerConfig.RingConfig.KVStore.Mock, "localhost", RingKey)
			})
		})
	}
}

// numTokens determines the number of tokens owned by the specified
// address
func numTokens(c kv.Client, name, ringKey string) int {
	ringDesc, err := c.Get(context.Background(), ringKey)

	// The ringDesc may be null if the lifecycler hasn't stored the ring
	// to the KVStore yet.
	if ringDesc == nil || err != nil {
		return 0
	}
	rd := ringDesc.(*ring.Desc)
	return len(rd.Ingesters[name].Tokens)
}
