package ruler

import (
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/ring/testutils"
	"github.com/cortexproject/cortex/pkg/util/test"
)

// TestRulerShutdown tests shutting down ruler unregisters correctly
func TestRulerShutdown(t *testing.T) {
	config := defaultRulerConfig()
	config.EnableSharding = true

	{
		r := newTestRuler(t, config)
		time.Sleep(100 * time.Millisecond)
		r.Stop() // doesn't actually unregister due to skipUnregister: true
	}

	test.Poll(t, 100*time.Millisecond, 0, func() interface{} {
		return testutils.NumTokens(config.LifecyclerConfig.RingConfig.KVStore.Mock, "localhost")
	})
}

// TestRulerRestart tests a restarting ruler doesn't keep adding more tokens.
func TestRulerRestart(t *testing.T) {
	config := defaultRulerConfig()
	config.LifecyclerConfig.SkipUnregister = true
	config.EnableSharding = true

	{
		r := newTestRuler(t, config)
		time.Sleep(100 * time.Millisecond)
		r.Stop() // doesn't actually unregister due to skipUnregister: true
	}

	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return testutils.NumTokens(config.LifecyclerConfig.RingConfig.KVStore.Mock, "localhost")
	})

	{
		r := newTestRuler(t, config)
		time.Sleep(100 * time.Millisecond)
		r.Stop() // doesn't actually unregister due to skipUnregister: true
	}

	time.Sleep(200 * time.Millisecond)

	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return testutils.NumTokens(config.LifecyclerConfig.RingConfig.KVStore.Mock, "localhost")
	})
}
