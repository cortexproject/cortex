package ruler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/testutils"
	"github.com/cortexproject/cortex/pkg/util/test"
)

// TestRulerShutdown tests shutting down ruler unregisters correctly
func TestRulerShutdown(t *testing.T) {
	config, cleanup := defaultRulerConfig(newMockRuleStore(mockRules))
	config.EnableSharding = true
	config.Ring.SkipUnregister = false
	defer cleanup()

	r := newTestRuler(t, config)

	// Wait until the tokens are registered in the ring
	test.Poll(t, 100*time.Millisecond, config.Ring.NumTokens, func() interface{} {
		return testutils.NumTokens(config.Ring.KVStore.Mock, "localhost", ring.RulerRingKey)
	})

	r.Stop()

	// Wait until the tokens are unregistered from the ring
	test.Poll(t, 100*time.Millisecond, 0, func() interface{} {
		return testutils.NumTokens(config.Ring.KVStore.Mock, "localhost", ring.RulerRingKey)
	})
}

// TestRulerRestart tests a restarting ruler doesn't keep adding more tokens.
func TestRulerRestart(t *testing.T) {
	config, cleanup := defaultRulerConfig(newMockRuleStore(mockRules))
	config.Ring.SkipUnregister = true
	config.EnableSharding = true
	defer cleanup()

	r := newTestRuler(t, config)

	// Wait until the tokens are registered in the ring
	test.Poll(t, 100*time.Millisecond, config.Ring.NumTokens, func() interface{} {
		return testutils.NumTokens(config.Ring.KVStore.Mock, "localhost", ring.RulerRingKey)
	})

	// Stop the ruler. Doesn't actually unregister due to skipUnregister: true
	r.Stop()

	// We expect the tokens are preserved in the ring.
	assert.Equal(t, config.Ring.NumTokens, testutils.NumTokens(config.Ring.KVStore.Mock, "localhost", ring.RulerRingKey))

	// Create a new ruler which is expected to pick up tokens from the ring.
	r = newTestRuler(t, config)
	defer r.Stop()

	// Wait until the ruler is ACTIVE in the ring.
	test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
		return r.lifecycler.GetState()
	})

	// We expect no new tokens have been added to the ring.
	assert.Equal(t, config.Ring.NumTokens, testutils.NumTokens(config.Ring.KVStore.Mock, "localhost", ring.RulerRingKey))
}
