package compactor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

func TestRingConfig_DefaultConfigToLifecyclerConfig(t *testing.T) {
	cfg := RingConfig{}
	expected := ring.LifecyclerConfig{}
	flagext.DefaultValues(&cfg, &expected)

	// The default config of the compactor ring must be the exact same
	// of the default lifecycler config, except few options which are
	// intentionally overridden
	expected.ListenPort = cfg.ListenPort
	expected.RingConfig.ReplicationFactor = 1
	expected.RingConfig.SubringCacheDisabled = true
	expected.NumTokens = 512
	expected.MinReadyDuration = 0
	expected.FinalSleep = 0

	assert.Equal(t, expected, cfg.ToLifecyclerConfig())
}

func TestRingConfig_CustomConfigToLifecyclerConfig(t *testing.T) {
	cfg := RingConfig{}
	expected := ring.LifecyclerConfig{}
	flagext.DefaultValues(&cfg, &expected)

	// Customize the compactor ring config
	cfg.HeartbeatPeriod = 1 * time.Second
	cfg.HeartbeatTimeout = 10 * time.Second
	cfg.InstanceID = "test"
	cfg.InstanceInterfaceNames = []string{"abc1"}
	cfg.InstancePort = 10
	cfg.InstanceAddr = "1.2.3.4"
	cfg.ListenPort = 10

	// The lifecycler config should be generated based upon the compactor
	// ring config
	expected.HeartbeatPeriod = cfg.HeartbeatPeriod
	expected.RingConfig.HeartbeatTimeout = cfg.HeartbeatTimeout
	expected.RingConfig.SubringCacheDisabled = true
	expected.ID = cfg.InstanceID
	expected.InfNames = cfg.InstanceInterfaceNames
	expected.Port = cfg.InstancePort
	expected.Addr = cfg.InstanceAddr
	expected.ListenPort = cfg.ListenPort

	// Hardcoded config
	expected.RingConfig.ReplicationFactor = 1
	expected.NumTokens = 512
	expected.MinReadyDuration = 0
	expected.FinalSleep = 0

	assert.Equal(t, expected, cfg.ToLifecyclerConfig())
}
