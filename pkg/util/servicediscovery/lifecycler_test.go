package servicediscovery

import (
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/stretchr/testify/require"
)

func TestRegistryLifecycler_Registration(t *testing.T) {
	kvStore := consul.NewInMemoryClient(GetServiceRegistryCodec())

	// Start the first lifecycler
	config1 := newRegistryLifecyclerTestConfig("test-1", 100*time.Millisecond, 500*time.Millisecond)
	lifecycler1, err := NewRegistryLifecyclerWithKVStore("test", config1, kvStore, util.Logger)
	require.NoError(t, err)
	defer lifecycler1.Stop()

	// Ensure the first instance has been registered
	test.Poll(t, time.Second, 1, func() interface{} {
		return lifecycler1.HealthyCount()
	})

	// Start the second lifecycler
	config2 := newRegistryLifecyclerTestConfig("test-2", 100*time.Millisecond, 500*time.Millisecond)
	lifecycler2, err := NewRegistryLifecyclerWithKVStore("test", config2, kvStore, util.Logger)
	require.NoError(t, err)
	defer lifecycler2.Stop()

	// Ensure the second instance has been registered and updated in both lifecyclers
	test.Poll(t, time.Second, 2, func() interface{} {
		return lifecycler2.HealthyCount()
	})

	test.Poll(t, time.Second, 2, func() interface{} {
		return lifecycler1.HealthyCount()
	})
}

func TestRegistryLifecycler_HeartbeatTimeout(t *testing.T) {
	kvStore := consul.NewInMemoryClient(GetServiceRegistryCodec())

	// Start two lifecyclers. The second one is configured with a large heartbeat period
	// so that it will timeout and will be removed by the first one
	config1 := newRegistryLifecyclerTestConfig("test-1", 100*time.Millisecond, 500*time.Millisecond)
	config2 := newRegistryLifecyclerTestConfig("test-2", 1*time.Minute, 5*time.Minute)

	lifecycler1, err := NewRegistryLifecyclerWithKVStore("test", config1, kvStore, util.Logger)
	require.NoError(t, err)
	defer lifecycler1.Stop()

	lifecycler2, err := NewRegistryLifecyclerWithKVStore("test", config2, kvStore, util.Logger)
	require.NoError(t, err)
	defer lifecycler2.Stop()

	// Wait the first lifecycler reaches the count of 2
	test.Poll(t, time.Second, 2, func() interface{} {
		return lifecycler1.HealthyCount()
	})

	// Wait the first lifecycler count goes back to 1, because the second lifecycler timedout
	test.Poll(t, time.Second, 1, func() interface{} {
		return lifecycler1.HealthyCount()
	})
}

func TestRegistryLifecycler_Stop(t *testing.T) {
	kvStore := consul.NewInMemoryClient(GetServiceRegistryCodec())

	// Start two lifecyclers
	config1 := newRegistryLifecyclerTestConfig("test-1", 100*time.Millisecond, time.Minute)
	config2 := newRegistryLifecyclerTestConfig("test-2", 100*time.Millisecond, time.Minute)

	lifecycler1, err := NewRegistryLifecyclerWithKVStore("test", config1, kvStore, util.Logger)
	require.NoError(t, err)
	defer lifecycler1.Stop()

	lifecycler2, err := NewRegistryLifecyclerWithKVStore("test", config2, kvStore, util.Logger)
	require.NoError(t, err)
	defer lifecycler2.Stop()

	// Wait until both lifecyclers reaches the count of 2
	test.Poll(t, 200*time.Millisecond, 2, func() interface{} {
		return lifecycler1.HealthyCount()
	})

	test.Poll(t, 200*time.Millisecond, 2, func() interface{} {
		return lifecycler2.HealthyCount()
	})

	// Stop the second lifecycler
	lifecycler2.Stop()

	// While stopping, the second lifecycler should have removed itself from the registry
	// so we do expect the first lifecycler to be soon updated
	test.Poll(t, 200*time.Millisecond, 1, func() interface{} {
		return lifecycler1.HealthyCount()
	})
}

func newRegistryLifecyclerTestConfig(instanceID string, heartbeatPeriod, heartbeatTimeout time.Duration) RegistryLifecyclerConfig {
	config := RegistryLifecyclerConfig{}
	flagext.DefaultValues(&config)

	config.HeartbeatPeriod = heartbeatPeriod
	config.HeartbeatTimeout = heartbeatTimeout
	config.InstanceID = instanceID

	return config
}
