package distributor

import (
	"flag"
	"os"
	"time"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/go-kit/kit/log/level"
)

// RingConfig masks the ring lifecycler config which contains
// many options not really required by the distributors ring. This config
// is used to strip down the config to the minimum, and avoid confusion
// to the user.
type RingConfig struct {
	KVStore          kv.Config     `yaml:"kvstore,omitempty"`
	HeartbeatPeriod  time.Duration `yaml:"heartbeat_period,omitempty"`
	HeartbeatTimeout time.Duration `yaml:"heartbeat_timeout,omitempty"`

	// Instance details
	InstanceID             string   `yaml:"instance_id"`
	InstanceInterfaceNames []string `yaml:"instance_interface_names"`
	InstancePort           int      `yaml:"instance_port"`
	InstanceAddr           string   `yaml:"instance_addr"`

	// Injected internally
	ListenPort int `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet) {
	hostname, err := os.Hostname()
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	// Ring flags
	cfg.KVStore.RegisterFlagsWithPrefix("distributor.ring.", f)
	f.DurationVar(&cfg.HeartbeatPeriod, "distributor.ring.heartbeat-period", 5*time.Second, "Period at which to heartbeat to the ring.")
	f.DurationVar(&cfg.HeartbeatTimeout, "distributor.ring.heartbeat-timeout", time.Minute, "The heartbeat timeout after which distributors are considered unhealthy within the ring.")

	// Instance flags
	cfg.InstanceInterfaceNames = []string{"eth0", "en0"}
	f.Var((*flagext.Strings)(&cfg.InstanceInterfaceNames), "distributor.ring.instance-interface", "Name of network interface to read address from.")
	f.StringVar(&cfg.InstanceAddr, "distributor.ring.instance-addr", "", "IP address to advertise in the ring.")
	f.IntVar(&cfg.InstancePort, "distributor.ring.instance-port", 0, "Port to advertise in the ring (defaults to server.grpc-listen-port).")
	f.StringVar(&cfg.InstanceID, "distributor.ring.instance-id", hostname, "Instance ID to register in the ring.")
}

// ToLifecyclerConfig returns a LifecyclerConfig based on the distributor
// ring config.
func (cfg *RingConfig) ToLifecyclerConfig() ring.LifecyclerConfig {
	return ring.LifecyclerConfig{
		ListenPort:      &cfg.ListenPort,
		Addr:            cfg.InstanceAddr,
		Port:            cfg.InstancePort,
		ID:              cfg.InstanceID,
		InfNames:        cfg.InstanceInterfaceNames,
		SkipUnregister:  false,
		HeartbeatPeriod: cfg.HeartbeatPeriod,

		RingConfig: ring.Config{
			KVStore:          cfg.KVStore,
			HeartbeatTimeout: cfg.HeartbeatTimeout,

			// Unused by the distributors ring
			ReplicationFactor: 1,
		},

		// Unused by the distributors ring
		NumTokens:        1,
		JoinAfter:        0,
		MinReadyDuration: 0,
		FinalSleep:       0,
	}
}
