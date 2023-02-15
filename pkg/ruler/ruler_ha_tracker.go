package ruler

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/cortexproject/cortex/pkg/ha"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

type HATrackerConfig struct {
	// We should only update the timestamp if the difference
	// between the stored timestamp and the time we received a sample at
	// is more than this duration.
	UpdateTimeout          time.Duration `yaml:"ha_tracker_update_timeout"`
	UpdateTimeoutJitterMax time.Duration `yaml:"ha_tracker_update_timeout_jitter_max"`
	// We should only failover to accepting samples from a replica
	// other than the replica written in the KVStore if the difference
	// between the stored timestamp and the time we received a sample is
	// more than this duration
	FailoverTimeout time.Duration `yaml:"ha_tracker_failover_timeout"`

	KVStore kv.Config `yaml:"kvstore" doc:"description=Backend storage to use for the ring. Please be aware that memberlist is not supported by the HA tracker since gossip propagation is too slow for HA purposes."`

	// ID of this replica (instance ID)
	ReplicaId string
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *HATrackerConfig) RegisterFlags(f *flag.FlagSet) {
	hostname, err := os.Hostname()
	if err != nil {
		panic(fmt.Errorf("failed to get hostname, %w", err))
	}

	f.DurationVar(&cfg.UpdateTimeout, "ruler.ha-tracker.update-timeout", 15*time.Second, "Update the timestamp in the KV store for a given replica only after this amount of time has passed since the current stored timestamp.")
	f.DurationVar(&cfg.UpdateTimeoutJitterMax, "ruler.ha-tracker.update-timeout-jitter-max", 5*time.Second, "Maximum jitter applied to the update timeout, in order to spread the HA heartbeats over time.")
	f.DurationVar(&cfg.FailoverTimeout, "ruler.ha-tracker.failover-timeout", 30*time.Second, "If we don't receive any ticks from the accepted replica in this amount of time we will failover to the next replica. This value must be greater than the update timeout")

	f.StringVar(&cfg.ReplicaId, "ruler.ha-tracker.replica-id", hostname, "Replica ID to register in the HA tracker.")

	// We want the ability to use different Consul instances for the ring and
	// for HA tracking. We also customize the default keys prefix, in
	// order to not clash with the ring key if they both share the same KVStore
	// backend (ie. run on the same consul cluster).
	cfg.KVStore.RegisterFlagsWithPrefix("ruler.ha-tracker.", "ruler-ha-tracker/", f)
}

func (cfg *HATrackerConfig) ToHATrackerConfig() ha.HATrackerConfig {
	haCfg := ha.HATrackerConfig{}
	flagext.DefaultValues(&haCfg)

	haCfg.EnableHATracker = true
	haCfg.UpdateTimeout = cfg.UpdateTimeout
	haCfg.UpdateTimeoutJitterMax = cfg.UpdateTimeoutJitterMax
	haCfg.FailoverTimeout = cfg.FailoverTimeout
	haCfg.KVStore = cfg.KVStore

	return haCfg
}
