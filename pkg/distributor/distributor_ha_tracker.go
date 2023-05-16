package distributor

import (
	"flag"
	"time"

	"github.com/cortexproject/cortex/pkg/ha"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

type HATrackerConfig struct {
	EnableHATracker bool `yaml:"enable_ha_tracker"`
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
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *HATrackerConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.EnableHATracker, "distributor.ha-tracker.enable", false, "Enable the distributors HA tracker so that it can accept samples from Prometheus HA replicas gracefully (requires labels).")
	f.DurationVar(&cfg.UpdateTimeout, "distributor.ha-tracker.update-timeout", 15*time.Second, "Update the timestamp in the KV store for a given cluster/replica only after this amount of time has passed since the current stored timestamp.")
	f.DurationVar(&cfg.UpdateTimeoutJitterMax, "distributor.ha-tracker.update-timeout-jitter-max", 5*time.Second, "Maximum jitter applied to the update timeout, in order to spread the HA heartbeats over time.")
	f.DurationVar(&cfg.FailoverTimeout, "distributor.ha-tracker.failover-timeout", 30*time.Second, "If we don't receive any samples from the accepted replica for a cluster in this amount of time we will failover to the next replica we receive a sample from. This value must be greater than the update timeout")

	// We want the ability to use different Consul instances for the ring and
	// for HA cluster tracking. We also customize the default keys prefix, in
	// order to not clash with the ring key if they both share the same KVStore
	// backend (ie. run on the same consul cluster).
	cfg.KVStore.RegisterFlagsWithPrefix("distributor.ha-tracker.", "ha-tracker/", f)
}

func (cfg *HATrackerConfig) ToHATrackerConfig() ha.HATrackerConfig {
	haCfg := ha.HATrackerConfig{}
	flagext.DefaultValues(&haCfg)

	haCfg.EnableHATracker = cfg.EnableHATracker
	haCfg.UpdateTimeout = cfg.UpdateTimeout
	haCfg.UpdateTimeoutJitterMax = cfg.UpdateTimeoutJitterMax
	haCfg.FailoverTimeout = cfg.FailoverTimeout
	haCfg.KVStore = cfg.KVStore

	return haCfg
}
