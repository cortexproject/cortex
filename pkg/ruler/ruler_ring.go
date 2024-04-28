package ruler

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/log"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

const (
	// If a ruler is unable to heartbeat the ring, its better to quickly remove it and resume
	// the evaluation of all rules since the worst case scenario is that some rulers will
	// receive duplicate/out-of-order sample errors.
	ringAutoForgetUnhealthyPeriods = 2
)

// RingOp is the operation used for distributing rule groups between rulers.
var RingOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, func(s ring.InstanceState) bool {
	// Only ACTIVE rulers get any rule groups. If instance is not ACTIVE, we need to find another ruler.
	return s != ring.ACTIVE
})

// ListRuleRingOp is the operation used for getting rule groups from rulers.
var ListRuleRingOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE, ring.LEAVING}, func(s ring.InstanceState) bool {
	// Although LEAVING ruler does not get any rule groups. If it is excluded, list rule will fail because not enough healthy instance.
	// So we still consider LEAVING as healthy. We also want to extend the listRule calls when the instance in the shard is not ACTIVE
	return s != ring.ACTIVE
})

// RingConfig masks the ring lifecycler config which contains
// many options not really required by the rulers ring. This config
// is used to strip down the config to the minimum, and avoid confusion
// to the user.
type RingConfig struct {
	KVStore              kv.Config     `yaml:"kvstore"`
	HeartbeatPeriod      time.Duration `yaml:"heartbeat_period"`
	HeartbeatTimeout     time.Duration `yaml:"heartbeat_timeout"`
	ReplicationFactor    int           `yaml:"replication_factor"`
	ZoneAwarenessEnabled bool          `yaml:"zone_awareness_enabled"`

	// Instance details
	InstanceID             string   `yaml:"instance_id" doc:"hidden"`
	InstanceInterfaceNames []string `yaml:"instance_interface_names"`
	InstancePort           int      `yaml:"instance_port" doc:"hidden"`
	InstanceAddr           string   `yaml:"instance_addr" doc:"hidden"`
	InstanceZone           string   `yaml:"instance_availability_zone" doc:"hidden"`
	NumTokens              int      `yaml:"num_tokens"`

	FinalSleep time.Duration `yaml:"final_sleep"`

	// Injected internally
	ListenPort int `yaml:"-"`

	// Used for testing
	SkipUnregister bool `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet) {
	hostname, err := os.Hostname()
	if err != nil {
		panic(fmt.Errorf("failed to get hostname, %w", err))
	}

	// Ring flags
	cfg.KVStore.RegisterFlagsWithPrefix("ruler.ring.", "rulers/", f)
	f.DurationVar(&cfg.HeartbeatPeriod, "ruler.ring.heartbeat-period", 5*time.Second, "Period at which to heartbeat to the ring. 0 = disabled.")
	f.DurationVar(&cfg.HeartbeatTimeout, "ruler.ring.heartbeat-timeout", time.Minute, "The heartbeat timeout after which rulers are considered unhealthy within the ring. 0 = never (timeout disabled).")
	f.DurationVar(&cfg.FinalSleep, "ruler.ring.final-sleep", 0*time.Second, "The sleep seconds when ruler is shutting down. Need to be close to or larger than KV Store information propagation delay")
	f.IntVar(&cfg.ReplicationFactor, "ruler.ring.replication-factor", 1, "EXPERIMENTAL: The replication factor to use when loading rule groups for API HA.")
	f.BoolVar(&cfg.ZoneAwarenessEnabled, "ruler.ring.zone-awareness-enabled", false, "EXPERIMENTAL: True to enable zone-awareness and load rule groups across different availability zones for API HA.")

	// Instance flags
	cfg.InstanceInterfaceNames = []string{"eth0", "en0"}
	f.Var((*flagext.StringSlice)(&cfg.InstanceInterfaceNames), "ruler.ring.instance-interface-names", "Name of network interface to read address from.")
	f.StringVar(&cfg.InstanceAddr, "ruler.ring.instance-addr", "", "IP address to advertise in the ring.")
	f.IntVar(&cfg.InstancePort, "ruler.ring.instance-port", 0, "Port to advertise in the ring (defaults to server.grpc-listen-port).")
	f.StringVar(&cfg.InstanceID, "ruler.ring.instance-id", hostname, "Instance ID to register in the ring.")
	f.StringVar(&cfg.InstanceZone, "ruler.ring.instance-availability-zone", "", "The availability zone where this instance is running. Required if zone-awareness is enabled.")
	f.IntVar(&cfg.NumTokens, "ruler.ring.num-tokens", 128, "Number of tokens for each ruler.")
}

// ToLifecyclerConfig returns a LifecyclerConfig based on the ruler
// ring config.
func (cfg *RingConfig) ToLifecyclerConfig(logger log.Logger) (ring.BasicLifecyclerConfig, error) {
	instanceAddr, err := ring.GetInstanceAddr(cfg.InstanceAddr, cfg.InstanceInterfaceNames, logger)
	if err != nil {
		return ring.BasicLifecyclerConfig{}, err
	}

	instancePort := ring.GetInstancePort(cfg.InstancePort, cfg.ListenPort)

	return ring.BasicLifecyclerConfig{
		ID:                  cfg.InstanceID,
		Addr:                fmt.Sprintf("%s:%d", instanceAddr, instancePort),
		Zone:                cfg.InstanceZone,
		HeartbeatPeriod:     cfg.HeartbeatPeriod,
		TokensObservePeriod: 0,
		NumTokens:           cfg.NumTokens,
		FinalSleep:          cfg.FinalSleep,
	}, nil
}

func (cfg *RingConfig) ToRingConfig() ring.Config {
	rc := ring.Config{}
	flagext.DefaultValues(&rc)

	rc.KVStore = cfg.KVStore
	rc.HeartbeatTimeout = cfg.HeartbeatTimeout
	rc.SubringCacheDisabled = true
	rc.ZoneAwarenessEnabled = cfg.ZoneAwarenessEnabled

	// Each rule group is evaluated by *exactly* one ruler, but it can be loaded by multiple rulers for API HA
	rc.ReplicationFactor = cfg.ReplicationFactor

	return rc
}

// GetReplicationSetForListRule is similar to ring.GetReplicationSetForOperation but does NOT require quorum. Because
// it does not require quorum it returns healthy instance in the AZ with failed instances unlike
// GetReplicationSetForOperation. This is important for ruler because healthy instances in the AZ with failed
// instance could be evaluating some rule groups.
func GetReplicationSetForListRule(r ring.ReadRing, cfg *RingConfig) (ring.ReplicationSet, map[string]struct{}, error) {
	healthy, unhealthy, err := r.GetAllInstanceDescs(ListRuleRingOp)
	if err != nil {
		return ring.ReplicationSet{}, make(map[string]struct{}), err
	}
	ringZones := make(map[string]struct{})
	zoneFailures := make(map[string]struct{})
	for _, instance := range healthy {
		ringZones[instance.Zone] = struct{}{}
	}
	for _, instance := range unhealthy {
		ringZones[instance.Zone] = struct{}{}
		zoneFailures[instance.Zone] = struct{}{}
	}
	// Max errors and max unavailable zones are mutually exclusive. We initialise both
	// to 0, and then we update them whether zone-awareness is enabled or not.
	maxErrors := 0
	maxUnavailableZones := 0
	// Because ring's Get method returns a number of ruler equal to the replication factor even if there is only 1 zone
	// and ZoneAwarenessEnabled, we can consider that ZoneAwarenessEnabled is disabled if there is only 1 zone since
	// rules are still replicated to rulers in the same zone.
	if cfg.ZoneAwarenessEnabled && len(ringZones) > 1 {
		numReplicatedZones := min(len(ringZones), r.ReplicationFactor())
		// Given that quorum is not required, we only need at least one of the zone to be healthy to succeed. But we
		// also need to handle case when RF < number of zones.
		maxUnavailableZones = numReplicatedZones - 1
		if len(zoneFailures) > maxUnavailableZones {
			return ring.ReplicationSet{}, zoneFailures, ring.ErrTooManyUnhealthyInstances
		}
	} else {
		numRequired := len(healthy) + len(unhealthy)
		if numRequired < r.ReplicationFactor() {
			numRequired = r.ReplicationFactor()
		}
		// quorum is not required so 1 replica is enough to handle the request
		numRequired -= r.ReplicationFactor() - 1
		if len(healthy) < numRequired {
			return ring.ReplicationSet{}, zoneFailures, ring.ErrTooManyUnhealthyInstances
		}

		maxErrors = len(healthy) - numRequired
	}
	return ring.ReplicationSet{
		Instances:           healthy,
		MaxErrors:           maxErrors,
		MaxUnavailableZones: maxUnavailableZones,
	}, zoneFailures, nil
}
