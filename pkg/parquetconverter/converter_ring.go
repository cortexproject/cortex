package parquetconverter

import (
	"flag"
	"os"
	"time"

	"github.com/go-kit/log/level"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

type RingConfig struct {
	KVStore          kv.Config     `yaml:"kvstore"`
	HeartbeatPeriod  time.Duration `yaml:"heartbeat_period"`
	HeartbeatTimeout time.Duration `yaml:"heartbeat_timeout"`
	AutoForgetDelay  time.Duration `yaml:"auto_forget_delay"`

	// Instance details
	InstanceID     string `yaml:"instance_id" doc:"hidden"`
	InstancePort   int    `yaml:"instance_port" doc:"hidden"`
	InstanceAddr   string `yaml:"instance_addr" doc:"hidden"`
	TokensFilePath string `yaml:"tokens_file_path"`

	// Injected internally
	ListenPort int `yaml:"-"`
}

func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet) {
	hostname, err := os.Hostname()
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	// Ring flags
	cfg.KVStore.RegisterFlagsWithPrefix("parquet-converter.ring.", "collectors/", f)
	f.DurationVar(&cfg.HeartbeatPeriod, "parquet-converter.ring.heartbeat-period", 5*time.Second, "Period at which to heartbeat to the ring. 0 = disabled.")
	f.DurationVar(&cfg.HeartbeatTimeout, "parquet-converter.ring.heartbeat-timeout", time.Minute, "The heartbeat timeout after which parquet-converter are considered unhealthy within the ring. 0 = never (timeout disabled).")
	f.DurationVar(&cfg.AutoForgetDelay, "parquet-converter.auto-forget-delay", 2*cfg.HeartbeatTimeout, "Time since last heartbeat before parquet-converter will be removed from ring. 0 to disable")

	f.StringVar(&cfg.InstanceAddr, "parquet-converter.ring.instance-addr", "", "IP address to advertise in the ring.")
	f.IntVar(&cfg.InstancePort, "parquet-converter.ring.instance-port", 0, "Port to advertise in the ring (defaults to server.grpc-listen-port).")
	f.StringVar(&cfg.InstanceID, "parquet-converter.ring.instance-id", hostname, "Instance ID to register in the ring.")
	f.StringVar(&cfg.TokensFilePath, "parquet-converter.ring.tokens-file-path", "", "File path where tokens are stored. If empty, tokens are not stored at shutdown and restored at startup.")
}

func (cfg *RingConfig) ToLifecyclerConfig() ring.LifecyclerConfig {
	// We have to make sure that the ring.LifecyclerConfig and ring.Config
	// defaults are preserved
	lc := ring.LifecyclerConfig{}
	rc := ring.Config{}

	flagext.DefaultValues(&lc)
	flagext.DefaultValues(&rc)

	// Configure ring
	rc.KVStore = cfg.KVStore
	rc.HeartbeatTimeout = cfg.HeartbeatTimeout
	rc.ReplicationFactor = 1

	// Configure lifecycler
	lc.RingConfig = rc
	lc.RingConfig.SubringCacheDisabled = true
	lc.ListenPort = cfg.ListenPort
	lc.Addr = cfg.InstanceAddr
	lc.Port = cfg.InstancePort
	lc.ID = cfg.InstanceID
	lc.UnregisterOnShutdown = true
	lc.HeartbeatPeriod = cfg.HeartbeatPeriod
	lc.JoinAfter = 0
	lc.MinReadyDuration = 0
	lc.FinalSleep = 0
	lc.TokensFilePath = cfg.TokensFilePath

	// We use a safe default instead of exposing to config option to the user
	// in order to simplify the config.
	lc.NumTokens = 512

	return lc
}
