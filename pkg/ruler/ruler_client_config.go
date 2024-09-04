package ruler

import (
	"flag"
	"time"

	"github.com/cortexproject/cortex/pkg/util/grpcclient"
)

type ClientConfig struct {
	grpcclient.Config `yaml:",inline"`
	RemoteTimeout     time.Duration `yaml:"remote_timeout"`
}

func (cfg *ClientConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.Config.RegisterFlagsWithPrefix(prefix, "", f)
	f.DurationVar(&cfg.RemoteTimeout, prefix+".remote-timeout", 2*time.Minute, "Timeout for downstream rulers.")
}
