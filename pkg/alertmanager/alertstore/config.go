package alertstore

import (
	"flag"
	"slices"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore/configdb"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore/local"
	"github.com/cortexproject/cortex/pkg/configs/client"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/util/users"
)

// Config configures the alertmanager storage backend.
type Config struct {
	bucket.Config `yaml:",inline"`
	ConfigDB      client.Config            `yaml:"configdb"`
	Local         local.StoreConfig        `yaml:"local"`
	UsersScanner  users.UsersScannerConfig `yaml:"users_scanner"`
}

// RegisterFlags registers the backend storage config.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	prefix := "alertmanager-storage."

	cfg.ExtraBackends = []string{configdb.Name, local.Name}
	cfg.ConfigDB.RegisterFlagsWithPrefix(prefix, f)
	cfg.Local.RegisterFlagsWithPrefix(prefix, f)
	cfg.RegisterFlagsWithPrefix(prefix, f)
	cfg.UsersScanner.RegisterFlagsWithPrefix(prefix, f)
}

// IsFullStateSupported returns if the given configuration supports access to FullState objects.
func (cfg *Config) IsFullStateSupported() bool {
	return slices.Contains(bucket.SupportedBackends, cfg.Backend)
}
