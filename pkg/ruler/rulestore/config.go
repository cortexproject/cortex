package rulestore

import (
	"context"
	"flag"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/configs/client"
	"github.com/cortexproject/cortex/pkg/ruler/rules"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
)

const (
	ConfigDB = "configdb"

	name   = "ruler-storage"
	prefix = "ruler-storage."
)

// Config configures a rule store.
type Config struct {
	bucket.Config `yaml:",inline"`
	ConfigDB      client.Config `yaml:"configdb"`
}

// RegisterFlags registers the backend storage config.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.ExtraBackends = []string{ConfigDB}
	cfg.ConfigDB.RegisterFlagsWithPrefix(prefix, f)
	cfg.RegisterFlagsWithPrefix(prefix, f)
}

// NewRuleStore creates a new bucket client based on the configured backend
func NewRuleStore(ctx context.Context, cfg Config, cfgProvider bucket.TenantConfigProvider, logger log.Logger, reg prometheus.Registerer) (rules.RuleStore, error) {
	if cfg.Backend == ConfigDB {
		c, err := client.New(cfg.ConfigDB)

		if err != nil {
			return nil, err
		}

		return rules.NewConfigRuleStore(c), nil
	}

	bucketClient, err := bucket.NewClient(ctx, cfg.Config, name, logger, reg)
	if err != nil {
		return nil, err
	}

	store := NewBucketRuleStore(bucketClient, cfgProvider, logger)
	if err != nil {
		return nil, err
	}

	return store, nil
}
