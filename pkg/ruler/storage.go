package ruler

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	promRules "github.com/prometheus/prometheus/rules"

	"github.com/cortexproject/cortex/pkg/configs/client"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore/bucketclient"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore/configdb"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore/local"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
)

// NewRuleStore returns a rule store backend client based on the provided cfg.
func NewRuleStore(ctx context.Context, cfg rulestore.Config, cfgProvider bucket.TenantConfigProvider, loader promRules.GroupLoader, logger log.Logger, reg prometheus.Registerer) (rulestore.RuleStore, error) {
	if cfg.Backend == configdb.Name {
		c, err := client.New(cfg.ConfigDB)

		if err != nil {
			return nil, err
		}

		return configdb.NewConfigRuleStore(c), nil
	}

	if cfg.Backend == local.Name {
		return local.NewLocalRulesClient(cfg.Local, loader)
	}

	bucketClient, err := bucket.NewClient(ctx, cfg.Config, "ruler-storage", logger, reg)
	if err != nil {
		return nil, err
	}

	store := bucketclient.NewBucketRuleStore(bucketClient, cfgProvider, logger)
	if err != nil {
		return nil, err
	}

	return store, nil
}
