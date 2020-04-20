package ruler

import (
	"context"
	"flag"
	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/configs/client"
	"github.com/cortexproject/cortex/pkg/ruler/rules"
	"github.com/cortexproject/cortex/pkg/ruler/rules/objectclient"
	"github.com/cortexproject/cortex/pkg/storage/backend/azure"
	"github.com/cortexproject/cortex/pkg/storage/backend/filesystem"
	"github.com/cortexproject/cortex/pkg/storage/backend/gcs"
	"github.com/cortexproject/cortex/pkg/storage/backend/s3"
)

// RuleStoreConfig conigures a rule store
type RuleStoreConfig struct {
	Type     string        `yaml:"type"`
	ConfigDB client.Config `yaml:"configdb"`

	// Object Storage Configs
	S3         s3.Config         `yaml:"s3"`
	GCS        gcs.Config        `yaml:"gcs"`
	Azure      azure.Config      `yaml:"azure"`
	Filesystem filesystem.Config `yaml:"filesystem"`

	mock rules.RuleStore `yaml:"-"`
}

// RegisterFlags registers flags.
func (cfg *RuleStoreConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.ConfigDB.RegisterFlagsWithPrefix("ruler.", f)
	cfg.Azure.RegisterFlagsWithPrefix("ruler.storage.", f)
	cfg.GCS.RegisterFlagsWithPrefix("ruler.storage.", f)
	cfg.S3.RegisterFlagsWithPrefix("ruler.storage.", f)
	f.StringVar(&cfg.Type, "ruler.storage.type", "configdb", "Method to use for backend rule storage (configdb, azure, gcs, s3)")
}

// NewRuleStorage returns a new rule storage backend poller and store
func NewRuleStorage(cfg RuleStoreConfig, logger log.Logger) (rules.RuleStore, error) {
	if cfg.mock != nil {
		return cfg.mock, nil
	}

	switch cfg.Type {
	case "configdb":
		c, err := client.New(cfg.ConfigDB)

		if err != nil {
			return nil, err
		}

		return rules.NewConfigRuleStore(c), nil
	case "azure":
		return newObjRuleStore(azure.NewBucketClient(cfg.Azure, "cortex-ruler", logger))
	case "gcs":
		return newObjRuleStore(gcs.NewBucketClient(context.Background(), cfg.GCS, "cortex-ruler", logger))
	case "s3":
		return newObjRuleStore(s3.NewBucketClient(cfg.S3, "cortex-ruler", logger))
	case "filesystem":
		return newObjRuleStore(filesystem.NewBucketClient(cfg.Filesystem))
	default:
		return nil, fmt.Errorf("Unrecognized rule storage mode %v, choose one of: configdb, gcs", cfg.Type)
	}
}

func newObjRuleStore(client objstore.Bucket, err error) (rules.RuleStore, error) {
	if err != nil {
		return nil, err
	}
	return objectclient.NewRuleStore(client), nil
}
