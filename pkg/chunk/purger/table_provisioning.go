package purger

import (
	"flag"

	"github.com/cortexproject/cortex/pkg/chunk"
)

// TableProvisioningConfig holds config for table throuput and autoscaling. Currently only used by DynamoDB.
type TableProvisioningConfig struct {
	chunk.ActiveTableProvisionConfig `yaml:",inline"`
	TableTags                        chunk.Tags `yaml:"tags"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
// Adding a separate RegisterFlags here instead of using it from embedded chunk.ActiveTableProvisionConfig to be able to manage defaults separately.
// Defaults for WriteScale and ReadScale are shared for now to avoid adding further complexity since autoscaling is disabled anyways by default.
func (cfg *TableProvisioningConfig) RegisterFlags(argPrefix string, f *flag.FlagSet) {
	f.Int64Var(&cfg.ProvisionedWriteThroughput, argPrefix+".write-throughput", 1, "Table default write throughput. Supported by DynamoDB")
	f.Int64Var(&cfg.ProvisionedReadThroughput, argPrefix+".read-throughput", 300, "Table default read throughput. Supported by DynamoDB")
	f.BoolVar(&cfg.ProvisionedThroughputOnDemandMode, argPrefix+".enable-ondemand-throughput-mode", false, "Enables on demand throughput provisioning for the storage provider (if supported). Applies only to tables which are not autoscaled. Supported by DynamoDB")
	f.Var(&cfg.TableTags, argPrefix+".tags", "Tag (of the form key=value) to be added to the tables. Supported by DynamoDB")

	cfg.WriteScale.RegisterFlags(argPrefix+".write-throughput.scale", f)
	cfg.ReadScale.RegisterFlags(argPrefix+".read-throughput.scale", f)
}

func (cfg DeleteStoreConfig) GetTables() []chunk.TableDesc {
	return []chunk.TableDesc{cfg.ProvisionConfig.BuildTableDesc(cfg.RequestsTableName, cfg.ProvisionConfig.TableTags)}
}
