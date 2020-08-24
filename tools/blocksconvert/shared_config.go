package blocksconvert

import (
	"flag"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

type SharedConfig struct {
	SchemaConfig chunk.SchemaConfig // Flags registered by main.go
	Bucket       tsdb.BucketConfig
	BucketPrefix string
}

func (cfg *SharedConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.SchemaConfig.RegisterFlags(flag.CommandLine)
	cfg.Bucket.RegisterFlags(flag.CommandLine)

	f.StringVar(&cfg.BucketPrefix, "blocksconvert.bucket-prefix", "migration", "Prefix in the bucket for storing plan files.")
}
