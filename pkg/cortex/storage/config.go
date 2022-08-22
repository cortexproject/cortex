package storage

import (
	"flag"

	"github.com/pkg/errors"
)

const (
	StorageEngineBlocks = "blocks"
)

type Config struct {
	Engine string `yaml:"engine"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Engine, "store.engine", "blocks", "The storage engine to use: blocks is the only supported option today.")
}

func (cfg *Config) Validate() error {
	if cfg.Engine != StorageEngineBlocks {
		return errors.New("unsupported storage engine (only blocks is supported for ingest)")
	}

	return nil
}
