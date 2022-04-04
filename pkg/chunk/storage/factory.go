package storage

import (
	"flag"

	"github.com/pkg/errors"
)

// Supported storage engines
const (
	StorageEngineChunks = "chunks"
	StorageEngineBlocks = "blocks"
)

// Config chooses which storage client to use.
type Config struct {
	Engine string `yaml:"engine"`
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Engine, "store.engine", "blocks", "The storage engine to use: blocks is the only supported option today.")
}

// Validate config and returns error on failure
func (cfg *Config) Validate() error {
	if cfg.Engine != StorageEngineBlocks {
		return errors.New("unsupported storage engine (only blocks is supported for ingest)")
	}

	return nil
}
