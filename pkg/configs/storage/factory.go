package storage

import (
	"context"
	"flag"
	"fmt"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/configs/storage/clients/client"
	"github.com/cortexproject/cortex/pkg/configs/storage/clients/gcp"
)

// Config is used to config an alertstore
type Config struct {
	BackendType string

	ClientConfig client.Config
	GCSConfig    gcp.GCSConfig
}

// RegisterFlags registers flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.BackendType, "configdb.backend", "client", "backend to use for storing and retrieving alerts")
	cfg.ClientConfig.RegisterFlags(f)
	cfg.GCSConfig.RegisterFlagsWithPrefix("configs.", f)
}

// New returns a configstore
func New(cfg Config) (configs.ConfigStore, error) {
	var (
		store configs.ConfigStore
		err   error
	)
	switch cfg.BackendType {
	case "client":
		store, err = client.New(cfg.ClientConfig)
	case "gcp":
		store, err = gcp.NewGCSConfigClient(context.Background(), cfg.GCSConfig)
	default:
		return nil, fmt.Errorf("Unrecognized config storage client %v, choose one of: client, gcp", cfg.BackendType)
	}

	return &instrumented{
		next: store,
	}, err
}
