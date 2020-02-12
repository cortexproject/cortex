package local

import (
	"context"
	"flag"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/prometheus/alertmanager/config"

	"github.com/cortexproject/cortex/pkg/alertmanager/alerts"
)

// StoreConfig configures a static file alertmanager store
type StoreConfig struct {
	Path string `yaml:"path"`
}

// RegisterFlags registers flags related to the alertmanager file store
func (cfg *StoreConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Path, "alertmanager.storage.local.path", "", "Path at which alertmanager configurations are stored.")
}

// Store is used to load user alertmanager configs from a local disk
type Store struct {
	cfg StoreConfig
}

// NewStore returns a new file alert store.
func NewStore(cfg StoreConfig) (*Store, error) {
	return &Store{cfg}, nil
}

// ListAlertConfigs returns a list of each users alertmanager config.
func (f *Store) ListAlertConfigs(ctx context.Context) (map[string]alerts.AlertConfigDesc, error) {
	configs := map[string]alerts.AlertConfigDesc{}
	err := filepath.Walk(f.cfg.Path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Ignore files that are directories or not yaml files
		ext := filepath.Ext(info.Name())
		if info.IsDir() || (ext != ".yml" && ext != ".yaml") {
			return nil
		}

		_, err = config.LoadFile(f.cfg.Path + info.Name())
		if err != nil {
			return err
		}

		content, err := ioutil.ReadFile(f.cfg.Path + info.Name())
		if err != nil {
			return err
		}

		// The file name must correspond to the user tenant ID
		user := strings.TrimSuffix(info.Name(), ext)

		configs[user] = alerts.AlertConfigDesc{
			User:      user,
			RawConfig: string(content),
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return configs, nil
}
