package local

import (
	"context"
	"flag"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/cortexproject/cortex/pkg/alertmanager/alerts"
	"github.com/prometheus/alertmanager/config"
)

// FileAlertStoreConfig configures a static file alertmanager store
type FileAlertStoreConfig struct {
	Path string `yaml:"path"`
}

// RegisterFlags registers flags related to the alertmanager file store
func (cfg *FileAlertStoreConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Path, "alertmanager.storage.local.path", "/etc/cortex/alertmanager_configs/", "Path at which alertmanager configurations are stored.")
}

// FileAlertStore is used to load user alertmanager configs from a local disk
type FileAlertStore struct {
	cfg FileAlertStoreConfig
}

// NewFileAlertStore returns a new file alert store.
func NewFileAlertStore(cfg FileAlertStoreConfig) (*FileAlertStore, error) {
	return &FileAlertStore{cfg}, nil
}

// ListAlertConfigs returns a list of each users alertmanager config.
func (f *FileAlertStore) ListAlertConfigs(ctx context.Context) (map[string]alerts.AlertConfigDesc, error) {
	var configs map[string]alerts.AlertConfigDesc

	err := filepath.Walk(f.cfg.Path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		ext := filepath.Ext(info.Name())

		if !info.IsDir() && (ext == ".yml" || ext == ".yaml") {
			_, err := config.LoadFile(f.cfg.Path + info.Name())
			if err != nil {
				return err
			}

			content, err := ioutil.ReadFile(f.cfg.Path + info.Name())
			if err != nil {
				return err
			}

			user := strings.TrimSuffix(info.Name(), ext)

			configs[user] = alerts.AlertConfigDesc{
				User:      user,
				RawConfig: string(content),
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return configs, nil
}
