package alertmanager

import (
	"context"
	"flag"
	"fmt"

	"github.com/cortexproject/cortex/pkg/alertmanager/storage"
	"github.com/cortexproject/cortex/pkg/storage/clients/configdb"
	"github.com/cortexproject/cortex/pkg/storage/clients/gcp"
	"github.com/cortexproject/cortex/pkg/util/usertracker"
)

// AlertStoreConfig configures the alertmanager backend
type AlertStoreConfig struct {
	Type string `yaml:"type"`

	ConfigDB configdb.Config

	GCS     gcp.GCSConfig
	Tracker usertracker.Config
}

// RegisterFlags registers flags.
func (cfg *AlertStoreConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.ConfigDB.RegisterFlagsWithPrefix("alertmanager", f)
	cfg.GCS.RegisterFlagsWithPrefix("alertmanager.store.", f)
	cfg.Tracker.RegisterFlagsWithPrefix("alertmanager.", f)
	f.StringVar(&cfg.Type, "alertmanager.storage.type", "configdb", "Method to use for backend rule storage (configdb, gcs)")
}

// NewAlertStore returns a new rule storage backend poller and store
func NewAlertStore(cfg AlertStoreConfig) (storage.AlertPoller, storage.AlertStore, error) {
	var (
		alertStore storage.AlertStore
		err        error
	)
	switch cfg.Type {
	case "configdb":
		poller, err := configdb.New(cfg.ConfigDB)
		return poller, nil, err
	case "gcs":
		alertStore, err = gcp.NewGCSClient(context.Background(), cfg.GCS)
	default:
		return nil, nil, fmt.Errorf("Unrecognized rule storage mode %v, choose one of: configdb, gcs", cfg.Type)
	}

	tracker, err := usertracker.NewTracker(cfg.Tracker)
	if err != nil {
		return nil, nil, err
	}

	p, err := newTrackedAlertPoller(tracker, alertStore)

	return p, p.trackedAlertStore(), err
}
