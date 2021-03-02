package objectclient

import (
	"bytes"
	"context"
	"io/ioutil"
	"path"

	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/runutil"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertspb"
	"github.com/cortexproject/cortex/pkg/chunk"
)

// Object Alert Storage Schema
// =======================
// Object Name: "alerts/<user_id>"
// Storage Format: Encoded AlertConfigDesc

const (
	alertPrefix = "alerts/"
)

// AlertStore allows cortex alertmanager configs to be stored using an object store backend.
type AlertStore struct {
	client chunk.ObjectClient
	logger log.Logger
}

// NewAlertStore returns a new AlertStore
func NewAlertStore(client chunk.ObjectClient, logger log.Logger) *AlertStore {
	return &AlertStore{
		client: client,
		logger: logger,
	}
}

// ListAlertConfigs implements alertstore.AlertStore.
func (a *AlertStore) ListAlertConfigs(ctx context.Context) (map[string]alertspb.AlertConfigDesc, error) {
	objs, _, err := a.client.List(ctx, alertPrefix, "")
	if err != nil {
		return nil, err
	}

	cfgs := map[string]alertspb.AlertConfigDesc{}

	for _, obj := range objs {
		cfg, err := a.getAlertConfig(ctx, obj.Key)
		if err != nil {
			return nil, err
		}
		cfgs[cfg.User] = cfg
	}

	return cfgs, nil
}

func (a *AlertStore) getAlertConfig(ctx context.Context, key string) (alertspb.AlertConfigDesc, error) {
	readCloser, err := a.client.GetObject(ctx, key)
	if err != nil {
		return alertspb.AlertConfigDesc{}, err
	}

	defer runutil.CloseWithLogOnErr(a.logger, readCloser, "close alert config reader")

	buf, err := ioutil.ReadAll(readCloser)
	if err != nil {
		return alertspb.AlertConfigDesc{}, err
	}

	config := alertspb.AlertConfigDesc{}
	err = config.Unmarshal(buf)
	if err != nil {
		return alertspb.AlertConfigDesc{}, err
	}

	return config, nil
}

// GetAlertConfig implements alertstore.AlertStore.
func (a *AlertStore) GetAlertConfig(ctx context.Context, user string) (alertspb.AlertConfigDesc, error) {
	cfg, err := a.getAlertConfig(ctx, path.Join(alertPrefix, user))
	if err == chunk.ErrStorageObjectNotFound {
		return cfg, alertspb.ErrNotFound
	}

	return cfg, err
}

// SetAlertConfig implements alertstore.AlertStore.
func (a *AlertStore) SetAlertConfig(ctx context.Context, cfg alertspb.AlertConfigDesc) error {
	cfgBytes, err := cfg.Marshal()
	if err != nil {
		return err
	}

	return a.client.PutObject(ctx, path.Join(alertPrefix, cfg.User), bytes.NewReader(cfgBytes))
}

// DeleteAlertConfig implements alertstore.AlertStore.
func (a *AlertStore) DeleteAlertConfig(ctx context.Context, user string) error {
	err := a.client.DeleteObject(ctx, path.Join(alertPrefix, user))
	if err == chunk.ErrStorageObjectNotFound {
		return nil
	}
	return err
}
