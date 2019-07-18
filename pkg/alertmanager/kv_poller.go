package alertmanager

import (
	"context"

	"github.com/cortexproject/cortex/pkg/alertmanager/storage"
	"github.com/cortexproject/cortex/pkg/util/usertracker"
)

// trackedAlertPoller checks for updated user configs and
// retreives the updated configuration from the backend
type trackedAlertPoller struct {
	tracker *usertracker.Tracker
	store   storage.AlertStore

	initialized bool
}

func newTrackedAlertPoller(tracker *usertracker.Tracker, store storage.AlertStore) (*trackedAlertPoller, error) {
	return &trackedAlertPoller{
		tracker: tracker,
		store:   store,

		initialized: false,
	}, nil
}

func (p *trackedAlertPoller) trackedAlertStore() *trackedAlertStore {
	return &trackedAlertStore{
		tracker: p.tracker,
		store:   p.store,
	}
}

// PollAlerts returns the alerts changed since the last poll
// All alert configurations are returned on the first poll
func (p *trackedAlertPoller) PollAlerts(ctx context.Context) (map[string]storage.AlertConfig, error) {
	updatedConfigs := map[string]storage.AlertConfig{}

	// First poll will return all rule groups
	if !p.initialized {
		p.initialized = true
		return p.store.ListAlertConfigs(ctx)
	}

	// Get the changed users from the user update tracker
	users := p.tracker.GetUpdatedUsers(ctx)

	// Retreive user configuration from the rule store
	// TODO: Add Retry logic for failed requests
	// TODO: store users that were failed to be updated and reattempt to retrieve on the next poll
	for _, u := range users {
		cfg, err := p.store.GetAlertConfig(ctx, u)
		if err != nil {
			return nil, err
		}

		updatedConfigs[u] = cfg
	}

	return updatedConfigs, nil
}

func (p *trackedAlertPoller) Stop() {
	p.tracker.Stop()
}

type trackedAlertStore struct {
	tracker *usertracker.Tracker
	store   storage.AlertStore
}

// ListAlertConfigs passes through to the embedded alert store
func (w *trackedAlertStore) ListAlertConfigs(ctx context.Context) (map[string]storage.AlertConfig, error) {
	return w.store.ListAlertConfigs(ctx)
}

// GetAlertConfig passes through to the embedded alert store
func (w *trackedAlertStore) GetAlertConfig(ctx context.Context, id string) (storage.AlertConfig, error) {
	return w.store.GetAlertConfig(ctx, id)
}

// SetAlertConfig passes through to the embedded alert store, and tracks a user change
func (w *trackedAlertStore) SetAlertConfig(ctx context.Context, id string, cfg storage.AlertConfig) error {
	err := w.store.SetAlertConfig(ctx, id, cfg)
	if err != nil {
		return err
	}

	return w.tracker.UpdateUser(ctx, id)
}

// DeleteAlertConfig passes through to the embedded alert store, and tracks a user change
func (w *trackedAlertStore) DeleteAlertConfig(ctx context.Context, id string) error {
	err := w.store.DeleteAlertConfig(ctx, id)
	if err != nil {
		return err
	}

	return w.tracker.UpdateUser(ctx, id)
}
