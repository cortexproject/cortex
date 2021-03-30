package alertmanager

import (
	"context"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/alertmanager/cluster/clusterpb"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertspb"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore"
	"github.com/cortexproject/cortex/pkg/util/services"
)

const (
	defaultPersistInterval = 15 * time.Minute
	defaultPersistTimeout  = 30 * time.Second
)

type PersistableState interface {
	State
	GetFullState() (*clusterpb.FullState, error)
}

// statePersister periodically writes the alertmanager state to persistent storage.
type statePersister struct {
	services.Service

	state  PersistableState
	store  alertstore.AlertStore
	userID string
	logger log.Logger

	interval time.Duration
	timeout  time.Duration
}

// newStatePersister creates a new state persister.
func newStatePersister(userID string, state PersistableState, store alertstore.AlertStore, l log.Logger) *statePersister {

	s := &statePersister{
		state:    state,
		store:    store,
		userID:   userID,
		logger:   l,
		interval: defaultPersistInterval,
		timeout:  defaultPersistTimeout,
	}

	s.Service = services.NewBasicService(s.starting, s.running, nil)

	return s
}

func (s *statePersister) starting(ctx context.Context) error {
	// Waits until the state replicator is settled, so that state is not
	// persisted before obtaining some initial state.
	return s.state.WaitReady(ctx)
}

func (s *statePersister) running(ctx context.Context) error {
	level.Debug(s.logger).Log("msg", "started state persister", "user", s.userID, "interval", s.interval)

	ticker := time.NewTicker(s.interval)
	for {
		select {
		case <-ticker.C:
			if err := s.persist(ctx); err != nil {
				level.Error(s.logger).Log("msg", "failed to persist state", "user", s.userID, "err", err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (s *statePersister) persist(ctx context.Context) error {
	// Only the replica at position zero should write the state.
	if s.state.Position() != 0 {
		return nil
	}

	level.Debug(s.logger).Log("msg", "persisting state", "user", s.userID)

	fs, err := s.state.GetFullState()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	desc := alertspb.FullStateDesc{State: fs}
	if err := s.store.SetFullState(ctx, s.userID, desc); err != nil {
		return err
	}

	return nil
}
