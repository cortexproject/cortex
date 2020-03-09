package util

import (
	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/pkg/util/services"
)

// ServiceFailureWatcher waits for service failures, and passed them to the channel.
type ServiceFailureWatcher struct {
	ch chan error
}

func NewServiceFailureWatcher() *ServiceFailureWatcher {
	return &ServiceFailureWatcher{ch: make(chan error)}
}

// Returns channel for this watcher. If watcher is nil, returns nil channel.
func (w *ServiceFailureWatcher) Chan() <-chan error {
	if w == nil {
		return nil
	}
	return w.ch
}

func (w *ServiceFailureWatcher) WatchService(service services.Service) {
	service.AddListener(services.NewListener(nil, nil, nil, nil, func(from services.State, failure error) {
		w.ch <- errors.Wrapf(failure, "service %v failed", service)
	}))
}

func (w *ServiceFailureWatcher) WatchManager(manager *services.Manager) {
	manager.AddListener(services.NewManagerListener(nil, nil, func(service services.Service) {
		w.ch <- errors.Wrapf(service.FailureCase(), "service %v failed", service)
	}))
}
