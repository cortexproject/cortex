package services

import (
	"context"
	"errors"
	"sync"
)

type noopService struct {
	listeners *serviceListeners

	stateMu sync.Mutex
	state   State

	runningCh, terminatedCh chan struct{}
}

// This service does nothing, except going through state transitions based on Start/Stop calls.
// It will only be in New, Running or Terminated state, although it also reports Starting/Stopping to listeners.
// It will never fail. It also doesn't react on context passed to it from outside.
// It is useful when implementing some kind of "noop" type, which also needs to be a Service.
func NewNoopService() Service {
	return &noopService{
		state:        New,
		listeners:    newServiceListeners(),
		runningCh:    make(chan struct{}),
		terminatedCh: make(chan struct{}),
	}
}

func (n *noopService) StartAsync(ctx context.Context) error {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()

	if n.state != New {
		return errors.New("not New")
	}

	n.state = Running
	n.listeners.notify(func(l Listener) { l.Starting(); l.Running() }, false)
	close(n.runningCh)
	return nil
}

func (n *noopService) StopAsync() {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()

	if n.state == New {
		n.listeners.notify(func(l Listener) { l.Terminated(New) }, true)
		close(n.runningCh)
		close(n.terminatedCh)
	} else if n.state == Running {
		n.listeners.notify(func(l Listener) {
			l.Stopping(Running)
			l.Terminated(Stopping)
		}, true)
		close(n.terminatedCh)
	}

	n.state = Terminated
}

func (n *noopService) AwaitRunning(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-n.runningCh:
		s := n.State()
		if s != Running {
			return invalidServiceStateError(s, Running)
		}
		return nil
	}
}

func (n *noopService) AwaitTerminated(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-n.terminatedCh:
		return nil
	}
}

func (n *noopService) FailureCase() error {
	return nil
}

func (n *noopService) State() State {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()

	return n.state
}

func (n *noopService) AddListener(listener Listener) {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()

	if n.state == Terminated {
		return
	}

	n.listeners.add(listener)
}
