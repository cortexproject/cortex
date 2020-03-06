package services

import "sync"

type serviceListeners struct {
	mu        sync.Mutex
	listeners []chan func(l Listener)
}

func newServiceListeners() *serviceListeners {
	return &serviceListeners{}
}

// each added listener starts a new goroutine to handle notifications.
// if no more notifications are expected, don't add the listener.
func (ls *serviceListeners) add(listener Listener) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	// There are max 4 state transitions. We use buffer to avoid blocking the sender,
	// which holds service lock.
	ch := make(chan func(l Listener), 4)
	ls.listeners = append(ls.listeners, ch)

	// each listener has its own goroutine, processing events.
	go func() {
		for lfn := range ch {
			lfn(listener)
		}
	}()
}

func (ls *serviceListeners) notify(lfn func(l Listener), closeChan bool) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	for _, ch := range ls.listeners {
		ch <- lfn
		if closeChan {
			close(ch)
		}
	}
}
