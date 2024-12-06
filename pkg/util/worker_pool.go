package util

import "sync"

// This code was based on: https://github.com/grpc/grpc-go/blob/66ba4b264d26808cb7af3c86eee66e843472915e/server.go

// serverWorkerResetThreshold defines how often the stack must be reset. Every
// N requests, by spawning a new goroutine in its place, a worker can reset its
// stack so that large stacks don't live in memory forever. 2^16 should allow
// each goroutine stack to live for at least a few seconds in a typical
// workload (assuming a QPS of a few thousand requests/sec).
const serverWorkerResetThreshold = 1 << 16

type AsyncExecutor interface {
	Submit(f func())
}

type noOpExecutor struct{}

func NewNoOpExecutor() AsyncExecutor {
	return &noOpExecutor{}
}

func (n noOpExecutor) Submit(f func()) {
	go f()
}

type workerPoolExecutor struct {
	serverWorkerChannel chan func()
	closeOnce           sync.Once
}

func NewWorkerPool(numWorkers int) AsyncExecutor {
	wp := &workerPoolExecutor{
		serverWorkerChannel: make(chan func()),
	}

	for i := 0; i < numWorkers; i++ {
		go wp.run()
	}

	return wp
}

func (s *workerPoolExecutor) Stop() {
	s.closeOnce.Do(func() {
		close(s.serverWorkerChannel)
	})
}

func (s *workerPoolExecutor) Submit(f func()) {
	select {
	case s.serverWorkerChannel <- f:
	default:
		go f()
	}
}

func (s *workerPoolExecutor) run() {
	for completed := 0; completed < serverWorkerResetThreshold; completed++ {
		f, ok := <-s.serverWorkerChannel
		if !ok {
			return
		}
		f()
	}
	go s.run()
}
