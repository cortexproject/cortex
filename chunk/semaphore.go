package chunk

// Semaphore allows users to control the level of concurrency of the Put function.
type Semaphore interface {
	Acquire()
	Release()
}

type semaphore chan struct{}

// NewSemaphore makes a new Semaphore
func NewSemaphore(size int) Semaphore {
	s := semaphore(make(chan struct{}, size))
	for i := 0; i < size; i++ {
		s.Release()
	}
	return s
}

func (s semaphore) Acquire() {
	<-s
}

func (s semaphore) Release() {
	s <- struct{}{}
}

type noopSemaphore int

func (noopSemaphore) Acquire() {}

func (noopSemaphore) Release() {}

// NoopSemaphore is a no-op semaphore
const NoopSemaphore = noopSemaphore(0)
