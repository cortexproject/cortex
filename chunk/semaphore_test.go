package chunk

import "testing"

func TestSemaphore(t *testing.T) {
	// A very dump test
	s := NewSemaphore(1)
	s.Acquire()
	s.Release()
}
