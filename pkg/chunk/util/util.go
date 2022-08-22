package util

import (
	"context"
	"io"
)

// ReadCloserWithContextCancelFunc helps with cancelling the context when closing a ReadCloser.
// NOTE: The consumer of ReadCloserWithContextCancelFunc should always call the Close method when it is done reading which otherwise could cause a resource leak.
type ReadCloserWithContextCancelFunc struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func NewReadCloserWithContextCancelFunc(readCloser io.ReadCloser, cancel context.CancelFunc) io.ReadCloser {
	return ReadCloserWithContextCancelFunc{
		ReadCloser: readCloser,
		cancel:     cancel,
	}
}

func (r ReadCloserWithContextCancelFunc) Close() error {
	defer r.cancel()
	return r.ReadCloser.Close()
}
