package ingester

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa/interp/testdata/src/errors"
)

func TestBasicErrorFuture(t *testing.T) {
	ef := NewErrorFuture()

	done, err := ef.Get()
	require.False(t, done)
	require.NoError(t, err)

	ef.Finish(nil)

	done, err = ef.Get()
	require.True(t, done)
	require.NoError(t, err)
}

func TestAsyncErrorFutureWithSuccess(t *testing.T) {
	ef := NewErrorFuture()
	go func() {
		time.Sleep(100 * time.Millisecond)
		ef.Finish(nil)
	}()

	done, err := ef.WaitAndGet(context.Background())
	require.True(t, done)
	require.NoError(t, err)
}

func TestAsyncErrorFutureWithError(t *testing.T) {
	initError := errors.New("test error")
	ef := NewErrorFuture()
	go func() {
		time.Sleep(100 * time.Millisecond)
		ef.Finish(initError)
	}()

	done, err := ef.WaitAndGet(context.Background())
	require.True(t, done)
	require.Equal(t, initError, err)
}

func TestAsyncErrorFutureWithTimeout(t *testing.T) {
	ef := NewErrorFuture()
	go func() {
		time.Sleep(500 * time.Millisecond)
		ef.Finish(nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done, err := ef.WaitAndGet(ctx)
	require.False(t, done)
	require.Equal(t, context.DeadlineExceeded, err)

	// wait for finish
	done, err = ef.WaitAndGet(context.Background())
	require.True(t, done)
	require.Equal(t, nil, err)
}

func TestDoubleFinishPanics(t *testing.T) {
	var recovered interface{}

	defer func() {
		// make sure we have recovered from panic.
		if recovered == nil {
			t.Fatal("no recovered panic")
		}
	}()

	defer func() {
		recovered = recover()
	}()

	ef := NewErrorFuture()
	ef.Finish(nil)
	ef.Finish(nil)
	// if we get here, there was no panic.
	t.Fatal("no panic")
}
