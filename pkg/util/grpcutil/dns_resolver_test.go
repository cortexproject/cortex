package grpcutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func stubLookups(t *testing.T, host func(ctx context.Context, host string) ([]string, error)) {
	t.Helper()
	orig := lookupHost
	lookupHost = host
	t.Cleanup(func() { lookupHost = orig })
}

func newTestDNSWatcher() *dnsWatcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &dnsWatcher{
		r:      &Resolver{freq: time.Hour},
		logger: log.NewNopLogger(),
		host:   "myhost",
		port:   "80",
		ctx:    ctx,
		cancel: cancel,
		t:      time.NewTimer(0),
	}
}

func TestDNSWatcher_Next(t *testing.T) {
	stubLookups(t, func(context.Context, string) ([]string, error) {
		return []string{"1.2.3.4"}, nil
	})

	w := newTestDNSWatcher()
	updates, err := w.Next()
	require.NoError(t, err)
	assert.ElementsMatch(t, []*Update{{Op: Add, Addr: "1.2.3.4:80"}}, updates)
	assert.Equal(t, map[string]*Update{"1.2.3.4:80": {Addr: "1.2.3.4:80"}}, w.curAddrs)

	// Lookup returns only 5.6.7.8, removing 1.2.3.4
	stubLookups(t, func(context.Context, string) ([]string, error) {
		return []string{"5.6.7.8"}, nil
	})

	// Fire the timer again so Next() performs another lookup immediately.
	w.t.Reset(0)
	updates, err = w.Next()
	require.NoError(t, err)
	assert.ElementsMatch(t, []*Update{
		{Op: Delete, Addr: "1.2.3.4:80"},
		{Op: Add, Addr: "5.6.7.8:80"},
	}, updates)
	assert.Equal(t, map[string]*Update{"5.6.7.8:80": {Addr: "5.6.7.8:80"}}, w.curAddrs)
}

func TestDNSWatcher_Lookup_TransientFailureRetainsCache(t *testing.T) {
	// First resolution succeeds and populates curAddrs.
	stubLookups(t, func(context.Context, string) ([]string, error) {
		return []string{"1.2.3.4"}, nil
	})

	w := newTestDNSWatcher()
	result := w.lookup()
	assert.ElementsMatch(t, []*Update{{Op: Add, Addr: "1.2.3.4:80"}}, result)
	require.Equal(t, map[string]*Update{"1.2.3.4:80": {Addr: "1.2.3.4:80"}}, w.curAddrs)

	// Fail next lookup
	stubLookups(t, func(context.Context, string) ([]string, error) {
		return nil, errors.New("unable to resolve address")
	})

	result = w.lookup()
	assert.Nil(t, result)
	assert.Equal(t, map[string]*Update{"1.2.3.4:80": {Addr: "1.2.3.4:80"}}, w.curAddrs)
}
