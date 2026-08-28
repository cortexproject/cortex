package grpcutil

import (
	"context"
	"errors"
	"net"
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

func stubSRVLookup(t *testing.T, srv func(ctx context.Context, service, proto, name string) (string, []*net.SRV, error)) {
	t.Helper()
	orig := lookupSRV
	lookupSRV = srv
	t.Cleanup(func() { lookupSRV = orig })
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

func TestDNSWatcher_Lookup_SRVTargetTransientFailureRetainsCache(t *testing.T) {
	stubSRVLookup(t, func(context.Context, string, string, string) (string, []*net.SRV, error) {
		return "", []*net.SRV{
			{Target: "target-1.example.com", Port: 9095},
			{Target: "target-2.example.com", Port: 9095},
		}, nil
	})
	stubLookups(t, func(context.Context, string) ([]string, error) {
		return nil, errors.New("unable to resolve address")
	})

	w := newTestDNSWatcher()
	w.service = "grpc"
	w.curAddrs = map[string]*Update{"1.2.3.4:9095": {Addr: "1.2.3.4:9095"}}

	result := w.lookup()

	assert.Nil(t, result)
	assert.Equal(t, map[string]*Update{"1.2.3.4:9095": {Addr: "1.2.3.4:9095"}}, w.curAddrs)
}

func TestDNSWatcher_Lookup_SRVPartialTargetSuccessUpdatesCache(t *testing.T) {
	stubSRVLookup(t, func(context.Context, string, string, string) (string, []*net.SRV, error) {
		return "", []*net.SRV{
			{Target: "target-1.example.com", Port: 9095},
			{Target: "target-2.example.com", Port: 9095},
		}, nil
	})
	stubLookups(t, func(_ context.Context, host string) ([]string, error) {
		switch host {
		case "target-1.example.com":
			return []string{"5.6.7.8"}, nil
		case "target-2.example.com":
			return nil, errors.New("unable to resolve address")
		default:
			t.Fatalf("unexpected host lookup: %s", host)
			return nil, nil
		}
	})

	w := newTestDNSWatcher()
	w.service = "grpc"
	w.curAddrs = map[string]*Update{"1.2.3.4:9095": {Addr: "1.2.3.4:9095"}}

	result := w.lookup()

	assert.ElementsMatch(t, []*Update{
		{Op: Delete, Addr: "1.2.3.4:9095"},
		{Op: Add, Addr: "5.6.7.8:9095"},
	}, result)
	assert.Equal(t, map[string]*Update{"5.6.7.8:9095": {Addr: "5.6.7.8:9095"}}, w.curAddrs)
}

func TestDNSWatcher_Lookup_EmptySRVResultClearsCache(t *testing.T) {
	stubSRVLookup(t, func(context.Context, string, string, string) (string, []*net.SRV, error) {
		return "", []*net.SRV{}, nil
	})
	stubLookups(t, func(_ context.Context, host string) ([]string, error) {
		t.Fatalf("unexpected host lookup: %s", host)
		return nil, nil
	})

	w := newTestDNSWatcher()
	w.service = "grpc"
	w.curAddrs = map[string]*Update{"1.2.3.4:9095": {Addr: "1.2.3.4:9095"}}

	result := w.lookup()

	assert.Equal(t, []*Update{{Op: Delete, Addr: "1.2.3.4:9095"}}, result)
	assert.NotNil(t, w.curAddrs)
	assert.Empty(t, w.curAddrs)
}

func TestDNSWatcher_Lookup_SRVFailureFallsBackToHost(t *testing.T) {
	stubSRVLookup(t, func(context.Context, string, string, string) (string, []*net.SRV, error) {
		return "", nil, errors.New("unable to resolve SRV record")
	})
	stubLookups(t, func(_ context.Context, host string) ([]string, error) {
		assert.Equal(t, "myhost", host)
		return []string{"5.6.7.8"}, nil
	})

	w := newTestDNSWatcher()
	w.service = "grpc"

	result := w.lookup()

	assert.Equal(t, []*Update{{Op: Add, Addr: "5.6.7.8:80"}}, result)
	assert.Equal(t, map[string]*Update{"5.6.7.8:80": {Addr: "5.6.7.8:80"}}, w.curAddrs)
}

func TestDNSWatcher_Lookup_SRVAllTargetsFailFallsBackToHost(t *testing.T) {
	stubSRVLookup(t, func(context.Context, string, string, string) (string, []*net.SRV, error) {
		return "", []*net.SRV{
			{Target: "target-1.example.com", Port: 9095},
		}, nil
	})
	stubLookups(t, func(_ context.Context, host string) ([]string, error) {
		// The SRV target lookup fails, but the plain A record lookup on the
		// watcher's host succeeds: the resolved addresses must then come from
		// the A record fallback, using the watcher's port.
		if host == "target-1.example.com" {
			return nil, errors.New("unable to resolve address")
		}
		assert.Equal(t, "myhost", host)
		return []string{"5.6.7.8"}, nil
	})

	w := newTestDNSWatcher()
	w.service = "grpc"
	w.curAddrs = map[string]*Update{"1.2.3.4:9095": {Addr: "1.2.3.4:9095"}}

	result := w.lookup()

	assert.ElementsMatch(t, []*Update{
		{Op: Delete, Addr: "1.2.3.4:9095"},
		{Op: Add, Addr: "5.6.7.8:80"},
	}, result)
	assert.Equal(t, map[string]*Update{"5.6.7.8:80": {Addr: "5.6.7.8:80"}}, w.curAddrs)
}

func TestDNSWatcher_Lookup_SRVTargetDotClearsCache(t *testing.T) {
	stubSRVLookup(t, func(context.Context, string, string, string) (string, []*net.SRV, error) {
		// RFC 2782: a single SRV record with target "." means the service is
		// decidedly not available at this domain.
		return "", []*net.SRV{{Target: ".", Port: 9095}}, nil
	})
	stubLookups(t, func(_ context.Context, host string) ([]string, error) {
		t.Fatalf("unexpected host lookup: %s", host)
		return nil, nil
	})

	w := newTestDNSWatcher()
	w.service = "grpc"
	w.curAddrs = map[string]*Update{"1.2.3.4:9095": {Addr: "1.2.3.4:9095"}}

	result := w.lookup()

	assert.Equal(t, []*Update{{Op: Delete, Addr: "1.2.3.4:9095"}}, result)
	assert.Empty(t, w.curAddrs)
}

func TestDNSWatcher_Lookup_SRVTargetFailureAcrossPolls(t *testing.T) {
	stubSRVLookup(t, func(context.Context, string, string, string) (string, []*net.SRV, error) {
		return "", []*net.SRV{{Target: "target-1.example.com", Port: 9095}}, nil
	})
	targetAddrs := []string{"1.2.3.4"}
	targetErr := error(nil)
	stubLookups(t, func(_ context.Context, host string) ([]string, error) {
		if host == "target-1.example.com" {
			return targetAddrs, targetErr
		}
		// The plain host A record fallback also fails during the outage.
		return nil, errors.New("unable to resolve address")
	})

	w := newTestDNSWatcher()
	w.service = "grpc"

	// The first poll succeeds and seeds the cache.
	result := w.lookup()
	assert.ElementsMatch(t, []*Update{{Op: Add, Addr: "1.2.3.4:9095"}}, result)

	// The target keeps failing for two consecutive polls: no updates are
	// emitted and the cached endpoint is retained.
	targetAddrs, targetErr = nil, errors.New("unable to resolve address")
	for range 2 {
		assert.Nil(t, w.lookup())
		assert.Equal(t, map[string]*Update{"1.2.3.4:9095": {Addr: "1.2.3.4:9095"}}, w.curAddrs)
	}

	// Resolution recovers with the same address: no churn is emitted.
	targetAddrs, targetErr = []string{"1.2.3.4"}, nil
	assert.Empty(t, w.lookup())
	assert.Equal(t, map[string]*Update{"1.2.3.4:9095": {Addr: "1.2.3.4:9095"}}, w.curAddrs)
}
