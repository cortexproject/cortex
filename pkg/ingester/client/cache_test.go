package client

import (
	fmt "fmt"
	"testing"

	"google.golang.org/grpc/health/grpc_health_v1"
)

func (i mockIngester) Close() error {
	return nil
}

func TestIngesterCache(t *testing.T) {
	buildCount := 0
	factory := func(addr string, _ Config) (IngesterClient, error) {
		if addr == "bad" {
			return nil, fmt.Errorf("Fail")
		}
		buildCount++
		return mockIngester{happy: true, status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
	}
	cache := NewIngesterClientCache(factory, Config{})

	cache.GetClientFor("1")
	if buildCount != 1 {
		t.Errorf("Did not create client")
	}

	cache.GetClientFor("1")
	if buildCount != 1 {
		t.Errorf("Created client that should have been cached")
	}

	cache.GetClientFor("2")
	if cache.Count() != 2 {
		t.Errorf("Expected Count() = 2, got %d", cache.Count())
	}

	cache.RemoveClientFor("1")
	if cache.Count() != 1 {
		t.Errorf("Expected Count() = 1, got %d", cache.Count())
	}

	cache.GetClientFor("1")
	if buildCount != 3 || cache.Count() != 2 {
		t.Errorf("Did not re-create client correctly")
	}

	_, err := cache.GetClientFor("bad")
	if err == nil {
		t.Errorf("Bad create should have thrown an error")
	}
	if cache.Count() != 2 {
		t.Errorf("Bad create should not have been added to cache")
	}

	addrs := cache.RegisteredAddresses()
	if len(addrs) != cache.Count() {
		t.Errorf("Lengths of registered addresses and cache.Count() do not match")
	}
}
