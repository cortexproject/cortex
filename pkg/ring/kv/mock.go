package kv

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// The mockClient does not anything.
// This is used for testing only.
type mockClient struct{}

func buildMockClient(logger log.Logger) (Client, error) {
	level.Warn(logger).Log("msg", "created mockClient for testing only")
	return mockClient{}, nil
}

func (m mockClient) List(ctx context.Context, prefix string) ([]string, error) {
	return []string{}, nil
}

func (m mockClient) Get(ctx context.Context, key string) (any, error) {
	return "", nil
}

func (m mockClient) Delete(ctx context.Context, key string) error {
	return nil
}

func (m mockClient) CAS(ctx context.Context, key string, f func(in any) (out any, retry bool, err error)) error {
	return nil
}

func (m mockClient) WatchKey(ctx context.Context, key string, f func(any) bool) {
}

func (m mockClient) WatchPrefix(ctx context.Context, prefix string, f func(string, any) bool) {
}

func (m mockClient) LastUpdateTime(key string) time.Time {
	return time.Now().UTC()
}
