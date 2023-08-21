package tripperware

import (
	"net/http"
	"time"
)

type MockLimits struct {
	maxQueryLookback  time.Duration
	maxQueryLength    time.Duration
	maxCacheFreshness time.Duration
	ShardSize         int
}

func (m MockLimits) MaxQueryLookback(string) time.Duration {
	return m.maxQueryLookback
}

func (m MockLimits) MaxQueryLength(string) time.Duration {
	return m.maxQueryLength
}

func (MockLimits) MaxQueryParallelism(string) int {
	return 14 // Flag default.
}

func (m MockLimits) MaxCacheFreshness(string) time.Duration {
	return m.maxCacheFreshness
}

func (m MockLimits) QueryVerticalShardSize(userID string) int {
	return m.ShardSize
}

type SingleHostRoundTripper struct {
	Host string
	Next http.RoundTripper
}

func (s SingleHostRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	r.URL.Scheme = "http"
	r.URL.Host = s.Host
	return s.Next.RoundTrip(r)
}
