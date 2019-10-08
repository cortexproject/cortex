package consul

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/kit/log/level"
	consul "github.com/hashicorp/consul/api"
	"github.com/hashicorp/go-cleanhttp"
	"github.com/weaveworks/common/instrument"
	"golang.org/x/time/rate"

	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/util"
)

const (
	longPollDuration = 10 * time.Second
)

var (
	writeOptions = &consul.WriteOptions{}

	// ErrNotFound is returned by ConsulClient.Get.
	ErrNotFound = fmt.Errorf("Not found")

	backoffConfig = util.BackoffConfig{
		MinBackoff: 1 * time.Second,
		MaxBackoff: 1 * time.Minute,
	}
)

// Config to create a ConsulClient
type Config struct {
	Host              string
	ACLToken          string
	HTTPClientTimeout time.Duration
	ConsistentReads   bool
	WatchKeyRate      float64 // Zero disables rate limit
	WatchKeyBurst     int     // Burst when doing rate-limit, defaults to 1
}

type kv interface {
	CAS(p *consul.KVPair, q *consul.WriteOptions) (bool, *consul.WriteMeta, error)
	Get(key string, q *consul.QueryOptions) (*consul.KVPair, *consul.QueryMeta, error)
	List(path string, q *consul.QueryOptions) (consul.KVPairs, *consul.QueryMeta, error)
	Put(p *consul.KVPair, q *consul.WriteOptions) (*consul.WriteMeta, error)
}

// Client is a KV.Client for Consul.
type Client struct {
	kv
	codec codec.Codec
	cfg   Config
}

// RegisterFlags adds the flags required to config this to the given FlagSet
// If prefix is not an empty string it should end with a period.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Host, prefix+"consul.hostname", "localhost:8500", "Hostname and port of Consul.")
	f.StringVar(&cfg.ACLToken, prefix+"consul.acltoken", "", "ACL Token used to interact with Consul.")
	f.DurationVar(&cfg.HTTPClientTimeout, prefix+"consul.client-timeout", 2*longPollDuration, "HTTP timeout when talking to Consul")
	f.BoolVar(&cfg.ConsistentReads, prefix+"consul.consistent-reads", true, "Enable consistent reads to Consul.")
	f.Float64Var(&cfg.WatchKeyRate, prefix+"consul.watch-rate", 0, "Rate limit when watching key or prefix in Consul, in requests per second. Zero disables the rate limit.")
	f.IntVar(&cfg.WatchKeyBurst, prefix+"consul.watch-burst", 1, "Burst size used in rate limit. Values less than 1 are treated as 1.")
}

// NewClient returns a new Client.
func NewClient(cfg Config, codec codec.Codec) (*Client, error) {
	client, err := consul.NewClient(&consul.Config{
		Address: cfg.Host,
		Token:   cfg.ACLToken,
		Scheme:  "http",
		HttpClient: &http.Client{
			Transport: cleanhttp.DefaultPooledTransport(),
			// See https://blog.cloudflare.com/the-complete-guide-to-golang-net-http-timeouts/
			Timeout: cfg.HTTPClientTimeout,
		},
	})
	if err != nil {
		return nil, err
	}
	c := &Client{
		kv:    consulMetrics{client.KV()},
		codec: codec,
		cfg:   cfg,
	}
	return c, nil
}

// CAS atomically modifies a value in a callback.
// If value doesn't exist you'll get nil as an argument to your callback.
func (c *Client) CAS(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	return instrument.CollectedRequest(ctx, "CAS loop", consulRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		return c.cas(ctx, key, f)
	})
}

func (c *Client) cas(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	var (
		index   = uint64(0)
		retries = 10
		retry   = true
	)
	for i := 0; i < retries; i++ {
		options := &consul.QueryOptions{
			AllowStale:        !c.cfg.ConsistentReads,
			RequireConsistent: c.cfg.ConsistentReads,
		}
		kvp, _, err := c.kv.Get(key, options.WithContext(ctx))
		if err != nil {
			level.Error(util.Logger).Log("msg", "error getting key", "key", key, "err", err)
			continue
		}
		var intermediate interface{}
		if kvp != nil {
			out, err := c.codec.Decode(kvp.Value)
			if err != nil {
				level.Error(util.Logger).Log("msg", "error decoding key", "key", key, "err", err)
				continue
			}
			// If key doesn't exist, index will be 0.
			index = kvp.ModifyIndex
			intermediate = out
		}

		intermediate, retry, err = f(intermediate)
		if err != nil {
			if !retry {
				return err
			}
			continue
		}

		// Treat the callback returning nil for intermediate as a decision to
		// not actually write to Consul, but this is not an error.
		if intermediate == nil {
			return nil
		}

		bytes, err := c.codec.Encode(intermediate)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error serialising value", "key", key, "err", err)
			continue
		}
		ok, _, err := c.kv.CAS(&consul.KVPair{
			Key:         key,
			Value:       bytes,
			ModifyIndex: index,
		}, writeOptions.WithContext(ctx))
		if err != nil {
			level.Error(util.Logger).Log("msg", "error CASing", "key", key, "err", err)
			continue
		}
		if !ok {
			level.Debug(util.Logger).Log("msg", "error CASing, trying again", "key", key, "index", index)
			continue
		}
		return nil
	}
	return fmt.Errorf("failed to CAS %s", key)
}

// WatchKey will watch a given key in consul for changes. When the value
// under said key changes, the f callback is called with the deserialised
// value. To construct the deserialised value, a factory function should be
// supplied which generates an empty struct for WatchKey to deserialise
// into. Values in Consul are assumed to be JSON. This function blocks until
// the context is cancelled.
func (c *Client) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	var (
		backoff = util.NewBackoff(ctx, backoffConfig)
		index   = uint64(0)
		limiter = c.createRateLimiter()
	)

	for backoff.Ongoing() {
		if limiter != nil {
			err := limiter.Wait(ctx)
			if err != nil {
				level.Error(util.Logger).Log("msg", "error while rate-limiting", "key", key, "err", err)
				backoff.Wait()
				continue
			}
		}

		queryOptions := &consul.QueryOptions{
			AllowStale:        !c.cfg.ConsistentReads,
			RequireConsistent: c.cfg.ConsistentReads,
			WaitIndex:         index,
			WaitTime:          longPollDuration,
		}

		kvp, meta, err := c.kv.Get(key, queryOptions.WithContext(ctx))
		if err != nil || kvp == nil {
			level.Error(util.Logger).Log("msg", "error getting path", "key", key, "err", err)
			backoff.Wait()
			continue
		}
		backoff.Reset()

		// See https://www.consul.io/api/features/blocking.html#implementation-details for logic behind these checks.
		if meta.LastIndex == 0 {
			// Don't just keep using index=0.
			// After blocking request, returned index must be at least 1.
			index = 1
		} else if meta.LastIndex < index {
			// Index reset.
			index = 0
		} else if index == meta.LastIndex {
			// Skip if the index is the same as last time, because the key value is
			// guaranteed to be the same as last time
			continue
		} else {
			index = meta.LastIndex
		}

		out, err := c.codec.Decode(kvp.Value)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error decoding key", "key", key, "err", err)
			continue
		}
		if !f(out) {
			return
		}
	}
}

// WatchPrefix will watch a given prefix in Consul for new keys and changes to existing keys under that prefix.
// When the value under said key changes, the f callback is called with the deserialised value.
// Values in Consul are assumed to be JSON. This function blocks until the context is cancelled.
func (c *Client) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	var (
		backoff = util.NewBackoff(ctx, backoffConfig)
		index   = uint64(0)
		limiter = c.createRateLimiter()
	)
	for backoff.Ongoing() {
		if limiter != nil {
			err := limiter.Wait(ctx)
			if err != nil {
				level.Error(util.Logger).Log("msg", "error while rate-limiting", "prefix", prefix, "err", err)
				backoff.Wait()
				continue
			}
		}

		queryOptions := &consul.QueryOptions{
			AllowStale:        !c.cfg.ConsistentReads,
			RequireConsistent: c.cfg.ConsistentReads,
			WaitIndex:         index,
			WaitTime:          longPollDuration,
		}

		kvps, meta, err := c.kv.List(prefix, queryOptions.WithContext(ctx))
		if err != nil || kvps == nil {
			level.Error(util.Logger).Log("msg", "error getting path", "prefix", prefix, "err", err)
			backoff.Wait()
			continue
		}
		backoff.Reset()
		// Skip if the index is the same as last time, because the key value is
		// guaranteed to be the same as last time
		if index == meta.LastIndex {
			continue
		}

		for _, kvp := range kvps {
			out, err := c.codec.Decode(kvp.Value)
			if err != nil {
				level.Error(util.Logger).Log("msg", "error decoding list of values for prefix:key", "prefix", prefix, "key", kvp.Key, "err", err)
				continue
			}
			// We should strip the prefix from the front of the key.
			key := strings.TrimPrefix(kvp.Key, prefix)
			if !f(key, out) {
				return
			}
		}
	}
}

// Get implements kv.Get.
func (c *Client) Get(ctx context.Context, key string) (interface{}, error) {
	options := &consul.QueryOptions{
		AllowStale:        !c.cfg.ConsistentReads,
		RequireConsistent: c.cfg.ConsistentReads,
	}
	kvp, _, err := c.kv.Get(key, options.WithContext(ctx))
	if err != nil {
		return nil, err
	} else if kvp == nil {
		return nil, nil
	}
	return c.codec.Decode(kvp.Value)
}

func (c *Client) createRateLimiter() *rate.Limiter {
	if c.cfg.WatchKeyRate <= 0 {
		return nil
	}
	burst := c.cfg.WatchKeyBurst
	if burst < 1 {
		burst = 1
	}
	return rate.NewLimiter(rate.Limit(c.cfg.WatchKeyRate), burst)
}
