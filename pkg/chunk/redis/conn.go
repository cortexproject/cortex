package redis

import (
	"context"
	"hash/fnv"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/util"
)

var (
	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "redis_request_duration_seconds",
		Help:      "Time spent doing redis requests.",
		Buckets:   []float64{.025, .05, .1, .25, .5, 1, 2},
	}, []string{"operation", "server"})
)

func init() {
	prometheus.MustRegister(requestDuration)
}

// Pool holds *redis.Pool instances, hashed consistently.
type Pool struct {
	addr  []string
	Conns map[string]*redis.Pool
}

// NewPool creates a new Pool instance taking the config from this dialer object.
func NewPool(cfg Config) *Pool {
	p := &Pool{
		addr:  cfg.Redis,
		Conns: map[string]*redis.Pool{},
	}

	for _, addr := range p.addr {
		if _, ok := p.Conns[addr]; ok {
			continue
		}

		p.Conns[addr] = &redis.Pool{
			MaxIdle:      cfg.MaxIdle,
			MaxActive:    cfg.MaxActive,
			Wait:         true,
			IdleTimeout:  cfg.IdleTimeout,
			Dial:         dialerFuncFor(addr),
			TestOnBorrow: testFunc,
		}
	}

	return p
}

// MetricConn is a redis.Conn that has been wrapped with a metric instrument
type MetricConn struct {
	c redis.Conn
	a string
}

// Err implements the redis.Conn interface
func (r *MetricConn) Err() error { return r.c.Err() }

// Close implements the redis.Conn interface
func (r *MetricConn) Close() error { return r.c.Close() }

// Send implements the redis.Conn interface
func (r *MetricConn) Send(c string, args ...interface{}) error { return r.c.Send(c, args...) }

// Flush implements the redis.Conn interface
func (r *MetricConn) Flush() error { return r.c.Flush() }

// Receive implements the redis.Conn interface
func (r *MetricConn) Receive() (interface{}, error) { return r.c.Receive() }

// Do implements the redis.Conn interface but wraps a histogram metric around the call
func (r *MetricConn) Do(c string, args ...interface{}) (interface{}, error) {
	defer func(start time.Time) {
		requestDuration.WithLabelValues(c, r.a).Observe(time.Since(start).Seconds())
	}(time.Now())
	return r.c.Do(c, args...)
}

// Get a connection for the given key (hashed off of Addresses)
func (r Pool) Get(ctx context.Context, key string) (redis.Conn, error) {
	addr := r.getAddr(key)
	c, err := r.Conns[addr].GetContext(ctx)
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to hash redis key", "key", key, "err", err)
		return nil, err
	}

	return &MetricConn{
		a: addr,
		c: c,
	}, nil
}

// getAddr will hash the key and return the node it maps to
func (r Pool) getAddr(key string) string {
	hasher := fnv.New64a()
	_, err := hasher.Write([]byte(key))
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to hash redis key", "key", key, "err", err)
		return r.addr[0]
	}

	k := hasher.Sum64()
	return r.addr[jumpHash(k, len(r.addr))]
}

// Close closes all connections
func (r Pool) Close() error {
	for _, c := range r.Conns {
		c.Close()
	}
	return nil
}

func dialerFuncFor(addr string) func() (redis.Conn, error) {
	return func() (redis.Conn, error) {
		level.Debug(util.Logger).Log("msg", "creating redis connection", "addr", addr)
		c, err := redis.Dial("tcp", addr)
		if err != nil {
			level.Error(util.Logger).Log("msg", "failed to create redis connection", "err", err)
		}
		return c, err
	}
}

func testFunc(c redis.Conn, t time.Time) error {
	if time.Since(t) < time.Minute {
		return nil
	}

	_, err := c.Do("PING")
	if err != nil {
		return errors.Wrap(err, "could not validate Pool connection")
	}

	return nil
}

// jumpHash described in A Fast, Minimal Memory, Consistent Hash Algorithm
// https://arxiv.org/pdf/1406.2294.pdf
// Takes a key and the number of buckets(nodes) to hash into. Returns the node key should be placed in
func jumpHash(key uint64, numBuckets int) int32 {
	var b int64 = -1
	for j := int64(0); j < int64(numBuckets); {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int32(b)
}
