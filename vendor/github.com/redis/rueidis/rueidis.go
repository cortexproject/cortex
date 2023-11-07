// Package rueidis is a fast Golang Redis RESP3 client that does auto pipelining and supports client side caching.
package rueidis

import (
	"context"
	"crypto/tls"
	"errors"
	"math"
	"math/rand"
	"net"
	"runtime"
	"strings"
	"time"
)

const (
	// DefaultCacheBytes is the default value of ClientOption.CacheSizeEachConn, which is 128 MiB
	DefaultCacheBytes = 128 * (1 << 20)
	// DefaultRingScale is the default value of ClientOption.RingScaleEachConn, which results into having a ring of size 2^10 for each connection
	DefaultRingScale = 10
	// DefaultPoolSize is the default value of ClientOption.BlockingPoolSize
	DefaultPoolSize = 1000
	// DefaultDialTimeout is the default value of ClientOption.Dialer.Timeout
	DefaultDialTimeout = 5 * time.Second
	// DefaultTCPKeepAlive is the default value of ClientOption.Dialer.KeepAlive
	DefaultTCPKeepAlive = 1 * time.Second
	// DefaultReadBuffer is the default value of bufio.NewReaderSize for each connection, which is 0.5MiB
	DefaultReadBuffer = 1 << 19
	// DefaultWriteBuffer is the default value of bufio.NewWriterSize for each connection, which is 0.5MiB
	DefaultWriteBuffer = 1 << 19
)

var (
	// ErrClosing means the Client.Close had been called
	ErrClosing = errors.New("rueidis client is closing or unable to connect redis")
	// ErrNoAddr means the ClientOption.InitAddress is empty
	ErrNoAddr = errors.New("no alive address in InitAddress")
	// ErrNoCache means your redis does not support client-side caching and must set ClientOption.DisableCache to true
	ErrNoCache = errors.New("ClientOption.DisableCache must be true for redis not supporting client-side caching or not supporting RESP3")
	// ErrRESP2PubSubMixed means your redis does not support RESP3 and rueidis can't handle SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE in mixed case
	ErrRESP2PubSubMixed = errors.New("rueidis does not support SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE mixed with other commands in RESP2")
	// ErrDoCacheAborted means redis abort EXEC request or connection closed
	ErrDoCacheAborted = errors.New("failed to fetch the cache because EXEC was aborted by redis or connection closed")
)

// ClientOption should be passed to NewClient to construct a Client
type ClientOption struct {
	// TCP & TLS
	// Dialer can be used to customized how rueidis connect to a redis instance via TCP, including:
	// - Timeout, the default is DefaultDialTimeout
	// - KeepAlive, the default is DefaultTCPKeepAlive
	// The Dialer.KeepAlive interval is used to detect an unresponsive idle tcp connection.
	// OS takes at least (tcp_keepalive_probes+1)*Dialer.KeepAlive time to conclude an idle connection to be unresponsive.
	// For example: DefaultTCPKeepAlive = 1s and the default of tcp_keepalive_probes on Linux is 9.
	// Therefore, it takes at least 10s to kill an idle and unresponsive tcp connection on Linux by default.
	Dialer    net.Dialer
	TLSConfig *tls.Config

	// DialFn allows for a custom function to be used to create net.Conn connections
	DialFn func(string, *net.Dialer, *tls.Config) (conn net.Conn, err error)

	// NewCacheStoreFn allows a custom client side caching store for each connection
	NewCacheStoreFn NewCacheStoreFn

	// OnInvalidations is a callback function in case of client-side caching invalidation received.
	// Note that this function must be fast, otherwise other redis messages will be blocked.
	OnInvalidations func([]RedisMessage)

	// Sentinel options, including MasterSet and Auth options
	Sentinel SentinelOption

	// Redis AUTH parameters
	Username   string
	Password   string
	ClientName string

	// ClientSetInfo will assign various info attributes to the current connection
	ClientSetInfo []string

	// InitAddress point to redis nodes.
	// Rueidis will connect to them one by one and issue CLUSTER SLOT command to initialize the cluster client until success.
	// If len(InitAddress) == 1 and the address is not running in cluster mode, rueidis will fall back to the single client mode.
	// If ClientOption.Sentinel.MasterSet is set, then InitAddress will be used to connect sentinels
	// You can bypass this behaviour by using ClientOption.ForceSingleClient.
	InitAddress []string

	// ClientTrackingOptions will be appended to CLIENT TRACKING ON command when the connection is established.
	// The default is []string{"OPTIN"}
	ClientTrackingOptions []string

	SelectDB int

	// CacheSizeEachConn is redis client side cache size that bind to each TCP connection to a single redis instance.
	// The default is DefaultCacheBytes.
	CacheSizeEachConn int

	// RingScaleEachConn sets the size of the ring buffer in each connection to (2 ^ RingScaleEachConn).
	// The default is RingScaleEachConn, which results into having a ring of size 2^10 for each connection.
	// Reduce this value can reduce the memory consumption of each connection at the cost of potential throughput degradation.
	// Values smaller than 8 is typically not recommended.
	RingScaleEachConn int

	// ReadBufferEachConn is the size of the bufio.NewReaderSize for each connection, default to DefaultReadBuffer (0.5 MiB).
	ReadBufferEachConn int
	// WriteBufferEachConn is the size of the bufio.NewWriterSize for each connection, default to DefaultWriteBuffer (0.5 MiB).
	WriteBufferEachConn int

	// BlockingPoolSize is the size of the connection pool shared by blocking commands (ex BLPOP, XREAD with BLOCK).
	// The default is DefaultPoolSize.
	BlockingPoolSize int

	// PipelineMultiplex determines how many tcp connections used to pipeline commands to one redis instance.
	// The default for single and sentinel clients is 2, which means 4 connections (2^2).
	// For cluster client, PipelineMultiplex doesn't have any effect.
	PipelineMultiplex int

	// ConnWriteTimeout is applied net.Conn.SetWriteDeadline and periodic PING to redis
	// Since the Dialer.KeepAlive will not be triggered if there is data in the outgoing buffer,
	// ConnWriteTimeout should be set in order to detect local congestion or unresponsive redis server.
	// This default is ClientOption.Dialer.KeepAlive * (9+1), where 9 is the default of tcp_keepalive_probes on Linux.
	ConnWriteTimeout time.Duration

	// MaxFlushDelay when greater than zero pauses pipeline write loop for some time (not larger than MaxFlushDelay)
	// after each flushing of data to the connection. This gives pipeline a chance to collect more commands to send
	// to Redis. Adding this delay increases latency, reduces throughput – but in most cases may significantly reduce
	// application and Redis CPU utilization due to less executed system calls. By default, Rueidis flushes data to the
	// connection without extra delays. Depending on network latency and application-specific conditions the value
	// of MaxFlushDelay may vary, sth like 20 microseconds should not affect latency/throughput a lot but still
	// produce notable CPU usage reduction under load. Ref: https://github.com/redis/rueidis/issues/156
	MaxFlushDelay time.Duration

	// ShuffleInit is a handy flag that shuffles the InitAddress after passing to the NewClient() if it is true
	ShuffleInit bool
	// ClientNoTouch controls whether commands alter LRU/LFU stats
	ClientNoTouch bool
	// DisableRetry disables retrying read-only commands under network errors
	DisableRetry bool
	// DisableCache falls back Client.DoCache/Client.DoMultiCache to Client.Do/Client.DoMulti
	DisableCache bool
	// AlwaysPipelining makes rueidis.Client always pipeline redis commands even if they are not issued concurrently.
	AlwaysPipelining bool
	// AlwaysRESP2 makes rueidis.Client always uses RESP2, otherwise it will try using RESP3 first.
	AlwaysRESP2 bool
	//  ForceSingleClient force the usage of a single client connection, without letting the lib guessing
	//  if redis instance is a cluster or a single redis instance.
	ForceSingleClient bool

	// ReplicaOnly indicates that this client will only try to connect to readonly replicas of redis setup.
	// currently, it is only implemented for sentinel client
	ReplicaOnly bool

	// ClientNoEvict sets the client eviction mode for the current connection.
	// When turned on and client eviction is configured,
	// the current connection will be excluded from the client eviction process
	// even if we're above the configured client eviction threshold.
	ClientNoEvict bool
}

// SentinelOption contains MasterSet,
type SentinelOption struct {
	// TCP & TLS, same as ClientOption but for connecting sentinel
	Dialer    net.Dialer
	TLSConfig *tls.Config

	// MasterSet is the redis master set name monitored by sentinel cluster.
	// If this field is set, then ClientOption.InitAddress will be used to connect to sentinel cluster.
	MasterSet string

	// Redis AUTH parameters for sentinel
	Username   string
	Password   string
	ClientName string
}

// Client is the redis client interface for both single redis instance and redis cluster. It should be created from the NewClient()
type Client interface {
	CoreClient

	// DoCache is similar to Do, but it uses opt-in client side caching and requires a client side TTL.
	// The explicit client side TTL specifies the maximum TTL on the client side.
	// If the key's TTL on the server is smaller than the client side TTL, the client side TTL will be capped.
	//  client.Do(ctx, client.B().Get().Key("k").Cache(), time.Minute).ToString()
	// The above example will send the following command to redis if cache miss:
	//  CLIENT CACHING YES
	//  PTTL k
	//  GET k
	// The in-memory cache size is configured by ClientOption.CacheSizeEachConn.
	// The cmd parameter is recycled after passing into DoCache() and should not be reused.
	DoCache(ctx context.Context, cmd Cacheable, ttl time.Duration) (resp RedisResult)

	// DoMultiCache is similar to DoCache, but works with multiple cacheable commands across different slots.
	// It will first group commands by slots and will send only cache missed commands to redis.
	DoMultiCache(ctx context.Context, multi ...CacheableTTL) (resp []RedisResult)

	// Dedicated acquire a connection from the blocking connection pool, no one else can use the connection
	// during Dedicated. The main usage of Dedicated is CAS operation, which is WATCH + MULTI + EXEC.
	// However, one should try to avoid CAS operation but use Lua script instead, because occupying a connection
	// is not good for performance.
	Dedicated(fn func(DedicatedClient) error) (err error)

	// Dedicate does the same as Dedicated, but it exposes DedicatedClient directly
	// and requires user to invoke cancel() manually to put connection back to the pool.
	Dedicate() (client DedicatedClient, cancel func())

	// Nodes returns each redis node this client known as rueidis.Client. This is useful if you want to
	// send commands to some specific redis nodes in the cluster.
	Nodes() map[string]Client
}

// DedicatedClient is obtained from Client.Dedicated() and it will be bound to single redis connection and
// no other commands can be pipelined in to this connection during Client.Dedicated().
// If the DedicatedClient is obtained from cluster client, the first command to it must have a Key() to identify the redis node.
type DedicatedClient interface {
	CoreClient

	// SetPubSubHooks is an alternative way to processing Pub/Sub messages instead of using Receive.
	// SetPubSubHooks is non-blocking and allows users to subscribe/unsubscribe channels later.
	// Note that the hooks will be called sequentially but in another goroutine.
	// The return value will be either:
	//   1. an error channel, if the hooks passed in is not zero, or
	//   2. nil, if the hooks passed in is zero. (used for reset hooks)
	// In the former case, the error channel is guaranteed to be close when the hooks will not be called anymore,
	// and has at most one error describing the reason why the hooks will not be called anymore.
	// Users can use the error channel to detect disconnection.
	SetPubSubHooks(hooks PubSubHooks) <-chan error
}

// CoreClient is the minimum interface shared by the Client and the DedicatedClient.
type CoreClient interface {
	// B is the getter function to the command builder for the client
	// If the client is a cluster client, the command builder also prohibits cross key slots in one command.
	B() Builder
	// Do is the method sending user's redis command building from the B() to a redis node.
	//  client.Do(ctx, client.B().Get().Key("k").Build()).ToString()
	// All concurrent non-blocking commands will be pipelined automatically and have better throughput.
	// Blocking commands will use another separated connection pool.
	// The cmd parameter is recycled after passing into Do() and should not be reused.
	Do(ctx context.Context, cmd Completed) (resp RedisResult)
	// DoMulti takes multiple redis commands and sends them together, reducing RTT from the user code.
	// The multi parameters are recycled after passing into DoMulti() and should not be reused.
	DoMulti(ctx context.Context, multi ...Completed) (resp []RedisResult)
	// Receive accepts SUBSCRIBE, SSUBSCRIBE, PSUBSCRIBE command and a message handler.
	// Receive will block and then return value only when the following cases:
	//   1. return nil when received any unsubscribe/punsubscribe message related to the provided `subscribe` command.
	//   2. return ErrClosing when the client is closed manually.
	//   3. return ctx.Err() when the `ctx` is done.
	//   4. return non-nil err when the provided `subscribe` command failed.
	Receive(ctx context.Context, subscribe Completed, fn func(msg PubSubMessage)) error
	// Close will make further calls to the client be rejected with ErrClosing,
	// and Close will wait until all pending calls finished.
	Close()
}

// CT is a shorthand constructor for CacheableTTL
func CT(cmd Cacheable, ttl time.Duration) CacheableTTL {
	return CacheableTTL{Cmd: cmd, TTL: ttl}
}

// CacheableTTL is parameter container of DoMultiCache
type CacheableTTL struct {
	Cmd Cacheable
	TTL time.Duration
}

// NewClient uses ClientOption to initialize the Client for both cluster client and single client.
// It will first try to connect as cluster client. If the len(ClientOption.InitAddress) == 1 and
// the address does not enable cluster mode, the NewClient() will use single client instead.
func NewClient(option ClientOption) (client Client, err error) {
	if option.ReadBufferEachConn <= 0 {
		option.ReadBufferEachConn = DefaultReadBuffer
	}
	if option.WriteBufferEachConn <= 0 {
		option.WriteBufferEachConn = DefaultWriteBuffer
	}
	if option.CacheSizeEachConn <= 0 {
		option.CacheSizeEachConn = DefaultCacheBytes
	}
	if option.Dialer.Timeout == 0 {
		option.Dialer.Timeout = DefaultDialTimeout
	}
	if option.Dialer.KeepAlive == 0 {
		option.Dialer.KeepAlive = DefaultTCPKeepAlive
	}
	if option.ConnWriteTimeout == 0 {
		option.ConnWriteTimeout = option.Dialer.KeepAlive * 10
	}
	if option.ShuffleInit {
		rand.Shuffle(len(option.InitAddress), func(i, j int) {
			option.InitAddress[i], option.InitAddress[j] = option.InitAddress[j], option.InitAddress[i]
		})
	}
	if option.Sentinel.MasterSet != "" {
		option.PipelineMultiplex = singleClientMultiplex(option.PipelineMultiplex)
		return newSentinelClient(&option, makeConn)
	}
	pmbk := option.PipelineMultiplex
	option.PipelineMultiplex = 0 // PipelineMultiplex is meaningless for cluster client

	if option.ForceSingleClient {
		option.PipelineMultiplex = singleClientMultiplex(pmbk)
		return newSingleClient(&option, nil, makeConn)
	}
	if client, err = newClusterClient(&option, makeConn); err != nil {
		if len(option.InitAddress) == 1 && (err.Error() == redisErrMsgCommandNotAllow || strings.Contains(strings.ToUpper(err.Error()), "CLUSTER")) {
			option.PipelineMultiplex = singleClientMultiplex(pmbk)
			client, err = newSingleClient(&option, client.(*clusterClient).single(), makeConn)
		} else if client != (*clusterClient)(nil) {
			client.Close()
			return nil, err
		}
	}
	return client, err
}

func singleClientMultiplex(multiplex int) int {
	if multiplex == 0 {
		if multiplex = int(math.Log2(float64(runtime.GOMAXPROCS(0)))); multiplex >= 2 {
			multiplex = 2
		}
	}
	if multiplex < 0 {
		multiplex = 0
	}
	return multiplex
}

func makeConn(dst string, opt *ClientOption) conn {
	return makeMux(dst, opt, dial)
}

func dial(dst string, opt *ClientOption) (conn net.Conn, err error) {
	if opt.DialFn != nil {
		return opt.DialFn(dst, &opt.Dialer, opt.TLSConfig)
	}
	if opt.TLSConfig != nil {
		conn, err = tls.DialWithDialer(&opt.Dialer, "tcp", dst, opt.TLSConfig)
	} else {
		conn, err = opt.Dialer.Dial("tcp", dst)
	}
	return conn, err
}

const redisErrMsgCommandNotAllow = "command is not allowed"
