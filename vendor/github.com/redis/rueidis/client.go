package rueidis

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis/internal/cmds"
)

type singleClient struct {
	conn         conn
	stop         uint32
	cmd          Builder
	retry        bool
	DisableCache bool
}

func newSingleClient(opt *ClientOption, prev conn, connFn connFn) (*singleClient, error) {
	if len(opt.InitAddress) == 0 {
		return nil, ErrNoAddr
	}

	if opt.ReplicaOnly {
		return nil, ErrReplicaOnlyNotSupported
	}

	conn := connFn(opt.InitAddress[0], opt)
	conn.Override(prev)
	if err := conn.Dial(); err != nil {
		return nil, err
	}
	return newSingleClientWithConn(conn, cmds.NewBuilder(cmds.NoSlot), !opt.DisableRetry, opt.DisableCache), nil
}

func newSingleClientWithConn(conn conn, builder Builder, retry, disableCache bool) *singleClient {
	return &singleClient{cmd: builder, conn: conn, retry: retry, DisableCache: disableCache}
}

func (c *singleClient) B() Builder {
	return c.cmd
}

func (c *singleClient) Do(ctx context.Context, cmd Completed) (resp RedisResult) {
retry:
	resp = c.conn.Do(ctx, cmd)
	if c.retry && cmd.IsReadOnly() && c.isRetryable(resp.NonRedisError(), ctx) {
		goto retry
	}
	if resp.NonRedisError() == nil { // not recycle cmds if error, since cmds may be used later in pipe. consider recycle them by pipe
		cmds.PutCompleted(cmd)
	}
	return resp
}

func (c *singleClient) DoStream(ctx context.Context, cmd Completed) RedisResultStream {
	s := c.conn.DoStream(ctx, cmd)
	cmds.PutCompleted(cmd)
	return s
}

func (c *singleClient) DoMultiStream(ctx context.Context, multi ...Completed) MultiRedisResultStream {
	if len(multi) == 0 {
		return RedisResultStream{e: io.EOF}
	}
	s := c.conn.DoMultiStream(ctx, multi...)
	for _, cmd := range multi {
		cmds.PutCompleted(cmd)
	}
	return s
}

func (c *singleClient) DoMulti(ctx context.Context, multi ...Completed) (resps []RedisResult) {
	if len(multi) == 0 {
		return nil
	}
retry:
	resps = c.conn.DoMulti(ctx, multi...).s
	if c.retry && allReadOnly(multi) {
		for _, resp := range resps {
			if c.isRetryable(resp.NonRedisError(), ctx) {
				goto retry
			}
		}
	}
	for i, cmd := range multi {
		if resps[i].NonRedisError() == nil {
			cmds.PutCompleted(cmd)
		}
	}
	return resps
}

func (c *singleClient) DoMultiCache(ctx context.Context, multi ...CacheableTTL) (resps []RedisResult) {
	if len(multi) == 0 {
		return nil
	}
retry:
	resps = c.conn.DoMultiCache(ctx, multi...).s
	if c.retry {
		for _, resp := range resps {
			if c.isRetryable(resp.NonRedisError(), ctx) {
				goto retry
			}
		}
	}
	for i, cmd := range multi {
		if err := resps[i].NonRedisError(); err == nil || err == ErrDoCacheAborted {
			cmds.PutCacheable(cmd.Cmd)
		}
	}
	return resps
}

func (c *singleClient) DoCache(ctx context.Context, cmd Cacheable, ttl time.Duration) (resp RedisResult) {
retry:
	resp = c.conn.DoCache(ctx, cmd, ttl)
	if c.retry && c.isRetryable(resp.NonRedisError(), ctx) {
		goto retry
	}
	if err := resp.NonRedisError(); err == nil || err == ErrDoCacheAborted {
		cmds.PutCacheable(cmd)
	}
	return resp
}

func (c *singleClient) Receive(ctx context.Context, subscribe Completed, fn func(msg PubSubMessage)) (err error) {
retry:
	err = c.conn.Receive(ctx, subscribe, fn)
	if c.retry {
		if _, ok := err.(*RedisError); !ok && c.isRetryable(err, ctx) {
			goto retry
		}
	}
	if err == nil {
		cmds.PutCompleted(subscribe)
	}
	return err
}

func (c *singleClient) Dedicated(fn func(DedicatedClient) error) (err error) {
	wire := c.conn.Acquire()
	dsc := &dedicatedSingleClient{cmd: c.cmd, conn: c.conn, wire: wire, retry: c.retry}
	err = fn(dsc)
	dsc.release()
	return err
}

func (c *singleClient) Dedicate() (DedicatedClient, func()) {
	wire := c.conn.Acquire()
	dsc := &dedicatedSingleClient{cmd: c.cmd, conn: c.conn, wire: wire, retry: c.retry}
	return dsc, dsc.release
}

func (c *singleClient) Nodes() map[string]Client {
	return map[string]Client{c.conn.Addr(): c}
}

func (c *singleClient) Close() {
	atomic.StoreUint32(&c.stop, 1)
	c.conn.Close()
}

type dedicatedSingleClient struct {
	conn conn
	wire wire
	mark uint32
	cmd  Builder

	retry bool
}

func (c *dedicatedSingleClient) B() Builder {
	return c.cmd
}

func (c *dedicatedSingleClient) Do(ctx context.Context, cmd Completed) (resp RedisResult) {
retry:
	if err := c.check(); err != nil {
		return newErrResult(err)
	}
	resp = c.wire.Do(ctx, cmd)
	if c.retry && cmd.IsReadOnly() && isRetryable(resp.NonRedisError(), c.wire, ctx) {
		goto retry
	}
	if resp.NonRedisError() == nil {
		cmds.PutCompleted(cmd)
	}
	return resp
}

func (c *dedicatedSingleClient) DoMulti(ctx context.Context, multi ...Completed) (resp []RedisResult) {
	if len(multi) == 0 {
		return nil
	}
	retryable := c.retry
	if retryable {
		retryable = allReadOnly(multi)
	}
retry:
	if err := c.check(); err != nil {
		return fillErrs(len(multi), err)
	}
	resp = c.wire.DoMulti(ctx, multi...).s
	if retryable && anyRetryable(resp, c.wire, ctx) {
		goto retry
	}
	for i, cmd := range multi {
		if resp[i].NonRedisError() == nil {
			cmds.PutCompleted(cmd)
		}
	}
	return resp
}

func (c *dedicatedSingleClient) Receive(ctx context.Context, subscribe Completed, fn func(msg PubSubMessage)) (err error) {
retry:
	if err := c.check(); err != nil {
		return err
	}
	err = c.wire.Receive(ctx, subscribe, fn)
	if c.retry {
		if _, ok := err.(*RedisError); !ok && isRetryable(err, c.wire, ctx) {
			goto retry
		}
	}
	if err == nil {
		cmds.PutCompleted(subscribe)
	}
	return err
}

func (c *dedicatedSingleClient) SetPubSubHooks(hooks PubSubHooks) <-chan error {
	if err := c.check(); err != nil {
		ch := make(chan error, 1)
		ch <- err
		return ch
	}
	return c.wire.SetPubSubHooks(hooks)
}

func (c *dedicatedSingleClient) Close() {
	c.wire.Close()
	c.release()
}

func (c *dedicatedSingleClient) check() error {
	if atomic.LoadUint32(&c.mark) != 0 {
		return ErrDedicatedClientRecycled
	}
	return nil
}

func (c *dedicatedSingleClient) release() {
	if atomic.CompareAndSwapUint32(&c.mark, 0, 1) {
		c.conn.Store(c.wire)
	}
}

func (c *singleClient) isRetryable(err error, ctx context.Context) bool {
	return err != nil && err != ErrDoCacheAborted && atomic.LoadUint32(&c.stop) == 0 && ctx.Err() == nil
}

func isRetryable(err error, w wire, ctx context.Context) bool {
	return err != nil && w.Error() == nil && ctx.Err() == nil
}

func anyRetryable(resp []RedisResult, w wire, ctx context.Context) bool {
	for _, r := range resp {
		if isRetryable(r.NonRedisError(), w, ctx) {
			return true
		}
	}
	return false
}

func allReadOnly(multi []Completed) bool {
	for _, cmd := range multi {
		if cmd.IsWrite() {
			return false
		}
	}
	return true
}

func chooseSlot(multi []Completed) uint16 {
	for i := 0; i < len(multi); i++ {
		if multi[i].Slot() != cmds.InitSlot {
			for j := i + 1; j < len(multi); j++ {
				if multi[j].Slot() != cmds.InitSlot && multi[j].Slot() != multi[i].Slot() {
					return cmds.NoSlot
				}
			}
			return multi[i].Slot()
		}
	}
	return cmds.InitSlot
}
