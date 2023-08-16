package rueidis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis/internal/cmds"
	"github.com/redis/rueidis/internal/util"
)

const LIB_NAME = "rueidis"
const LIB_VER = "1.0.14"

var noHello = regexp.MustCompile("unknown command .?(HELLO|hello).?")

type wire interface {
	Do(ctx context.Context, cmd Completed) RedisResult
	DoCache(ctx context.Context, cmd Cacheable, ttl time.Duration) RedisResult
	DoMulti(ctx context.Context, multi ...Completed) *redisresults
	DoMultiCache(ctx context.Context, multi ...CacheableTTL) *redisresults
	Receive(ctx context.Context, subscribe Completed, fn func(message PubSubMessage)) error
	Info() map[string]RedisMessage
	Error() error
	Close()

	CleanSubscriptions()
	SetPubSubHooks(hooks PubSubHooks) <-chan error
	SetOnCloseHook(fn func(error))
}

type redisresults struct {
	s []RedisResult
}

func (r *redisresults) Capacity() int {
	return cap(r.s)
}

func (r *redisresults) ResetLen(n int) {
	r.s = r.s[:n]
	for i := 0; i < n; i++ {
		r.s[i] = RedisResult{}
	}
}

var resultsp = util.NewPool(func(capacity int) *redisresults {
	return &redisresults{s: make([]RedisResult, 0, capacity)}
})

type cacheentries struct {
	e map[int]CacheEntry
	c int
}

func (c *cacheentries) Capacity() int {
	return c.c
}

func (c *cacheentries) ResetLen(n int) {
	for k := range c.e {
		delete(c.e, k)
	}
}

var entriesp = util.NewPool(func(capacity int) *cacheentries {
	return &cacheentries{e: make(map[int]CacheEntry, capacity), c: capacity}
})

var _ wire = (*pipe)(nil)

type pipe struct {
	conn            net.Conn
	error           atomic.Value
	clhks           atomic.Value
	pshks           atomic.Value
	queue           queue
	cache           CacheStore
	r               *bufio.Reader
	w               *bufio.Writer
	close           chan struct{}
	onInvalidations func([]RedisMessage)
	r2psFn          func() (p *pipe, err error)
	r2pipe          *pipe
	ssubs           *subs
	nsubs           *subs
	psubs           *subs
	info            map[string]RedisMessage
	timeout         time.Duration
	pinggap         time.Duration
	maxFlushDelay   time.Duration
	once            sync.Once
	r2mu            sync.Mutex
	version         int32
	_               [10]int32
	blcksig         int32
	state           int32
	waits           int32
	recvs           int32
	r2ps            bool
}

func newPipe(connFn func() (net.Conn, error), option *ClientOption) (p *pipe, err error) {
	return _newPipe(connFn, option, false)
}

func _newPipe(connFn func() (net.Conn, error), option *ClientOption, r2ps bool) (p *pipe, err error) {
	conn, err := connFn()
	if err != nil {
		return nil, err
	}
	p = &pipe{
		conn:  conn,
		queue: newRing(option.RingScaleEachConn),
		r:     bufio.NewReaderSize(conn, option.ReadBufferEachConn),
		w:     bufio.NewWriterSize(conn, option.WriteBufferEachConn),

		nsubs: newSubs(),
		psubs: newSubs(),
		ssubs: newSubs(),
		close: make(chan struct{}),

		timeout:       option.ConnWriteTimeout,
		pinggap:       option.Dialer.KeepAlive,
		maxFlushDelay: option.MaxFlushDelay,

		r2ps: r2ps,
	}
	if !r2ps {
		p.r2psFn = func() (p *pipe, err error) {
			return _newPipe(connFn, option, true)
		}
	}
	if !option.DisableCache {
		cacheStoreFn := option.NewCacheStoreFn
		if cacheStoreFn == nil {
			cacheStoreFn = newLRU
		}
		p.cache = cacheStoreFn(CacheStoreOption{CacheSizeEachConn: option.CacheSizeEachConn})
	}
	p.pshks.Store(emptypshks)
	p.clhks.Store(emptyclhks)

	helloCmd := []string{"HELLO", "3"}
	if option.Password != "" && option.Username == "" {
		helloCmd = append(helloCmd, "AUTH", "default", option.Password)
	} else if option.Username != "" {
		helloCmd = append(helloCmd, "AUTH", option.Username, option.Password)
	}
	if option.ClientName != "" {
		helloCmd = append(helloCmd, "SETNAME", option.ClientName)
	}

	init := make([][]string, 0, 4)
	if option.ClientTrackingOptions == nil {
		init = append(init, helloCmd, []string{"CLIENT", "TRACKING", "ON", "OPTIN"})
	} else {
		init = append(init, helloCmd, append([]string{"CLIENT", "TRACKING", "ON"}, option.ClientTrackingOptions...))
	}
	if option.DisableCache {
		init = init[:1]
	}
	if option.SelectDB != 0 {
		init = append(init, []string{"SELECT", strconv.Itoa(option.SelectDB)})
	}
	if option.ClientNoTouch {
		init = append(init, []string{"CLIENT", "NO-TOUCH", "ON"})
	}
	if option.ClientNoEvict {
		init = append(init, []string{"CLIENT", "NO-EVICT", "ON"})
	}
	if option.ClientSetInfo != nil {
		init = append(init, append([]string{"CLIENT", "SETINFO"}, option.ClientSetInfo...))
	} else {
		init = append(init, []string{"CLIENT", "SETINFO", "LIB-NAME", LIB_NAME, "LIB-VER", LIB_VER})
	}

	timeout := option.Dialer.Timeout
	if timeout <= 0 {
		timeout = DefaultDialTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	r2 := option.AlwaysRESP2
	if !r2 && !r2ps {
		resp := p.DoMulti(ctx, cmds.NewMultiCompleted(init)...)
		defer resultsp.Put(resp)
		for i, r := range resp.s[:len(resp.s)-1] { // skip error checking on the last CLIENT SETINFO
			if i == 0 {
				p.info, err = r.AsMap()
			} else {
				err = r.Error()
			}
			if err != nil {
				if re, ok := err.(*RedisError); ok {
					if !r2 && noHello.MatchString(re.string) {
						r2 = true
						continue
					} else if strings.Contains(re.string, "CLIENT") {
						err = fmt.Errorf("%s: %w", re.string, ErrNoCache)
					} else if r2 {
						continue
					}
				}
				p.Close()
				return nil, err
			}
		}
	}
	if proto := p.info["proto"]; proto.integer < 3 {
		r2 = true
	}
	if !r2 && !r2ps {
		if ver, ok := p.info["version"]; ok {
			if v := strings.Split(ver.string, "."); len(v) != 0 {
				vv, _ := strconv.ParseInt(v[0], 10, 32)
				p.version = int32(vv)
			}
		}
		p.onInvalidations = option.OnInvalidations
	} else {
		if !option.DisableCache {
			p.Close()
			return nil, ErrNoCache
		}
		init = init[:0]
		if option.Password != "" && option.Username == "" {
			init = append(init, []string{"AUTH", option.Password})
		} else if option.Username != "" {
			init = append(init, []string{"AUTH", option.Username, option.Password})
		}
		if option.ClientName != "" {
			init = append(init, []string{"CLIENT", "SETNAME", option.ClientName})
		}
		if option.SelectDB != 0 {
			init = append(init, []string{"SELECT", strconv.Itoa(option.SelectDB)})
		}
		if option.ClientNoTouch {
			init = append(init, []string{"CLIENT", "NO-TOUCH", "ON"})
		}
		if option.ClientNoEvict {
			init = append(init, []string{"CLIENT", "NO-EVICT", "ON"})
		}
		if option.ClientSetInfo != nil {
			init = append(init, append([]string{"CLIENT", "SETINFO"}, option.ClientSetInfo...))
		} else {
			init = append(init, []string{"CLIENT", "SETINFO", "LIB-NAME", LIB_NAME, "LIB-VER", LIB_VER})
		}
		p.version = 5
		if len(init) != 0 {
			resp := p.DoMulti(ctx, cmds.NewMultiCompleted(init)...)
			defer resultsp.Put(resp)
			for _, r := range resp.s[:len(resp.s)-1] { // skip error checking on the last CLIENT SETINFO
				if err = r.Error(); err != nil {
					p.Close()
					return nil, err
				}
			}
		}
	}
	if p.onInvalidations != nil || option.AlwaysPipelining {
		p.background()
	}
	if p.timeout > 0 && p.pinggap > 0 {
		go p.backgroundPing()
	}
	return p, nil
}

func (p *pipe) background() {
	atomic.CompareAndSwapInt32(&p.state, 0, 1)
	p.once.Do(func() { go p._background() })
}

func (p *pipe) _exit(err error) {
	p.error.CompareAndSwap(nil, &errs{error: err})
	atomic.CompareAndSwapInt32(&p.state, 1, 2) // stop accepting new requests
	_ = p.conn.Close()                         // force both read & write goroutine to exit
	p.clhks.Load().(func(error))(err)
}

func (p *pipe) _background() {
	p.conn.SetDeadline(time.Time{})
	go func() {
		p._exit(p._backgroundWrite())
		close(p.close)
	}()
	{
		p._exit(p._backgroundRead())
		atomic.CompareAndSwapInt32(&p.state, 2, 3) // make write goroutine to exit
		atomic.AddInt32(&p.waits, 1)
		go func() {
			<-p.queue.PutOne(cmds.QuitCmd)
			atomic.AddInt32(&p.waits, -1)
		}()
	}

	p.nsubs.Close()
	p.psubs.Close()
	p.ssubs.Close()
	if old := p.pshks.Swap(emptypshks).(*pshks); old.close != nil {
		old.close <- p.Error()
		close(old.close)
	}

	var (
		resps []RedisResult
		ch    chan RedisResult
		cond  *sync.Cond
	)

	// clean up cache and free pending calls
	if p.cache != nil {
		p.cache.Close(ErrDoCacheAborted)
	}
	if p.onInvalidations != nil {
		p.onInvalidations(nil)
	}
	for atomic.LoadInt32(&p.waits) != 0 {
		select {
		case <-p.close:
			_, _, _ = p.queue.NextWriteCmd()
		default:
		}
		if _, _, ch, resps, cond = p.queue.NextResultCh(); ch != nil {
			err := newErrResult(p.Error())
			for i := range resps {
				resps[i] = err
			}
			ch <- err
			cond.L.Unlock()
			cond.Signal()
		} else {
			cond.L.Unlock()
			cond.Signal()
			runtime.Gosched()
		}
	}
	<-p.close
	atomic.StoreInt32(&p.state, 4)
}

func (p *pipe) _backgroundWrite() (err error) {
	var (
		ones  = make([]Completed, 1)
		multi []Completed
		ch    chan RedisResult

		flushDelay = p.maxFlushDelay
		flushStart = time.Time{}
	)

	for atomic.LoadInt32(&p.state) < 3 {
		if ones[0], multi, ch = p.queue.NextWriteCmd(); ch == nil {
			if flushDelay != 0 {
				flushStart = time.Now()
			}
			if p.w.Buffered() == 0 {
				err = p.Error()
			} else {
				err = p.w.Flush()
			}
			if err == nil {
				if atomic.LoadInt32(&p.state) == 1 {
					ones[0], multi, ch = p.queue.WaitForWrite()
				} else {
					runtime.Gosched()
					continue
				}
				if flushDelay != 0 && atomic.LoadInt32(&p.waits) > 1 { // do not delay for sequential usage
					time.Sleep(flushDelay - time.Since(flushStart)) // ref: https://github.com/redis/rueidis/issues/156
				}
			}
		}
		if ch != nil && multi == nil {
			multi = ones
		}
		for _, cmd := range multi {
			err = writeCmd(p.w, cmd.Commands())
		}
		if err != nil {
			if err != ErrClosing { // ignore ErrClosing to allow final QUIT command to be sent
				return
			}
			runtime.Gosched()
		}
	}
	return
}

func (p *pipe) _backgroundRead() (err error) {
	var (
		msg   RedisMessage
		cond  *sync.Cond
		ones  = make([]Completed, 1)
		multi []Completed
		resps []RedisResult
		ch    chan RedisResult
		ff    int // fulfilled count
		skip  int // skip rest push messages
		ver   = p.version
		prply bool // push reply
		unsub bool // unsubscribe notification
		r2ps  = p.r2ps
	)

	defer func() {
		resp := newErrResult(err)
		if err != nil && ff < len(multi) {
			for ; ff < len(resps); ff++ {
				resps[ff] = resp
			}
			ch <- resp
			cond.L.Unlock()
			cond.Signal()
		}
	}()

	for {
		if msg, err = readNextMessage(p.r); err != nil {
			return
		}
		if msg.typ == '>' || (r2ps && len(msg.values) != 0 && msg.values[0].string != "pong") {
			if prply, unsub = p.handlePush(msg.values); !prply {
				continue
			}
			if skip > 0 {
				skip--
				prply = false
				unsub = false
				continue
			}
		} else if ver == 6 && len(msg.values) != 0 {
			// This is a workaround for Redis 6's broken invalidation protocol: https://github.com/redis/redis/issues/8935
			// When Redis 6 handles MULTI, MGET, or other multi-keys command,
			// it will send invalidation message immediately if it finds the keys are expired, thus causing the multi-keys command response to be broken.
			// We fix this by fetching the next message and patch it back to the response.
			i := 0
			for j, v := range msg.values {
				if v.typ == '>' {
					p.handlePush(v.values)
				} else {
					if i != j {
						msg.values[i] = v
					}
					i++
				}
			}
			for ; i < len(msg.values); i++ {
				if msg.values[i], err = readNextMessage(p.r); err != nil {
					return
				}
			}
		}
		if ff == len(multi) {
			ff = 0
			ones[0], multi, ch, resps, cond = p.queue.NextResultCh() // ch should not be nil, otherwise it must be a protocol bug
			if ch == nil {
				cond.L.Unlock()
				// Redis will send sunsubscribe notification proactively in the event of slot migration.
				// We should ignore them and go fetch next message.
				// We also treat all the other unsubscribe notifications just like sunsubscribe,
				// so that we don't need to track how many channels we have subscribed to deal with wildcard unsubscribe command
				if unsub {
					prply = false
					unsub = false
					continue
				}
				panic(protocolbug)
			}
			if multi == nil {
				multi = ones
			}
		} else if ff >= 4 && len(msg.values) >= 2 && multi[0].IsOptIn() { // if unfulfilled multi commands are lead by opt-in and get success response
			now := time.Now()
			if cacheable := Cacheable(multi[ff-1]); cacheable.IsMGet() {
				cc := cmds.MGetCacheCmd(cacheable)
				msgs := msg.values[len(msg.values)-1].values
				for i, cp := range msgs {
					ck := cmds.MGetCacheKey(cacheable, i)
					cp.attrs = cacheMark
					if pttl := msg.values[i].integer; pttl >= 0 {
						cp.setExpireAt(now.Add(time.Duration(pttl) * time.Millisecond).UnixMilli())
					}
					msgs[i].setExpireAt(p.cache.Update(ck, cc, cp))
				}
			} else {
				ck, cc := cmds.CacheKey(cacheable)
				ci := len(msg.values) - 1
				cp := msg.values[ci]
				cp.attrs = cacheMark
				if pttl := msg.values[ci-1].integer; pttl >= 0 {
					cp.setExpireAt(now.Add(time.Duration(pttl) * time.Millisecond).UnixMilli())
				}
				msg.values[ci].setExpireAt(p.cache.Update(ck, cc, cp))
			}
		}
		if prply {
			// Redis will send sunsubscribe notification proactively in the event of slot migration.
			// We should ignore them and go fetch next message.
			// We also treat all the other unsubscribe notifications just like sunsubscribe,
			// so that we don't need to track how many channels we have subscribed to deal with wildcard unsubscribe command
			if unsub && (!multi[ff].NoReply() || !strings.HasSuffix(multi[ff].Commands()[0], "UNSUBSCRIBE")) {
				prply = false
				unsub = false
				continue
			}
			prply = false
			unsub = false
			if !multi[ff].NoReply() {
				panic(protocolbug)
			}
			skip = len(multi[ff].Commands()) - 2
			msg = RedisMessage{} // override successful subscribe/unsubscribe response to empty
		} else if multi[ff].NoReply() && msg.string == "QUEUED" {
			panic(multiexecsub)
		}
		resp := newResult(msg, err)
		if resps != nil {
			resps[ff] = resp
		}
		if ff++; ff == len(multi) {
			ch <- resp
			cond.L.Unlock()
			cond.Signal()
		}
	}
}

func (p *pipe) backgroundPing() {
	var err error
	var prev, recv int32

	ticker := time.NewTicker(p.pinggap)
	defer ticker.Stop()
	for ; err == nil; prev = recv {
		select {
		case <-ticker.C:
			recv = atomic.LoadInt32(&p.recvs)
			if recv != prev || atomic.LoadInt32(&p.blcksig) != 0 || (atomic.LoadInt32(&p.state) == 0 && atomic.LoadInt32(&p.waits) != 0) {
				continue
			}
			ch := make(chan error, 1)
			tm := time.NewTimer(p.timeout)
			go func() { ch <- p.Do(context.Background(), cmds.PingCmd).NonRedisError() }()
			select {
			case <-tm.C:
				err = context.DeadlineExceeded
			case err = <-ch:
				tm.Stop()
			}
			if err != nil && atomic.LoadInt32(&p.blcksig) != 0 {
				err = nil
			}
		case <-p.close:
			return
		}
	}
	if err != ErrClosing {
		p._exit(err)
	}
}

func (p *pipe) handlePush(values []RedisMessage) (reply bool, unsubscribe bool) {
	if len(values) < 2 {
		return
	}
	// TODO: handle other push data
	// tracking-redir-broken
	// server-cpu-usage
	switch values[0].string {
	case "invalidate":
		if p.cache != nil {
			if values[1].IsNil() {
				p.cache.Delete(nil)
			} else {
				p.cache.Delete(values[1].values)
			}
		}
		if p.onInvalidations != nil {
			if values[1].IsNil() {
				p.onInvalidations(nil)
			} else {
				p.onInvalidations(values[1].values)
			}
		}
	case "message":
		if len(values) >= 3 {
			m := PubSubMessage{Channel: values[1].string, Message: values[2].string}
			p.nsubs.Publish(values[1].string, m)
			p.pshks.Load().(*pshks).hooks.OnMessage(m)
		}
	case "pmessage":
		if len(values) >= 4 {
			m := PubSubMessage{Pattern: values[1].string, Channel: values[2].string, Message: values[3].string}
			p.psubs.Publish(values[1].string, m)
			p.pshks.Load().(*pshks).hooks.OnMessage(m)
		}
	case "smessage":
		if len(values) >= 3 {
			m := PubSubMessage{Channel: values[1].string, Message: values[2].string}
			p.ssubs.Publish(values[1].string, m)
			p.pshks.Load().(*pshks).hooks.OnMessage(m)
		}
	case "unsubscribe":
		p.nsubs.Unsubscribe(values[1].string)
		if len(values) >= 3 {
			p.pshks.Load().(*pshks).hooks.OnSubscription(PubSubSubscription{Kind: values[0].string, Channel: values[1].string, Count: values[2].integer})
		}
		return true, true
	case "punsubscribe":
		p.psubs.Unsubscribe(values[1].string)
		if len(values) >= 3 {
			p.pshks.Load().(*pshks).hooks.OnSubscription(PubSubSubscription{Kind: values[0].string, Channel: values[1].string, Count: values[2].integer})
		}
		return true, true
	case "sunsubscribe":
		p.ssubs.Unsubscribe(values[1].string)
		if len(values) >= 3 {
			p.pshks.Load().(*pshks).hooks.OnSubscription(PubSubSubscription{Kind: values[0].string, Channel: values[1].string, Count: values[2].integer})
		}
		return true, true
	case "subscribe", "psubscribe", "ssubscribe":
		if len(values) >= 3 {
			p.pshks.Load().(*pshks).hooks.OnSubscription(PubSubSubscription{Kind: values[0].string, Channel: values[1].string, Count: values[2].integer})
		}
		return true, false
	}
	return false, false
}

func (p *pipe) _r2pipe() (r2p *pipe) {
	p.r2mu.Lock()
	if p.r2pipe != nil {
		r2p = p.r2pipe
	} else {
		var err error
		if r2p, err = p.r2psFn(); err != nil {
			r2p = epipeFn(err)
		} else {
			p.r2pipe = r2p
		}
	}
	p.r2mu.Unlock()
	return r2p
}

func (p *pipe) Receive(ctx context.Context, subscribe Completed, fn func(message PubSubMessage)) error {
	if p.nsubs == nil || p.psubs == nil || p.ssubs == nil {
		return p.Error()
	}

	if p.version < 6 && p.r2psFn != nil {
		return p._r2pipe().Receive(ctx, subscribe, fn)
	}

	cmds.CompletedCS(subscribe).Verify()

	var sb *subs
	cmd, args := subscribe.Commands()[0], subscribe.Commands()[1:]

	switch cmd {
	case "SUBSCRIBE":
		sb = p.nsubs
	case "PSUBSCRIBE":
		sb = p.psubs
	case "SSUBSCRIBE":
		sb = p.ssubs
	default:
		panic(wrongreceive)
	}

	if ch, cancel := sb.Subscribe(args); ch != nil {
		defer cancel()
		if err := p.Do(ctx, subscribe).Error(); err != nil {
			return err
		}
		if ctxCh := ctx.Done(); ctxCh == nil {
			for msg := range ch {
				fn(msg)
			}
		} else {
		next:
			select {
			case msg, ok := <-ch:
				if ok {
					fn(msg)
					goto next
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return p.Error()
}

func (p *pipe) CleanSubscriptions() {
	if atomic.LoadInt32(&p.state) == 1 {
		if p.version >= 7 {
			p.DoMulti(context.Background(), cmds.UnsubscribeCmd, cmds.PUnsubscribeCmd, cmds.SUnsubscribeCmd)
		} else {
			p.DoMulti(context.Background(), cmds.UnsubscribeCmd, cmds.PUnsubscribeCmd)
		}
	}
}

func (p *pipe) SetPubSubHooks(hooks PubSubHooks) <-chan error {
	if p.version < 6 && p.r2psFn != nil {
		return p._r2pipe().SetPubSubHooks(hooks)
	}
	if hooks.isZero() {
		if old := p.pshks.Swap(emptypshks).(*pshks); old.close != nil {
			close(old.close)
		}
		return nil
	}
	if hooks.OnMessage == nil {
		hooks.OnMessage = func(m PubSubMessage) {}
	}
	if hooks.OnSubscription == nil {
		hooks.OnSubscription = func(s PubSubSubscription) {}
	}
	ch := make(chan error, 1)
	if old := p.pshks.Swap(&pshks{hooks: hooks, close: ch}).(*pshks); old.close != nil {
		close(old.close)
	}
	if err := p.Error(); err != nil {
		if old := p.pshks.Swap(emptypshks).(*pshks); old.close != nil {
			old.close <- err
			close(old.close)
		}
	}
	if atomic.AddInt32(&p.waits, 1) == 1 && atomic.LoadInt32(&p.state) == 0 {
		p.background()
	}
	atomic.AddInt32(&p.waits, -1)
	return ch
}

func (p *pipe) SetOnCloseHook(fn func(error)) {
	p.clhks.Store(fn)
}

func (p *pipe) Info() map[string]RedisMessage {
	return p.info
}

func (p *pipe) Do(ctx context.Context, cmd Completed) (resp RedisResult) {
	if err := ctx.Err(); err != nil {
		return newErrResult(err)
	}

	cmds.CompletedCS(cmd).Verify()

	if cmd.IsBlock() {
		atomic.AddInt32(&p.blcksig, 1)
		defer func() {
			if resp.err == nil {
				atomic.AddInt32(&p.blcksig, -1)
			}
		}()
	}

	if cmd.NoReply() {
		if p.version < 6 && p.r2psFn != nil {
			return p._r2pipe().Do(ctx, cmd)
		}
	}

	waits := atomic.AddInt32(&p.waits, 1) // if this is 1, and background worker is not started, no need to queue
	state := atomic.LoadInt32(&p.state)

	if state == 1 {
		goto queue
	}

	if state == 0 {
		if waits != 1 {
			goto queue
		}
		if cmd.NoReply() {
			p.background()
			goto queue
		}
		dl, ok := ctx.Deadline()
		if !ok && ctx.Done() != nil {
			p.background()
			goto queue
		}
		resp = p.syncDo(dl, ok, cmd)
	} else {
		resp = newErrResult(p.Error())
	}
	if left := atomic.AddInt32(&p.waits, -1); state == 0 && waits == 1 && left != 0 {
		p.background()
	}
	atomic.AddInt32(&p.recvs, 1)
	return resp

queue:
	ch := p.queue.PutOne(cmd)
	if ctxCh := ctx.Done(); ctxCh == nil {
		resp = <-ch
		atomic.AddInt32(&p.waits, -1)
		atomic.AddInt32(&p.recvs, 1)
	} else {
		select {
		case resp = <-ch:
			atomic.AddInt32(&p.waits, -1)
			atomic.AddInt32(&p.recvs, 1)
		case <-ctxCh:
			resp = newErrResult(ctx.Err())
			go func() {
				<-ch
				atomic.AddInt32(&p.waits, -1)
				atomic.AddInt32(&p.recvs, 1)
			}()
		}
	}
	return resp
}

func (p *pipe) DoMulti(ctx context.Context, multi ...Completed) *redisresults {
	resp := resultsp.Get(len(multi), len(multi))
	if err := ctx.Err(); err != nil {
		for i := 0; i < len(resp.s); i++ {
			resp.s[i] = newErrResult(err)
		}
		return resp
	}

	cmds.CompletedCS(multi[0]).Verify()

	isOptIn := multi[0].IsOptIn() // len(multi) > 0 should have already been checked by upper layer
	noReply := 0
	isBlock := false

	for _, cmd := range multi {
		if cmd.NoReply() {
			noReply++
		}
	}

	if p.version < 6 && noReply != 0 {
		if noReply != len(multi) {
			for i := 0; i < len(resp.s); i++ {
				resp.s[i] = newErrResult(ErrRESP2PubSubMixed)
			}
			return resp
		} else if p.r2psFn != nil {
			resultsp.Put(resp)
			return p._r2pipe().DoMulti(ctx, multi...)
		}
	}

	for _, cmd := range multi {
		if cmd.IsBlock() {
			isBlock = true
			break
		}
	}

	if isBlock {
		atomic.AddInt32(&p.blcksig, 1)
		defer func() {
			for _, r := range resp.s {
				if r.err != nil {
					return
				}
			}
			atomic.AddInt32(&p.blcksig, -1)
		}()
	}

	waits := atomic.AddInt32(&p.waits, 1) // if this is 1, and background worker is not started, no need to queue
	state := atomic.LoadInt32(&p.state)

	if state == 1 {
		goto queue
	}

	if state == 0 {
		if waits != 1 {
			goto queue
		}
		if isOptIn || noReply != 0 {
			p.background()
			goto queue
		}
		dl, ok := ctx.Deadline()
		if !ok && ctx.Done() != nil {
			p.background()
			goto queue
		}
		p.syncDoMulti(dl, ok, resp.s, multi)
	} else {
		err := newErrResult(p.Error())
		for i := 0; i < len(resp.s); i++ {
			resp.s[i] = err
		}
	}
	if left := atomic.AddInt32(&p.waits, -1); state == 0 && waits == 1 && left != 0 {
		p.background()
	}
	atomic.AddInt32(&p.recvs, 1)
	return resp

queue:
	ch := p.queue.PutMulti(multi, resp.s)
	if ctxCh := ctx.Done(); ctxCh == nil {
		<-ch
	} else {
		select {
		case <-ch:
		case <-ctxCh:
			goto abort
		}
	}
	atomic.AddInt32(&p.waits, -1)
	atomic.AddInt32(&p.recvs, 1)
	return resp
abort:
	go func(resp *redisresults) {
		<-ch
		resultsp.Put(resp)
		atomic.AddInt32(&p.waits, -1)
		atomic.AddInt32(&p.recvs, 1)
	}(resp)
	resp = resultsp.Get(len(multi), len(multi))
	err := newErrResult(ctx.Err())
	for i := 0; i < len(resp.s); i++ {
		resp.s[i] = err
	}
	return resp
}

func (p *pipe) syncDo(dl time.Time, dlOk bool, cmd Completed) (resp RedisResult) {
	if dlOk {
		p.conn.SetDeadline(dl)
	} else if p.timeout > 0 && !cmd.IsBlock() {
		p.conn.SetDeadline(time.Now().Add(p.timeout))
	} else {
		p.conn.SetDeadline(time.Time{})
	}

	var msg RedisMessage
	err := writeCmd(p.w, cmd.Commands())
	if err == nil {
		if err = p.w.Flush(); err == nil {
			msg, err = syncRead(p.r)
		}
	}
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			err = context.DeadlineExceeded
		}
		p.error.CompareAndSwap(nil, &errs{error: err})
		p.conn.Close()
		p.background() // start the background worker to clean up goroutines
	}
	return newResult(msg, err)
}

func (p *pipe) syncDoMulti(dl time.Time, dlOk bool, resp []RedisResult, multi []Completed) {
	if dlOk {
		p.conn.SetDeadline(dl)
	} else if p.timeout > 0 {
		for _, cmd := range multi {
			if cmd.IsBlock() {
				goto process
			}
		}
		p.conn.SetDeadline(time.Now().Add(p.timeout))
	} else {
		p.conn.SetDeadline(time.Time{})
	}
process:
	var err error
	var msg RedisMessage

	for _, cmd := range multi {
		_ = writeCmd(p.w, cmd.Commands())
	}
	if err = p.w.Flush(); err != nil {
		goto abort
	}
	for i := 0; i < len(resp); i++ {
		if msg, err = syncRead(p.r); err != nil {
			goto abort
		}
		resp[i] = newResult(msg, err)
	}
	return
abort:
	if errors.Is(err, os.ErrDeadlineExceeded) {
		err = context.DeadlineExceeded
	}
	p.error.CompareAndSwap(nil, &errs{error: err})
	p.conn.Close()
	p.background() // start the background worker to clean up goroutines
	for i := 0; i < len(resp); i++ {
		resp[i] = newErrResult(err)
	}
	return
}

func syncRead(r *bufio.Reader) (m RedisMessage, err error) {
next:
	if m, err = readNextMessage(r); err != nil {
		return m, err
	}
	if m.typ == '>' {
		goto next
	}
	return m, nil
}

func (p *pipe) DoCache(ctx context.Context, cmd Cacheable, ttl time.Duration) RedisResult {
	if p.cache == nil {
		return p.Do(ctx, Completed(cmd))
	}

	cmds.CacheableCS(cmd).Verify()

	if cmd.IsMGet() {
		return p.doCacheMGet(ctx, cmd, ttl)
	}
	ck, cc := cmds.CacheKey(cmd)
	now := time.Now()
	if v, entry := p.cache.Flight(ck, cc, ttl, now); v.typ != 0 {
		return newResult(v, nil)
	} else if entry != nil {
		return newResult(entry.Wait(ctx))
	}
	resp := p.DoMulti(
		ctx,
		cmds.OptInCmd,
		cmds.MultiCmd,
		cmds.NewCompleted([]string{"PTTL", ck}),
		Completed(cmd),
		cmds.ExecCmd,
	)
	defer resultsp.Put(resp)
	exec, err := resp.s[4].ToArray()
	if err != nil {
		if _, ok := err.(*RedisError); ok {
			err = ErrDoCacheAborted
		}
		p.cache.Cancel(ck, cc, err)
		return newErrResult(err)
	}
	return newResult(exec[1], nil)
}

func (p *pipe) doCacheMGet(ctx context.Context, cmd Cacheable, ttl time.Duration) RedisResult {
	commands := cmd.Commands()
	keys := len(commands) - 1
	builder := cmds.NewBuilder(cmds.InitSlot)
	result := RedisResult{val: RedisMessage{typ: '*', values: nil}}
	mgetcc := cmds.MGetCacheCmd(cmd)
	if mgetcc[0] == 'J' {
		keys-- // the last one of JSON.MGET is a path, not a key
	}
	entries := entriesp.Get(keys, keys)
	defer entriesp.Put(entries)
	var now = time.Now()
	var rewrite cmds.Arbitrary
	for i, key := range commands[1 : keys+1] {
		v, entry := p.cache.Flight(key, mgetcc, ttl, now)
		if v.typ != 0 { // cache hit for one key
			if len(result.val.values) == 0 {
				result.val.values = make([]RedisMessage, keys)
			}
			result.val.values[i] = v
			continue
		}
		if entry != nil {
			entries.e[i] = entry // store entries for later entry.Wait() to avoid MGET deadlock each others.
			continue
		}
		if rewrite.IsZero() {
			rewrite = builder.Arbitrary(commands[0])
		}
		rewrite = rewrite.Args(key)
	}

	var partial []RedisMessage
	if !rewrite.IsZero() {
		var rewritten Completed
		var keys int
		if mgetcc[0] == 'J' { // rewrite JSON.MGET path
			rewritten = rewrite.Args(commands[len(commands)-1]).MultiGet()
			keys = len(rewritten.Commands()) - 2
		} else {
			rewritten = rewrite.MultiGet()
			keys = len(rewritten.Commands()) - 1
		}

		multi := make([]Completed, 0, keys+4)
		multi = append(multi, cmds.OptInCmd, cmds.MultiCmd)
		for _, key := range rewritten.Commands()[1 : keys+1] {
			multi = append(multi, builder.Pttl().Key(key).Build())
		}
		multi = append(multi, rewritten, cmds.ExecCmd)

		resp := p.DoMulti(ctx, multi...)
		defer resultsp.Put(resp)
		exec, err := resp.s[len(multi)-1].ToArray()
		if err != nil {
			if _, ok := err.(*RedisError); ok {
				err = ErrDoCacheAborted
			}
			for _, key := range rewritten.Commands()[1 : keys+1] {
				p.cache.Cancel(key, mgetcc, err)
			}
			return newErrResult(err)
		}
		defer func() {
			for _, cmd := range multi[2 : len(multi)-1] {
				cmds.PutCompleted(cmd)
			}
		}()
		last := len(exec) - 1
		if len(rewritten.Commands()) == len(commands) { // all cache miss
			return newResult(exec[last], nil)
		}
		partial = exec[last].values
	} else { // all cache hit
		result.val.attrs = cacheMark
	}

	if len(result.val.values) == 0 {
		result.val.values = make([]RedisMessage, keys)
	}
	for i, entry := range entries.e {
		v, err := entry.Wait(ctx)
		if err != nil {
			return newErrResult(err)
		}
		result.val.values[i] = v
	}

	j := 0
	for _, ret := range partial {
		for ; j < len(result.val.values); j++ {
			if result.val.values[j].typ == 0 {
				result.val.values[j] = ret
				break
			}
		}
	}
	return result
}

func (p *pipe) DoMultiCache(ctx context.Context, multi ...CacheableTTL) *redisresults {
	if p.cache == nil {
		commands := make([]Completed, len(multi))
		for i, ct := range multi {
			commands[i] = Completed(ct.Cmd)
		}
		return p.DoMulti(ctx, commands...)
	}

	cmds.CacheableCS(multi[0].Cmd).Verify()

	results := resultsp.Get(len(multi), len(multi))
	entries := entriesp.Get(len(multi), len(multi))
	defer entriesp.Put(entries)
	var missing []Completed
	now := time.Now()
	for _, ct := range multi {
		if ct.Cmd.IsMGet() {
			panic(panicmgetcsc)
		}
	}
	if cache, ok := p.cache.(*lru); ok {
		missed := cache.Flights(now, multi, results.s, entries.e)
		for _, i := range missed {
			ct := multi[i]
			ck, _ := cmds.CacheKey(ct.Cmd)
			missing = append(missing, cmds.OptInCmd, cmds.MultiCmd, cmds.NewCompleted([]string{"PTTL", ck}), Completed(ct.Cmd), cmds.ExecCmd)
		}
	} else {
		for i, ct := range multi {
			ck, cc := cmds.CacheKey(ct.Cmd)
			v, entry := p.cache.Flight(ck, cc, ct.TTL, now)
			if v.typ != 0 { // cache hit for one key
				results.s[i] = newResult(v, nil)
				continue
			}
			if entry != nil {
				entries.e[i] = entry // store entries for later entry.Wait() to avoid MGET deadlock each others.
				continue
			}
			missing = append(missing, cmds.OptInCmd, cmds.MultiCmd, cmds.NewCompleted([]string{"PTTL", ck}), Completed(ct.Cmd), cmds.ExecCmd)
		}
	}

	var resp *redisresults
	if len(missing) > 0 {
		resp = p.DoMulti(ctx, missing...)
		defer resultsp.Put(resp)
		for i := 4; i < len(resp.s); i += 5 {
			if err := resp.s[i].Error(); err != nil {
				if _, ok := err.(*RedisError); ok {
					err = ErrDoCacheAborted
				}
				ck, cc := cmds.CacheKey(Cacheable(missing[i-1]))
				p.cache.Cancel(ck, cc, err)
			}
		}
	}

	for i, entry := range entries.e {
		results.s[i] = newResult(entry.Wait(ctx))
	}

	if len(missing) == 0 {
		return results
	}

	j := 0
	for i := 4; i < len(resp.s); i += 5 {
		for ; j < len(results.s); j++ {
			if results.s[j].val.typ == 0 && results.s[j].err == nil {
				exec, err := resp.s[i].ToArray()
				if err != nil {
					if _, ok := err.(*RedisError); ok {
						err = ErrDoCacheAborted
					}
					results.s[j] = newErrResult(err)
				} else {
					results.s[j] = newResult(exec[len(exec)-1], nil)
				}
				break
			}
		}
	}
	return results
}

func (p *pipe) Error() error {
	if err, ok := p.error.Load().(*errs); ok {
		return err.error
	}
	return nil
}

func (p *pipe) Close() {
	p.error.CompareAndSwap(nil, errClosing)
	block := atomic.AddInt32(&p.blcksig, 1)
	waits := atomic.AddInt32(&p.waits, 1)
	stopping1 := atomic.CompareAndSwapInt32(&p.state, 0, 2)
	stopping2 := atomic.CompareAndSwapInt32(&p.state, 1, 2)
	if p.queue != nil {
		if stopping1 && waits == 1 { // make sure there is no sync read
			p.background()
		}
		if block == 1 && (stopping1 || stopping2) { // make sure there is no block cmd
			<-p.queue.PutOne(cmds.QuitCmd)
		}
	}
	atomic.AddInt32(&p.waits, -1)
	atomic.AddInt32(&p.blcksig, -1)
	if p.conn != nil {
		p.conn.Close()
	}
	p.r2mu.Lock()
	if p.r2pipe != nil {
		p.r2pipe.Close()
	}
	p.r2mu.Unlock()
}

type pshks struct {
	hooks PubSubHooks
	close chan error
}

var emptypshks = &pshks{
	hooks: PubSubHooks{
		OnMessage:      func(m PubSubMessage) {},
		OnSubscription: func(s PubSubSubscription) {},
	},
	close: nil,
}

var emptyclhks = func(error) {}

func deadFn() *pipe {
	dead := &pipe{state: 3}
	dead.error.Store(errClosing)
	dead.pshks.Store(emptypshks)
	dead.clhks.Store(emptyclhks)
	return dead
}

func epipeFn(err error) *pipe {
	dead := &pipe{state: 3}
	dead.error.Store(&errs{error: err})
	dead.pshks.Store(emptypshks)
	dead.clhks.Store(emptyclhks)
	return dead
}

const (
	protocolbug  = "protocol bug, message handled out of order"
	wrongreceive = "only SUBSCRIBE, SSUBSCRIBE, or PSUBSCRIBE command are allowed in Receive"
	multiexecsub = "SUBSCRIBE/UNSUBSCRIBE are not allowed in MULTI/EXEC block"
	panicmgetcsc = "MGET and JSON.MGET in DoMultiCache are not implemented, use DoCache instead"
)

var cacheMark = &(RedisMessage{})
var errClosing = &errs{error: ErrClosing}

type errs struct{ error }
