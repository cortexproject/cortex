// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// +build all unit

package gocql

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocql/gocql/internal/streams"
)

const (
	defaultProto = protoVersion2
)

func TestApprove(t *testing.T) {
	tests := map[bool]bool{
		approve("org.apache.cassandra.auth.PasswordAuthenticator"):          true,
		approve("com.instaclustr.cassandra.auth.SharedSecretAuthenticator"): true,
		approve("com.datastax.bdp.cassandra.auth.DseAuthenticator"):         true,
		approve("com.apache.cassandra.auth.FakeAuthenticator"):              false,
	}
	for k, v := range tests {
		if k != v {
			t.Fatalf("expected '%v', got '%v'", k, v)
		}
	}
}

func TestJoinHostPort(t *testing.T) {
	tests := map[string]string{
		"127.0.0.1:0":                                 JoinHostPort("127.0.0.1", 0),
		"127.0.0.1:1":                                 JoinHostPort("127.0.0.1:1", 9142),
		"[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:0": JoinHostPort("2001:0db8:85a3:0000:0000:8a2e:0370:7334", 0),
		"[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1": JoinHostPort("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1", 9142),
	}
	for k, v := range tests {
		if k != v {
			t.Fatalf("expected '%v', got '%v'", k, v)
		}
	}
}

func testCluster(addr string, proto protoVersion) *ClusterConfig {
	cluster := NewCluster(addr)
	cluster.ProtoVersion = int(proto)
	cluster.disableControlConn = true
	return cluster
}

func TestSimple(t *testing.T) {
	srv := NewTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	cluster := testCluster(srv.Address, defaultProto)
	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("0x%x: NewCluster: %v", defaultProto, err)
	}

	if err := db.Query("void").Exec(); err != nil {
		t.Fatalf("0x%x: %v", defaultProto, err)
	}
}

func TestSSLSimple(t *testing.T) {
	srv := NewSSLTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	db, err := createTestSslCluster(srv.Address, defaultProto, true).CreateSession()
	if err != nil {
		t.Fatalf("0x%x: NewCluster: %v", defaultProto, err)
	}

	if err := db.Query("void").Exec(); err != nil {
		t.Fatalf("0x%x: %v", defaultProto, err)
	}
}

func TestSSLSimpleNoClientCert(t *testing.T) {
	srv := NewSSLTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	db, err := createTestSslCluster(srv.Address, defaultProto, false).CreateSession()
	if err != nil {
		t.Fatalf("0x%x: NewCluster: %v", defaultProto, err)
	}

	if err := db.Query("void").Exec(); err != nil {
		t.Fatalf("0x%x: %v", defaultProto, err)
	}
}

func createTestSslCluster(addr string, proto protoVersion, useClientCert bool) *ClusterConfig {
	cluster := testCluster(addr, proto)
	sslOpts := &SslOptions{
		CaPath:                 "testdata/pki/ca.crt",
		EnableHostVerification: false,
	}

	if useClientCert {
		sslOpts.CertPath = "testdata/pki/gocql.crt"
		sslOpts.KeyPath = "testdata/pki/gocql.key"
	}

	cluster.SslOpts = sslOpts
	return cluster
}

func TestClosed(t *testing.T) {
	t.Skip("Skipping the execution of TestClosed for now to try to concentrate on more important test failures on Travis")

	srv := NewTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	session, err := newTestSession(srv.Address, defaultProto)
	if err != nil {
		t.Fatalf("0x%x: NewCluster: %v", defaultProto, err)
	}

	session.Close()

	if err := session.Query("void").Exec(); err != ErrSessionClosed {
		t.Fatalf("0x%x: expected %#v, got %#v", defaultProto, ErrSessionClosed, err)
	}
}

func newTestSession(addr string, proto protoVersion) (*Session, error) {
	return testCluster(addr, proto).CreateSession()
}

func TestDNSLookupConnected(t *testing.T) {
	log := &testLogger{}
	Logger = log
	defer func() {
		Logger = &defaultLogger{}
	}()

	srv := NewTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	cluster := NewCluster("cassandra1.invalid", srv.Address, "cassandra2.invalid")
	cluster.ProtoVersion = int(defaultProto)
	cluster.disableControlConn = true

	// CreateSession() should attempt to resolve the DNS name "cassandraX.invalid"
	// and fail, but continue to connect via srv.Address
	_, err := cluster.CreateSession()
	if err != nil {
		t.Fatal("CreateSession() should have connected")
	}

	if !strings.Contains(log.String(), "gocql: dns error") {
		t.Fatalf("Expected to receive dns error log message  - got '%s' instead", log.String())
	}
}

func TestDNSLookupError(t *testing.T) {
	log := &testLogger{}
	Logger = log
	defer func() {
		Logger = &defaultLogger{}
	}()

	srv := NewTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	cluster := NewCluster("cassandra1.invalid", "cassandra2.invalid")
	cluster.ProtoVersion = int(defaultProto)
	cluster.disableControlConn = true

	// CreateSession() should attempt to resolve each DNS name "cassandraX.invalid"
	// and fail since it could not resolve any dns entries
	_, err := cluster.CreateSession()
	if err == nil {
		t.Fatal("CreateSession() should have returned an error")
	}

	if !strings.Contains(log.String(), "gocql: dns error") {
		t.Fatalf("Expected to receive dns error log message  - got '%s' instead", log.String())
	}

	if err.Error() != "gocql: unable to create session: failed to resolve any of the provided hostnames" {
		t.Fatalf("Expected CreateSession() to fail with message  - got '%s' instead", err.Error())
	}
}

func TestStartupTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	log := &testLogger{}
	Logger = log
	defer func() {
		Logger = &defaultLogger{}
	}()

	srv := NewTestServer(t, defaultProto, ctx)
	defer srv.Stop()

	// Tell the server to never respond to Startup frame
	atomic.StoreInt32(&srv.TimeoutOnStartup, 1)

	startTime := time.Now()
	cluster := NewCluster(srv.Address)
	cluster.ProtoVersion = int(defaultProto)
	cluster.disableControlConn = true
	// Set very long query connection timeout
	// so we know CreateSession() is using the ConnectTimeout
	cluster.Timeout = time.Second * 5

	// Create session should timeout during connect attempt
	_, err := cluster.CreateSession()
	if err == nil {
		t.Fatal("CreateSession() should have returned a timeout error")
	}

	elapsed := time.Since(startTime)
	if elapsed > time.Second*5 {
		t.Fatal("ConnectTimeout is not respected")
	}

	if !strings.Contains(err.Error(), "no connections were made when creating the session") {
		t.Fatalf("Expected to receive no connections error - got '%s'", err)
	}

	if !strings.Contains(log.String(), "no response to connection startup within timeout") {
		t.Fatalf("Expected to receive timeout log message  - got '%s'", log.String())
	}

	cancel()
}

func TestTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	srv := NewTestServer(t, defaultProto, ctx)
	defer srv.Stop()

	db, err := newTestSession(srv.Address, defaultProto)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		select {
		case <-time.After(5 * time.Second):
			t.Errorf("no timeout")
		case <-ctx.Done():
		}
	}()

	if err := db.Query("kill").WithContext(ctx).Exec(); err == nil {
		t.Fatal("expected error got nil")
	}
	cancel()

	wg.Wait()
}

// TestQueryRetry will test to make sure that gocql will execute
// the exact amount of retry queries designated by the user.
func TestQueryRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := NewTestServer(t, defaultProto, ctx)
	defer srv.Stop()

	db, err := newTestSession(srv.Address, defaultProto)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
			t.Errorf("no timeout")
		}
	}()

	rt := &SimpleRetryPolicy{NumRetries: 1}

	qry := db.Query("kill").RetryPolicy(rt)
	if err := qry.Exec(); err == nil {
		t.Fatalf("expected error")
	}

	requests := atomic.LoadInt64(&srv.nKillReq)
	attempts := qry.Attempts()
	if requests != int64(attempts) {
		t.Fatalf("expected requests %v to match query attempts %v", requests, attempts)
	}

	// the query will only be attempted once, but is being retried
	if requests != int64(rt.NumRetries) {
		t.Fatalf("failed to retry the query %v time(s). Query executed %v times", rt.NumRetries, requests-1)
	}
}

func TestStreams_Protocol1(t *testing.T) {
	srv := NewTestServer(t, protoVersion1, context.Background())
	defer srv.Stop()

	// TODO: these are more like session tests and should instead operate
	// on a single Conn
	cluster := testCluster(srv.Address, protoVersion1)
	cluster.NumConns = 1
	cluster.ProtoVersion = 1

	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	for i := 1; i < 128; i++ {
		// here were just validating that if we send NumStream request we get
		// a response for every stream and the lengths for the queries are set
		// correctly.
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := db.Query("void").Exec(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

func TestStreams_Protocol3(t *testing.T) {
	srv := NewTestServer(t, protoVersion3, context.Background())
	defer srv.Stop()

	// TODO: these are more like session tests and should instead operate
	// on a single Conn
	cluster := testCluster(srv.Address, protoVersion3)
	cluster.NumConns = 1
	cluster.ProtoVersion = 3

	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 1; i < 32768; i++ {
		// the test server processes each conn synchronously
		// here were just validating that if we send NumStream request we get
		// a response for every stream and the lengths for the queries are set
		// correctly.
		if err = db.Query("void").Exec(); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkProtocolV3(b *testing.B) {
	srv := NewTestServer(b, protoVersion3, context.Background())
	defer srv.Stop()

	// TODO: these are more like session tests and should instead operate
	// on a single Conn
	cluster := NewCluster(srv.Address)
	cluster.NumConns = 1
	cluster.ProtoVersion = 3

	db, err := cluster.CreateSession()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err = db.Query("void").Exec(); err != nil {
			b.Fatal(err)
		}
	}
}

// This tests that the policy connection pool handles SSL correctly
func TestPolicyConnPoolSSL(t *testing.T) {
	srv := NewSSLTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	cluster := createTestSslCluster(srv.Address, defaultProto, true)
	cluster.PoolConfig.HostSelectionPolicy = RoundRobinHostPolicy()

	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("failed to create new session: %v", err)
	}

	if err := db.Query("void").Exec(); err != nil {
		t.Fatalf("query failed due to error: %v", err)
	}
	db.Close()

	// wait for the pool to drain
	time.Sleep(100 * time.Millisecond)
	size := db.pool.Size()
	if size != 0 {
		t.Fatalf("connection pool did not drain, still contains %d connections", size)
	}
}

func TestQueryTimeout(t *testing.T) {
	srv := NewTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	cluster := testCluster(srv.Address, defaultProto)
	// Set the timeout arbitrarily low so that the query hits the timeout in a
	// timely manner.
	cluster.Timeout = 1 * time.Millisecond

	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	ch := make(chan error, 1)

	go func() {
		err := db.Query("timeout").Exec()
		if err != nil {
			ch <- err
			return
		}
		t.Errorf("err was nil, expected to get a timeout after %v", db.cfg.Timeout)
	}()

	select {
	case err := <-ch:
		if err != ErrTimeoutNoResponse {
			t.Fatalf("expected to get %v for timeout got %v", ErrTimeoutNoResponse, err)
		}
	case <-time.After(10*time.Millisecond + db.cfg.Timeout):
		// ensure that the query goroutines have been scheduled
		t.Fatalf("query did not timeout after %v", db.cfg.Timeout)
	}
}

func BenchmarkSingleConn(b *testing.B) {
	srv := NewTestServer(b, 3, context.Background())
	defer srv.Stop()

	cluster := testCluster(srv.Address, 3)
	// Set the timeout arbitrarily low so that the query hits the timeout in a
	// timely manner.
	cluster.Timeout = 500 * time.Millisecond
	cluster.NumConns = 1
	db, err := cluster.CreateSession()
	if err != nil {
		b.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := db.Query("void").Exec()
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func TestQueryTimeoutReuseStream(t *testing.T) {
	t.Skip("no longer tests anything")
	// TODO(zariel): move this to conn test, we really just want to check what
	// happens when a conn is

	srv := NewTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	cluster := testCluster(srv.Address, defaultProto)
	// Set the timeout arbitrarily low so that the query hits the timeout in a
	// timely manner.
	cluster.Timeout = 1 * time.Millisecond
	cluster.NumConns = 1

	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	db.Query("slow").Exec()

	err = db.Query("void").Exec()
	if err != nil {
		t.Fatal(err)
	}
}

func TestQueryTimeoutClose(t *testing.T) {
	srv := NewTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	cluster := testCluster(srv.Address, defaultProto)
	// Set the timeout arbitrarily low so that the query hits the timeout in a
	// timely manner.
	cluster.Timeout = 1000 * time.Millisecond
	cluster.NumConns = 1

	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}

	ch := make(chan error)
	go func() {
		err := db.Query("timeout").Exec()
		ch <- err
	}()
	// ensure that the above goroutine gets sheduled
	time.Sleep(50 * time.Millisecond)

	db.Close()
	select {
	case err = <-ch:
	case <-time.After(1 * time.Second):
		t.Fatal("timedout waiting to get a response once cluster is closed")
	}

	if err != ErrConnectionClosed {
		t.Fatalf("expected to get %v got %v", ErrConnectionClosed, err)
	}
}

func TestStream0(t *testing.T) {
	// TODO: replace this with type check
	const expErr = "gocql: received unexpected frame on stream 0"

	var buf bytes.Buffer
	f := newFramer(nil, &buf, nil, protoVersion4)
	f.writeHeader(0, opResult, 0)
	f.writeInt(resultKindVoid)
	f.wbuf[0] |= 0x80
	if err := f.finishWrite(); err != nil {
		t.Fatal(err)
	}

	conn := &Conn{
		r:       bufio.NewReader(&buf),
		streams: streams.New(protoVersion4),
	}

	err := conn.recv()
	if err == nil {
		t.Fatal("expected to get an error on stream 0")
	} else if !strings.HasPrefix(err.Error(), expErr) {
		t.Fatalf("expected to get error prefix %q got %q", expErr, err.Error())
	}
}

func TestConnClosedBlocked(t *testing.T) {
	// issue 664
	const proto = 3

	srv := NewTestServer(t, proto, context.Background())
	defer srv.Stop()
	errorHandler := connErrorHandlerFn(func(conn *Conn, err error, closed bool) {
		t.Log(err)
	})

	s, err := srv.session()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	conn, err := s.connect(srv.host(), errorHandler)
	if err != nil {
		t.Fatal(err)
	}

	if err := conn.conn.Close(); err != nil {
		t.Fatal(err)
	}

	// This will block indefintaly if #664 is not fixed
	err = conn.executeQuery(&Query{stmt: "void"}).Close()
	if !strings.HasSuffix(err.Error(), "use of closed network connection") {
		t.Fatalf("expected to get use of closed networking connection error got: %v\n", err)
	}
}

func TestContext_Timeout(t *testing.T) {
	srv := NewTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	cluster := testCluster(srv.Address, defaultProto)
	cluster.Timeout = 5 * time.Second
	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = db.Query("timeout").WithContext(ctx).Exec()
	if err != context.Canceled {
		t.Fatalf("expected to get context cancel error: %v got %v", context.Canceled, err)
	}
}

func NewTestServer(t testing.TB, protocol uint8, ctx context.Context) *TestServer {
	laddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	listen, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		t.Fatal(err)
	}

	headerSize := 8
	if protocol > protoVersion2 {
		headerSize = 9
	}

	ctx, cancel := context.WithCancel(ctx)
	srv := &TestServer{
		Address:    listen.Addr().String(),
		listen:     listen,
		t:          t,
		protocol:   protocol,
		headerSize: headerSize,
		ctx:        ctx,
		cancel:     cancel,
	}

	go srv.closeWatch()
	go srv.serve()

	return srv
}

func NewSSLTestServer(t testing.TB, protocol uint8, ctx context.Context) *TestServer {
	pem, err := ioutil.ReadFile("testdata/pki/ca.crt")
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(pem) {
		t.Fatalf("Failed parsing or appending certs")
	}
	mycert, err := tls.LoadX509KeyPair("testdata/pki/cassandra.crt", "testdata/pki/cassandra.key")
	if err != nil {
		t.Fatalf("could not load cert")
	}
	config := &tls.Config{
		Certificates: []tls.Certificate{mycert},
		RootCAs:      certPool,
	}
	listen, err := tls.Listen("tcp", "127.0.0.1:0", config)
	if err != nil {
		t.Fatal(err)
	}

	headerSize := 8
	if protocol > protoVersion2 {
		headerSize = 9
	}

	ctx, cancel := context.WithCancel(ctx)
	srv := &TestServer{
		Address:    listen.Addr().String(),
		listen:     listen,
		t:          t,
		protocol:   protocol,
		headerSize: headerSize,
		ctx:        ctx,
		cancel:     cancel,
	}

	go srv.closeWatch()
	go srv.serve()
	return srv
}

type TestServer struct {
	Address          string
	TimeoutOnStartup int32
	t                testing.TB
	nreq             uint64
	listen           net.Listener
	nKillReq         int64
	compressor       Compressor

	protocol   byte
	headerSize int
	ctx        context.Context
	cancel     context.CancelFunc

	quit   chan struct{}
	mu     sync.Mutex
	closed bool
}

func (srv *TestServer) session() (*Session, error) {
	return testCluster(srv.Address, protoVersion(srv.protocol)).CreateSession()
}

func (srv *TestServer) host() *HostInfo {
	hosts, err := hostInfo(srv.Address, 9042)
	if err != nil {
		srv.t.Fatal(err)
	}
	return hosts[0]
}

func (srv *TestServer) closeWatch() {
	<-srv.ctx.Done()

	srv.mu.Lock()
	defer srv.mu.Unlock()

	srv.closeLocked()
}

func (srv *TestServer) serve() {
	defer srv.listen.Close()
	for !srv.isClosed() {
		conn, err := srv.listen.Accept()
		if err != nil {
			break
		}

		go func(conn net.Conn) {
			defer conn.Close()
			for !srv.isClosed() {
				framer, err := srv.readFrame(conn)
				if err != nil {
					if err == io.EOF {
						return
					}
					srv.errorLocked(err)
					return
				}

				atomic.AddUint64(&srv.nreq, 1)

				go srv.process(framer)
			}
		}(conn)
	}
}

func (srv *TestServer) isClosed() bool {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.closed
}

func (srv *TestServer) closeLocked() {
	if srv.closed {
		return
	}

	srv.closed = true

	srv.listen.Close()
	srv.cancel()
}

func (srv *TestServer) Stop() {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.closeLocked()
}

func (srv *TestServer) errorLocked(err interface{}) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.closed {
		return
	}
	srv.t.Error(err)
}

func (srv *TestServer) process(f *framer) {
	head := f.header
	if head == nil {
		srv.errorLocked("process frame with a nil header")
		return
	}

	switch head.op {
	case opStartup:
		if atomic.LoadInt32(&srv.TimeoutOnStartup) > 0 {
			// Do not respond to startup command
			// wait until we get a cancel signal
			select {
			case <-srv.ctx.Done():
				return
			}
		}
		f.writeHeader(0, opReady, head.stream)
	case opOptions:
		f.writeHeader(0, opSupported, head.stream)
		f.writeShort(0)
	case opQuery:
		query := f.readLongString()
		first := query
		if n := strings.Index(query, " "); n > 0 {
			first = first[:n]
		}
		switch strings.ToLower(first) {
		case "kill":
			atomic.AddInt64(&srv.nKillReq, 1)
			f.writeHeader(0, opError, head.stream)
			f.writeInt(0x1001)
			f.writeString("query killed")
		case "use":
			f.writeInt(resultKindKeyspace)
			f.writeString(strings.TrimSpace(query[3:]))
		case "void":
			f.writeHeader(0, opResult, head.stream)
			f.writeInt(resultKindVoid)
		case "timeout":
			<-srv.ctx.Done()
			return
		case "slow":
			go func() {
				f.writeHeader(0, opResult, head.stream)
				f.writeInt(resultKindVoid)
				f.wbuf[0] = srv.protocol | 0x80
				select {
				case <-srv.ctx.Done():
					return
				case <-time.After(50 * time.Millisecond):
					f.finishWrite()
				}
			}()
			return
		default:
			f.writeHeader(0, opResult, head.stream)
			f.writeInt(resultKindVoid)
		}
	case opError:
		f.writeHeader(0, opError, head.stream)
		f.wbuf = append(f.wbuf, f.rbuf...)
	default:
		f.writeHeader(0, opError, head.stream)
		f.writeInt(0)
		f.writeString("not supported")
	}

	f.wbuf[0] = srv.protocol | 0x80

	if err := f.finishWrite(); err != nil {
		srv.errorLocked(err)
	}
}

func (srv *TestServer) readFrame(conn net.Conn) (*framer, error) {
	buf := make([]byte, srv.headerSize)
	head, err := readHeader(conn, buf)
	if err != nil {
		return nil, err
	}
	framer := newFramer(conn, conn, nil, srv.protocol)

	err = framer.readFrame(&head)
	if err != nil {
		return nil, err
	}

	// should be a request frame
	if head.version.response() {
		return nil, fmt.Errorf("expected to read a request frame got version: %v", head.version)
	} else if head.version.version() != srv.protocol {
		return nil, fmt.Errorf("expected to read protocol version 0x%x got 0x%x", srv.protocol, head.version.version())
	}

	return framer, nil
}
