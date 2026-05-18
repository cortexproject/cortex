package memberlist

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/concurrency"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

func TestTCPTransport_WriteTo_ShouldNotLogAsWarningExpectedFailures(t *testing.T) {
	tests := map[string]struct {
		setup          func(t *testing.T, cfg *TCPTransportConfig)
		remoteAddr     string
		expectedLogs   string
		unexpectedLogs string
	}{
		"should not log 'connection refused' by default": {
			remoteAddr:     "localhost:12345",
			unexpectedLogs: "connection refused",
		},
		"should log 'connection refused' if debug log level is enabled": {
			setup: func(t *testing.T, cfg *TCPTransportConfig) {
				cfg.TransportDebug = true
			},
			remoteAddr:   "localhost:12345",
			expectedLogs: "connection refused",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			logs := &concurrency.SyncBuffer{}
			logger := log.NewLogfmtLogger(logs)

			cfg := TCPTransportConfig{}
			flagext.DefaultValues(&cfg)
			cfg.BindAddrs = []string{"localhost"}
			cfg.BindPort = 0
			if testData.setup != nil {
				testData.setup(t, &cfg)
			}

			transport, err := NewTCPTransport(cfg, logger)
			require.NoError(t, err)

			_, err = transport.WriteTo([]byte("test"), testData.remoteAddr)
			require.NoError(t, err)

			if testData.expectedLogs != "" {
				assert.Contains(t, logs.String(), testData.expectedLogs)
			}
			if testData.unexpectedLogs != "" {
				assert.NotContains(t, logs.String(), testData.unexpectedLogs)
			}
		})
	}
}

func TestTCPTransport_PacketDigestMismatch(t *testing.T) {
	logs := &concurrency.SyncBuffer{}
	logger := log.NewLogfmtLogger(logs)

	cfg := TCPTransportConfig{}
	flagext.DefaultValues(&cfg)
	cfg.BindAddrs = []string{"127.0.0.1"}
	cfg.BindPort = 0

	transport, err := NewTCPTransport(cfg, logger)
	require.NoError(t, err)
	defer transport.Shutdown() //nolint:errcheck

	port := transport.GetAutoBindPort()
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	defer conn.Close() //nolint:errcheck

	ourAddr := "127.0.0.1:0"
	data := []byte("test data")
	wrongDigest := make([]byte, md5.Size) // All zeros, incorrect digest

	var buf bytes.Buffer
	buf.WriteByte(byte(packet))
	buf.WriteByte(byte(len(ourAddr)))
	buf.WriteString(ourAddr)
	buf.Write(data)
	buf.Write(wrongDigest)

	_, err = conn.Write(buf.Bytes())
	require.NoError(t, err)
	conn.Close() // Close connection to terminate server's io.ReadAll

	packetReceived := make(chan struct{})
	go func() {
		select {
		case <-transport.PacketCh():
			close(packetReceived)
		case <-time.After(500 * time.Millisecond):
			// Expected timeout
		}
	}()

	select {
	case <-packetReceived:
		t.Fatal("Received corrupted packet, expected it to be dropped")
	case <-time.After(500 * time.Millisecond):
		// Success
	}

	assert.Contains(t, logs.String(), "packet digest mismatch")

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(transport.activeConnections) == 0
	}, 2*time.Second, 10*time.Millisecond, "activeConnections should be back to 0 after digest mismatch")
}

func TestTCPTransport_PacketReadTimeout(t *testing.T) {
	logger := log.NewNopLogger()

	cfg := TCPTransportConfig{}
	flagext.DefaultValues(&cfg)
	cfg.BindAddrs = []string{"127.0.0.1"}
	cfg.BindPort = 0
	cfg.PacketReadTimeout = 200 * time.Millisecond

	transport, err := NewTCPTransport(cfg, logger)
	require.NoError(t, err)
	defer transport.Shutdown() //nolint:errcheck

	port := transport.GetAutoBindPort()
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	defer conn.Close() //nolint:errcheck

	// Send packet type byte and address header, then stall – never send payload.
	ourAddr := "127.0.0.1:0"
	var buf bytes.Buffer
	buf.WriteByte(byte(packet))
	buf.WriteByte(byte(len(ourAddr)))
	buf.WriteString(ourAddr)
	_, err = conn.Write(buf.Bytes())
	require.NoError(t, err)

	// The transport should close the connection after PacketReadTimeout.
	// We verify this by trying to read from the conn; once the server side
	// closes it due to the deadline, our Read should return an error.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second)) //nolint:errcheck
	oneByte := make([]byte, 1)
	_, readErr := conn.Read(oneByte)
	assert.Error(t, readErr, "expected connection to be closed by server after read timeout")

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(transport.activeConnections) == 0
	}, 2*time.Second, 10*time.Millisecond, "activeConnections should be back to 0 after read timeout")
}

func TestTCPTransport_MaxPacketSize(t *testing.T) {
	logs := &concurrency.SyncBuffer{}
	logger := log.NewLogfmtLogger(logs)

	cfg := TCPTransportConfig{}
	flagext.DefaultValues(&cfg)
	cfg.BindAddrs = []string{"127.0.0.1"}
	cfg.BindPort = 0
	cfg.MaxPacketSize = 128

	transport, err := NewTCPTransport(cfg, logger)
	require.NoError(t, err)
	defer transport.Shutdown() //nolint:errcheck

	port := transport.GetAutoBindPort()
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	defer conn.Close() //nolint:errcheck

	// Build a packet that exceeds MaxPacketSize.
	ourAddr := "127.0.0.1:0"
	oversizedData := make([]byte, int(cfg.MaxPacketSize)+64)
	digest := md5.Sum(oversizedData)

	var buf bytes.Buffer
	buf.WriteByte(byte(packet))
	buf.WriteByte(byte(len(ourAddr)))
	buf.WriteString(ourAddr)
	buf.Write(oversizedData)
	buf.Write(digest[:])

	_, err = conn.Write(buf.Bytes())
	require.NoError(t, err)
	conn.Close() //nolint:errcheck

	// Packet should be dropped; nothing must arrive on packetCh.
	select {
	case <-transport.PacketCh():
		t.Fatal("oversized packet should have been dropped")
	case <-time.After(500 * time.Millisecond):
		// success
	}

	assert.Contains(t, logs.String(), "packet too large")

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(transport.activeConnections) == 0
	}, 2*time.Second, 10*time.Millisecond, "activeConnections should be back to 0 after oversized packet")
}

func TestTCPTransport_MaxConcurrentConnections(t *testing.T) {
	logs := &concurrency.SyncBuffer{}
	logger := log.NewLogfmtLogger(logs)

	const maxConns = 3

	cfg := TCPTransportConfig{}
	flagext.DefaultValues(&cfg)
	cfg.BindAddrs = []string{"127.0.0.1"}
	cfg.BindPort = 0
	cfg.PacketReadTimeout = 5 * time.Second
	cfg.MaxConcurrentConnections = maxConns

	transport, err := NewTCPTransport(cfg, logger)
	require.NoError(t, err)
	defer transport.Shutdown() //nolint:errcheck

	port := transport.GetAutoBindPort()

	openSlowConn := func() net.Conn {
		c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		require.NoError(t, err)
		// Send packet type byte to enter the packet branch, then stall.
		_, err = c.Write([]byte{byte(packet)})
		require.NoError(t, err)
		return c
	}

	// Fill up the semaphore.
	holders := make([]net.Conn, maxConns)
	for i := range maxConns {
		holders[i] = openSlowConn()
	}
	defer func() {
		for _, c := range holders {
			c.Close() //nolint:errcheck
		}
	}()

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(transport.receivedPackets) == float64(maxConns)
	}, 2*time.Second, 10*time.Millisecond, "server never accepted %d connections", maxConns)

	// This extra connection should be rejected.
	var wg sync.WaitGroup
	wg.Go(func() {
		extra, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			return // connection may be refused outright
		}
		defer extra.Close() //nolint:errcheck
		// Try to read; the server should close immediately.
		extra.SetReadDeadline(time.Now().Add(time.Second)) //nolint:errcheck
		buf := make([]byte, 1)
		extra.Read(buf) //nolint:errcheck
	})
	wg.Wait()

	assert.Contains(t, logs.String(), "max concurrent connections reached")

	assert.GreaterOrEqual(t, testutil.ToFloat64(transport.rejectedConnections), float64(1))
	assert.Equal(t, float64(maxConns), testutil.ToFloat64(transport.activeConnections))
}

// TestTCPTransport_StreamHoldsSlotUntilClose asserts that
// -memberlist.max-concurrent-connections bounds the number of *live* inbound
// TCP connections: once a stream conn has been handed off to memberlist via
// StreamCh(), its slot stays held until the conn is actually closed.
func TestTCPTransport_StreamHoldsSlotUntilClose(t *testing.T) {
	logger := log.NewNopLogger()

	const maxConns = 2

	cfg := TCPTransportConfig{}
	flagext.DefaultValues(&cfg)
	cfg.BindAddrs = []string{"127.0.0.1"}
	cfg.BindPort = 0
	cfg.PacketReadTimeout = 5 * time.Second
	cfg.MaxConcurrentConnections = maxConns

	transport, err := NewTCPTransport(cfg, logger)
	require.NoError(t, err)
	defer transport.Shutdown() //nolint:errcheck

	port := transport.GetAutoBindPort()

	// Consumer goroutine: drains StreamCh and holds conns alive (never closes
	// them) — simulating memberlist actively using streams.
	var heldMu sync.Mutex
	var held []net.Conn
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case c := <-transport.StreamCh():
				heldMu.Lock()
				held = append(held, c)
				heldMu.Unlock()
			}
		}
	}()
	defer func() {
		close(done)
		heldMu.Lock()
		for _, c := range held {
			c.Close() //nolint:errcheck
		}
		heldMu.Unlock()
	}()

	openStreamConn := func() net.Conn {
		c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		require.NoError(t, err)
		_, err = c.Write([]byte{byte(stream)})
		require.NoError(t, err)
		return c
	}

	// Fill the semaphore with maxConns live stream handoffs.
	clients := make([]net.Conn, 0, maxConns+1)
	defer func() {
		for _, c := range clients {
			c.Close() //nolint:errcheck
		}
	}()
	for range maxConns {
		clients = append(clients, openStreamConn())
	}

	// Wait until memberlist side has observed all maxConns streams.
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(transport.incomingStreams) == float64(maxConns)
	}, 2*time.Second, 10*time.Millisecond)

	// activeConnections must reflect both stream slots still being held.
	assert.Equal(t, float64(maxConns), testutil.ToFloat64(transport.activeConnections))

	// One extra stream conn. If the slot is correctly held for the conn's
	// real lifetime, the transport must reject this one because all slots
	// are still occupied by the held streams above.
	clients = append(clients, openStreamConn())

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(transport.rejectedConnections) >= 1
	}, 2*time.Second, 10*time.Millisecond,
		"expected extra stream conn to be rejected while %d prior streams are held open, "+
			"but the transport released the slot on handoff — flag does not bound live connections",
		maxConns)
}

// TestTCPTransport_ActiveConnectionsCountsLiveStreams asserts that
// active_connections reflects the number of *live* inbound TCP connections,
// including stream connections that have been handed off to memberlist but
// have not yet been closed. Operators rely on this gauge to size
// -memberlist.max-concurrent-connections.
func TestTCPTransport_ActiveConnectionsCountsLiveStreams(t *testing.T) {
	logger := log.NewNopLogger()

	const numStreams = 3

	cfg := TCPTransportConfig{}
	flagext.DefaultValues(&cfg)
	cfg.BindAddrs = []string{"127.0.0.1"}
	cfg.BindPort = 0
	cfg.PacketReadTimeout = 5 * time.Second
	// Plenty of headroom — we want this test to assert on the gauge, not on the
	// semaphore.
	cfg.MaxConcurrentConnections = numStreams + 5

	transport, err := NewTCPTransport(cfg, logger)
	require.NoError(t, err)
	defer transport.Shutdown() //nolint:errcheck

	port := transport.GetAutoBindPort()

	// Consumer goroutine: drains StreamCh and holds conns alive (never closes
	// them) — simulating memberlist actively using streams.
	var heldMu sync.Mutex
	var held []net.Conn
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case c := <-transport.StreamCh():
				heldMu.Lock()
				held = append(held, c)
				heldMu.Unlock()
			}
		}
	}()
	defer func() {
		close(done)
		heldMu.Lock()
		for _, c := range held {
			c.Close() //nolint:errcheck
		}
		heldMu.Unlock()
	}()

	openStreamConn := func() net.Conn {
		c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		require.NoError(t, err)
		_, err = c.Write([]byte{byte(stream)})
		require.NoError(t, err)
		return c
	}

	// Open numStreams stream connections. Memberlist is holding all of them
	// open (consumer goroutine above never closes them).
	clients := make([]net.Conn, 0, numStreams)
	defer func() {
		for _, c := range clients {
			c.Close() //nolint:errcheck
		}
	}()
	for range numStreams {
		clients = append(clients, openStreamConn())
	}

	// Wait until all stream handoffs have been observed.
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(transport.incomingStreams) == float64(numStreams)
	}, 2*time.Second, 10*time.Millisecond)

	// While numStreams stream conns are still alive on the memberlist side,
	// active_connections should equal numStreams.
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(transport.activeConnections) == float64(numStreams)
	}, 2*time.Second, 10*time.Millisecond,
		"expected active_connections == %d while %d stream conns are held open by memberlist; got %v",
		numStreams, numStreams, testutil.ToFloat64(transport.activeConnections))
}
