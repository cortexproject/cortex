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

	// Give goroutines a moment to acquire the semaphore slots.
	time.Sleep(100 * time.Millisecond)

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
}
