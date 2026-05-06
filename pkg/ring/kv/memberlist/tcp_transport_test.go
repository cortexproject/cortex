package memberlist

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"net"
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
