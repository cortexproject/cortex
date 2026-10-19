package cache

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// certPEM is copied from the golang crypto/tls library and is used here as a
// CA certificate to exercise the TLS dialer construction.
// https://github.com/golang/go/blob/7eb5941b95a588a23f18fa4c22fe42ff0119c311/src/crypto/tls/example_test.go#L127
const certPEM = `-----BEGIN CERTIFICATE-----
MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD0d
7VNhbWvZLWPuj/RtHFjvtJBEwOkhbN/BnnE8rnZR8+sbwnc/KhCk3FhnpHZnQz7B
5aETbbIgmuvewdjvSBSjYzBhMA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAKBggr
BgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MCkGA1UdEQQiMCCCDmxvY2FsaG9zdDo1
NDUzgg4xMjcuMC4wLjE6NTQ1MzAKBggqhkjOPQQDAgNIADBFAiEA2zpJEPQyz6/l
Wf86aX6PepsntZv2GYlA5UpabfT2EZICICpJ5h/iI+i341gBmLiAFQOyTDT+/wQc
6MF9+Yw1Yy0t
-----END CERTIFICATE-----`

func writeTestCA(t *testing.T) string {
	t.Helper()

	path := fmt.Sprintf("%s/ca.pem", t.TempDir())
	require.NoError(t, os.WriteFile(path, []byte(certPEM), 0600))
	return path
}

// When TLS is disabled the dialer should establish a plain TCP connection.
func TestNewMemcachedDialContext_Plain(t *testing.T) {
	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer ln.Close()

	accepted := make(chan struct{}, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- struct{}{}
		conn.Close()
	}()

	dial, err := newMemcachedDialContext(MemcachedClientConfig{Timeout: time.Second})
	require.NoError(t, err)

	conn, err := dial(context.Background(), "tcp", ln.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("expected the server to accept a plain connection")
	}
}

// When TLS is enabled with a valid config the dialer is built without error.
func TestNewMemcachedDialContext_TLS(t *testing.T) {
	cfg := MemcachedClientConfig{Timeout: time.Second, TLSEnabled: true}
	cfg.TLS.CAPath = writeTestCA(t)

	dial, err := newMemcachedDialContext(cfg)
	require.NoError(t, err)
	assert.NotNil(t, dial)
}

// An invalid TLS config must fail dialer construction rather than silently
// falling back to a plaintext connection.
func TestNewMemcachedDialContext_InvalidTLSConfig(t *testing.T) {
	cfg := MemcachedClientConfig{TLSEnabled: true}
	cfg.TLS.CAPath = "/does/not/exist.pem"

	_, err := newMemcachedDialContext(cfg)
	assert.Error(t, err)
}
