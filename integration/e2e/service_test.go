package e2e

import (
	"math"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util"
)

func TestWaitSumMetric(t *testing.T) {
	// Listen on a random port before starting the HTTP server, to
	// make sure the port is already open when we'll call WaitSumMetric()
	// the first time (this avoid flaky tests).
	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer ln.Close()

	// Get the port.
	_, addrPort, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)

	port, err := strconv.Atoi(addrPort)
	require.NoError(t, err)

	// Start an HTTP server exposing the metrics.
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`
# HELP metric_c cheescake
# TYPE metric_c GAUGE
metric_c 20
# HELP metric_a cheescake
# TYPE metric_a GAUGE
metric_a 1
metric_a{first="value1"} 10
metric_a{first="value1", something="x"} 4
metric_a{first="value1", something2="a"} 203
metric_a{first="value2"} 2
metric_a{second="value1"} 1
# HELP metric_b cheescake
# TYPE metric_b GAUGE
metric_b 1000
`))
		}),
	}
	defer srv.Close()

	go func() {
		_ = srv.Serve(ln)
	}()

	s := &HTTPService{
		httpPort: 0,
		ConcreteService: &ConcreteService{
			networkPortsContainerToLocal: map[int]int{
				0: port,
			},
		},
	}

	s.SetBackoff(util.BackoffConfig{
		MinBackoff: 300 * time.Millisecond,
		MaxBackoff: 600 * time.Millisecond,
		MaxRetries: 50,
	})
	require.NoError(t, s.WaitSumMetric("metric_a", 221))

	// No retry.
	s.SetBackoff(util.BackoffConfig{
		MinBackoff: 0,
		MaxBackoff: 0,
		MaxRetries: 1,
	})
	require.Error(t, s.WaitSumMetric("metric_a", 16))
}

func TestWaitSumMetric_Nan(t *testing.T) {
	// Listen on a random port before starting the HTTP server, to
	// make sure the port is already open when we'll call WaitSumMetric()
	// the first time (this avoid flaky tests).
	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer ln.Close()

	// Get the port.
	_, addrPort, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)

	port, err := strconv.Atoi(addrPort)
	require.NoError(t, err)

	// Start an HTTP server exposing the metrics.
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`
# HELP metric_c cheescake
# TYPE metric_c GAUGE
metric_c 20
# HELP metric_a cheescake
# TYPE metric_a GAUGE
metric_a 1
metric_a{first="value1"} 10
metric_a{first="value1", something="x"} 4
metric_a{first="value1", something2="a"} 203
metric_a{first="value1", something3="b"} Nan
metric_a{first="value2"} 2
metric_a{second="value1"} 1
# HELP metric_b cheescake
# TYPE metric_b GAUGE
metric_b 1000
`))
		}),
	}
	defer srv.Close()

	go func() {
		_ = srv.Serve(ln)
	}()

	s := &HTTPService{
		httpPort: 0,
		ConcreteService: &ConcreteService{
			networkPortsContainerToLocal: map[int]int{
				0: port,
			},
		},
	}

	s.SetBackoff(util.BackoffConfig{
		MinBackoff: 300 * time.Millisecond,
		MaxBackoff: 600 * time.Millisecond,
		MaxRetries: 50,
	})
	require.NoError(t, s.WaitSumMetric("metric_a", math.NaN()))

	// No retry.
	s.SetBackoff(util.BackoffConfig{
		MinBackoff: 0,
		MaxBackoff: 0,
		MaxRetries: 1,
	})
	require.Error(t, s.WaitSumMetric("metric_a", 16))
}
