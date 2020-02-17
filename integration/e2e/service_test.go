package e2e

import (
	"fmt"
	"math"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util"
)

// freePort returns port that is free now.
func freePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", ":0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	return l.Addr().(*net.TCPAddr).Port, l.Close()
}

func TestWaitMetric(t *testing.T) {
	p, err := freePort()
	require.NoError(t, err)

	srv := &http.Server{
		Addr: net.JoinHostPort("localhost", fmt.Sprintf("%v", p)),
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
		srv.ListenAndServe()
	}()

	s := &HTTPService{
		httpPort: 0,
		ConcreteService: &ConcreteService{
			networkPortsContainerToLocal: map[int]int{
				0: p,
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

func TestWaitMetric_Nan(t *testing.T) {
	p, err := freePort()
	require.NoError(t, err)

	srv := &http.Server{
		Addr: net.JoinHostPort("localhost", fmt.Sprintf("%v", p)),
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
		srv.ListenAndServe()
	}()

	s := &HTTPService{
		httpPort: 0,
		ConcreteService: &ConcreteService{
			networkPortsContainerToLocal: map[int]int{
				0: p,
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
