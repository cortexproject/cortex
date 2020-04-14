package frontend

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/stretchr/testify/assert"
	httpgrpc_server "github.com/weaveworks/common/httpgrpc/server"
)

func TestResetParallelism(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("Hello World"))
		assert.NoError(t, err)
	})

	tests := []struct {
		parallelism         int
		totalParallelism    int
		numManagers         int
		expectedConcurrency int32
	}{
		{
			parallelism:         0,
			totalParallelism:    0,
			numManagers:         2,
			expectedConcurrency: 2,
		},
		{
			parallelism:         1,
			totalParallelism:    0,
			numManagers:         2,
			expectedConcurrency: 2,
		},
		{
			parallelism:         1,
			totalParallelism:    7,
			numManagers:         4,
			expectedConcurrency: 7,
		},
		{
			parallelism:         1,
			totalParallelism:    3,
			numManagers:         6,
			expectedConcurrency: 6,
		},
		{
			parallelism:         1,
			totalParallelism:    6,
			numManagers:         2,
			expectedConcurrency: 6,
		},
	}

	for _, tt := range tests {

		cfg := WorkerConfig{
			Parallelism:      tt.parallelism,
			TotalParallelism: tt.totalParallelism,
		}

		w := &worker{
			cfg:      cfg,
			log:      util.Logger,
			managers: map[string]*frontendManager{},
		}

		for i := 0; i < tt.numManagers; i++ {
			w.managers[strconv.Itoa(i)] = newFrontendManager(context.Background(), util.Logger, httpgrpc_server.NewServer(handler), &mockFrontendClient{}, 0, 100000000)
		}

		w.resetParallelism()
		time.Sleep(100 * time.Millisecond)

		concurrency := int32(0)
		for _, mgr := range w.managers {
			concurrency += mgr.currentProcessors.Load()
		}
		assert.Equal(t, tt.expectedConcurrency, concurrency)

		err := w.stopping(nil)
		assert.NoError(t, err)

		concurrency = int32(0)
		for _, mgr := range w.managers {
			concurrency += mgr.currentProcessors.Load()
		}
		assert.Equal(t, int32(0), concurrency)
	}
}
