package frontend

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	httpgrpc_server "github.com/weaveworks/common/httpgrpc/server"

	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
)

func TestResetConcurrency(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("Hello World"))
		assert.NoError(t, err)
	})

	tests := []struct {
		name                string
		parallelism         int
		maxConcurrent       int
		numManagers         int
		expectedConcurrency int32
	}{
		{
			name:                "Test create least one worker per manager",
			parallelism:         0,
			maxConcurrent:       0,
			numManagers:         2,
			expectedConcurrency: 2,
		},
		{
			name:                "Test concurrency per query frontend configuration",
			parallelism:         4,
			maxConcurrent:       0,
			numManagers:         2,
			expectedConcurrency: 8,
		},
		{
			name:                "Test Total Parallelism with a remainder",
			parallelism:         1,
			maxConcurrent:       7,
			numManagers:         4,
			expectedConcurrency: 7,
		},
		{
			name:                "Test Total Parallelism dividing evenly",
			parallelism:         1,
			maxConcurrent:       6,
			numManagers:         2,
			expectedConcurrency: 6,
		},
		{
			name:                "Test Total Parallelism at least one worker per manager",
			parallelism:         1,
			maxConcurrent:       3,
			numManagers:         6,
			expectedConcurrency: 6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := WorkerConfig{
				Parallelism:         tt.parallelism,
				MatchMaxConcurrency: tt.maxConcurrent > 0,
			}
			querierCfg := querier.Config{
				MaxConcurrent: tt.maxConcurrent,
			}

			w := &worker{
				cfg:        cfg,
				querierCfg: querierCfg,
				log:        util.Logger,
				managers:   map[string]*frontendManager{},
			}

			for i := 0; i < tt.numManagers; i++ {
				w.managers[strconv.Itoa(i)] = newFrontendManager(context.Background(), util.Logger, httpgrpc_server.NewServer(handler), mockCloser{}, &mockFrontendClient{}, grpcclient.ConfigWithTLS{}, "querier")
			}

			w.resetConcurrency()
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
		})
	}
}
