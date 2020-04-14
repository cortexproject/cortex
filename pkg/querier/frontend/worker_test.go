package frontend

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	httpgrpc_server "github.com/weaveworks/common/httpgrpc/server"
	"google.golang.org/grpc/naming"

	"github.com/cortexproject/cortex/pkg/util"
)

func TestResetParallelism(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("Hello World"))
		assert.NoError(t, err)
	})

	tests := []struct {
		name                string
		parallelism         int
		totalParallelism    int
		numManagers         int
		expectedConcurrency int32
	}{
		{
			name:                "Test create least one worker per manager",
			parallelism:         0,
			totalParallelism:    0,
			numManagers:         2,
			expectedConcurrency: 2,
		},
		{
			name:                "Test concurrency per query frontend configuration",
			parallelism:         4,
			totalParallelism:    0,
			numManagers:         2,
			expectedConcurrency: 8,
		},
		{
			name:                "Test Total Parallelism with a remainder",
			parallelism:         1,
			totalParallelism:    7,
			numManagers:         4,
			expectedConcurrency: 7,
		},
		{
			name:                "Test Total Parallelism dividing evenly",
			parallelism:         1,
			totalParallelism:    6,
			numManagers:         2,
			expectedConcurrency: 6,
		},
		{
			name:                "Test Total Parallelism at least one worker per manager",
			parallelism:         1,
			totalParallelism:    3,
			numManagers:         6,
			expectedConcurrency: 6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
		})
	}
}

type mockDNSWatcher struct {
	updates []*naming.Update //nolint:staticcheck
	wg      sync.WaitGroup
}

func (m *mockDNSWatcher) Next() ([]*naming.Update, error) { //nolint:staticcheck
	m.wg.Add(1)
	m.wg.Wait()

	var update *naming.Update //nolint:staticcheck
	update, m.updates = m.updates[0], m.updates[1:]

	return []*naming.Update{update}, nil //nolint:staticcheck
}

func (m *mockDNSWatcher) Close() {

}

func TestDNSWatcher(t *testing.T) {
	tests := []struct {
		name              string
		updates           []*naming.Update //nolint:staticcheck
		expectedFrontends [][]string
	}{
		{
			name: "Test add one",
			updates: []*naming.Update{ //nolint:staticcheck
				{
					Op:   naming.Add,
					Addr: "blerg",
				},
			},
			expectedFrontends: [][]string{
				{
					"blerg",
				},
			},
		},
		{
			name: "Test add one and delete",
			updates: []*naming.Update{ //nolint:staticcheck
				{
					Op:   naming.Add,
					Addr: "blerg",
				},
				{
					Op:   naming.Delete,
					Addr: "blerg",
				},
			},
			expectedFrontends: [][]string{
				{
					"blerg",
				},
				{},
			},
		},
		{
			name: "Test delete nonexistent",
			updates: []*naming.Update{ //nolint:staticcheck
				{
					Op:   naming.Delete,
					Addr: "blerg",
				},
				{
					Op:   naming.Add,
					Addr: "blerg",
				},
			},
			expectedFrontends: [][]string{
				{},
				{
					"blerg",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := WorkerConfig{
				Parallelism:      0,
				TotalParallelism: 0,
			}

			watcher := &mockDNSWatcher{
				updates: tt.updates,
			}
			w := &worker{
				cfg:      cfg,
				log:      util.Logger,
				managers: map[string]*frontendManager{},
				watcher:  watcher,
			}

			ctx, cancel := context.WithCancel(context.Background())
			go w.watchDNSLoop(ctx) //nolint:errcheck
			time.Sleep(50 * time.Millisecond)

			for i := range tt.updates {
				watcher.wg.Done()

				time.Sleep(50 * time.Millisecond)

				// confirm all expected frontends exist
				for _, expected := range tt.expectedFrontends[i] {
					_, ok := w.managers[expected]

					assert.Truef(t, ok, "Unable to find %s on iteration %d", expected, i)
				}
			}

			cancel()
		})
	}
}
