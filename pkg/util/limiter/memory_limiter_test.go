package limiter

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/common/logging"
	"go.uber.org/atomic"

	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

func TestMemoryLimiter(t *testing.T) {
	tc := []struct {
		name             string
		alloc            uint64
		redThreshold     uint64
		yellowThreshold  uint64
		failFast         bool
		gc               bool
		memAfterGC       uint64
		waitStart        bool
		totalRequests    int
		pendingRequests  int
		finishedRequests int
		failedRequests   int
		finalState       State
	}{
		{
			name:             "state=green",
			alloc:            0,
			redThreshold:     80,
			yellowThreshold:  10,
			gc:               false,
			totalRequests:    5,
			pendingRequests:  0,
			finishedRequests: 5,
			failedRequests:   0,
			finalState:       Yellow,
		},
		{
			name:             "state=red;",
			alloc:            100,
			redThreshold:     80,
			yellowThreshold:  40,
			gc:               false,
			totalRequests:    5,
			pendingRequests:  4, // When under memory pressure, only the first request must finish.
			finishedRequests: 1,
			failedRequests:   0,
			finalState:       Red,
		},
		{
			name:             "state=red; stateAfterGC=green",
			alloc:            100,
			redThreshold:     80,
			yellowThreshold:  70,
			gc:               true,
			memAfterGC:       0,
			totalRequests:    5,
			pendingRequests:  0, // Was under memory pressure, but the GC cleared the memory.
			finishedRequests: 5, // All requests should finish after memory is cleared by GC.
			failedRequests:   0,
			finalState:       Green,
		},
		{
			name:             "state=yellow; stateAfterGC=green",
			alloc:            70,
			redThreshold:     80,
			yellowThreshold:  40,
			gc:               true,
			memAfterGC:       0,
			totalRequests:    5,
			pendingRequests:  0, // Was under memory pressure, but the GC cleared the memory.
			finishedRequests: 5, // All requests should finish after memory is cleared by GC.
			failedRequests:   0,
			finalState:       Green,
		},
		{
			name:             "state=red; stateAfterGC=yellow",
			alloc:            200,
			redThreshold:     80,
			yellowThreshold:  10,
			gc:               true,
			memAfterGC:       20,
			totalRequests:    5,
			pendingRequests:  0, // Was under memory pressure, but the GC cleared the memory.
			finishedRequests: 5, // All requests should finish after memory is cleared by GC.
			failedRequests:   0,
			finalState:       Yellow,
		},
		{
			name:             "state=yellow; waitStart=true",
			alloc:            45,
			redThreshold:     80,
			yellowThreshold:  40,
			gc:               false,
			waitStart:        true,
			totalRequests:    5,
			pendingRequests:  0, // All requests should wait for alloc to go below reset. No requests should be queued.
			finishedRequests: 0, // Since no requests are queued, no requests should finish.
			failedRequests:   0,
			finalState:       Yellow,
		},
		{
			name:             "state=red; failFast=true",
			alloc:            90,
			redThreshold:     80,
			yellowThreshold:  40,
			gc:               false,
			failFast:         true,
			totalRequests:    5,
			pendingRequests:  0, // All requests should finish.
			finishedRequests: 5, // All requests should finish.
			failedRequests:   4, // All except the first request should fail.
			finalState:       Red,
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			finished := atomic.NewInt64(0)
			failed := atomic.NewInt64(0)
			workDuration := 50 * time.Millisecond
			tickerDuration := 50 * time.Millisecond
			alloc := atomic.Uint64{}
			alloc.Store(tt.alloc)

			reg := prometheus.NewRegistry()
			logger, _ := util_log.NewPrometheusLogger(logging.Level{}, logging.Format{})
			lim := &HeapMemoryLimiter{
				failFast:         tt.failFast,
				memCheckInterval: time.Second,
				redThreshold:     tt.redThreshold,
				yellowThreshold:  tt.yellowThreshold,
				readMemStatsFn: func(m *runtime.MemStats) {
					m.Alloc = alloc.Load()
				},
				ticker:     time.NewTicker(tickerDuration),
				logger:     logger,
				yellowCond: newCond(5*time.Second, logger),
				greenCond:  newCond(5*time.Minute, logger),
				stateGuage: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
					Namespace: "cortex",
					Name:      "memory_limiter_state",
					Help:      "The state of the memory limiter (Green is 0, Yellow is 1, Red is 2).",
				}),
			}
			lim.start()
			waitStart := tt.waitStart
			time.Sleep(2 * tickerDuration) // Wait for at-least one iteration of the memLimiter

			f := func(lim *HeapMemoryLimiter, id int64) {
				job := lim.NewJob()
				defer job.Complete()

				for i := 0; i <= 5; i++ {
					t.Logf("Processing request: %v data %v", id, i)
					err := job.Continue(context.Background())
					if err != nil {
						failed.Inc()
						break
					}
					alloc.Inc()
					time.Sleep(workDuration)
				}

				t.Logf("Finished %d", id)
				finished.Inc()
			}

			for i := 1; i <= tt.totalRequests; i++ {
				id := int64(i)
				go func() {
					if waitStart {
						_ = lim.Wait(context.Background(), 0, Green, false, false)
					}
					f(lim, id)
				}()
				time.Sleep(tickerDuration)
			}

			if tt.gc {
				// Mock a gc cycle which clears the alloc.
				alloc.Store(tt.memAfterGC)
			}

			// Give enough time for all the goroutines to finish.
			time.Sleep(workDuration * time.Duration(tt.totalRequests*2+1))

			lim.jobsMutex.Lock()
			assert.Equal(t, tt.pendingRequests, len(lim.jobsQueue))
			assert.Equal(t, int64(tt.finishedRequests), finished.Load())
			assert.Equal(t, int64(tt.failedRequests), failed.Load())
			lim.jobsMutex.Unlock()

			assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
				# TYPE cortex_memory_limiter_state gauge
				# HELP cortex_memory_limiter_state The state of the memory limiter (Green is 0, Yellow is 1, Red is 2).
				cortex_memory_limiter_state %d
			`, tt.finalState)),
				"cortex_memory_limiter_state",
			))
		})
	}
}
