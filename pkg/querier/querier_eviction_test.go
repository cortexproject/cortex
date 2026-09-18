package querier

import (
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/queryeviction"
	"github.com/cortexproject/cortex/pkg/util/resource"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// simpleMonitor implements resource.IMonitor for testing.
type simpleMonitor struct{}

func (m *simpleMonitor) GetCPUUtilization() float64  { return 0.5 }
func (m *simpleMonitor) GetHeapUtilization() float64 { return 0.5 }

// Compile-time check that simpleMonitor implements resource.IMonitor.
var _ resource.IMonitor = (*simpleMonitor)(nil)

func TestQuerier_EvictionIntegration(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		evictionCPU     float64
		evictionHeap    float64
		resourceMonitor resource.IMonitor
		expectWrapped   bool
		expectService   bool
	}{
		"engine wrapped when eviction enabled and resourceMonitor provided": {
			evictionCPU:     0.85,
			evictionHeap:    0.85,
			resourceMonitor: &simpleMonitor{},
			expectWrapped:   true,
			expectService:   true,
		},
		"engine not wrapped when eviction disabled (both thresholds 0)": {
			evictionCPU:     0,
			evictionHeap:    0,
			resourceMonitor: &simpleMonitor{},
			expectWrapped:   false,
			expectService:   false,
		},
		"engine not wrapped when resourceMonitor is nil": {
			evictionCPU:     0.85,
			evictionHeap:    0.85,
			resourceMonitor: nil,
			expectWrapped:   false,
			expectService:   false,
		},
		"engine wrapped with CPU-only threshold": {
			evictionCPU:     0.9,
			evictionHeap:    0,
			resourceMonitor: &simpleMonitor{},
			expectWrapped:   true,
			expectService:   true,
		},
		"engine wrapped with heap-only threshold": {
			evictionCPU:     0,
			evictionHeap:    0.9,
			resourceMonitor: &simpleMonitor{},
			expectWrapped:   true,
			expectService:   true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var cfg Config
			flagext.DefaultValues(&cfg)
			// Disable active query tracker to avoid mmap error in tests.
			cfg.ActiveQueryTrackerDir = ""

			cfg.QueryProtection = configs.QueryProtection{
				Eviction: configs.EvictionConfig{
					Threshold: configs.Threshold{
						CPUUtilization:  tc.evictionCPU,
						HeapUtilization: tc.evictionHeap,
					},
					CheckInterval:        1 * time.Second,
					CooldownPeriod:       3,
					EvictionMetric:       "fetched_samples",
					MaxEvictionsPerCycle: 1,
				},
			}

			overrides := validation.NewOverrides(DefaultLimitsConfig(), nil)

			distributor := &MockDistributor{}
			distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(&client.QueryStreamResponse{}, nil)

			queryables := []QueryableWithFilter{}

			_, _, eng, svc := New(cfg, overrides, distributor, queryables, nil, log.NewNopLogger(), nil, tc.resourceMonitor)

			if tc.expectWrapped {
				_, ok := eng.(*queryeviction.ResourceEvictingEngine)
				assert.True(t, ok, "expected engine to be *queryeviction.ResourceEvictingEngine")
			} else {
				_, ok := eng.(*queryeviction.ResourceEvictingEngine)
				assert.False(t, ok, "expected engine NOT to be *queryeviction.ResourceEvictingEngine")
			}

			if tc.expectService {
				assert.NotNil(t, svc, "expected evictor service to be non-nil")
			} else {
				assert.Nil(t, svc, "expected evictor service to be nil")
			}
		})
	}
}
