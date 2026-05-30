package ring

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

var _ ReadRing = (*mockReadRing)(nil)

// recoveringExecutor wraps each submitted function in a goroutine that
// recovers from panics. This prevents a panic inside a DoBatch callback
// from crashing the test process, while still allowing us to observe
// whether cleanup is called (which depends on defer wg.Done()).
type recoveringExecutor struct{}

func (e *recoveringExecutor) Submit(f func()) {
	go func() {
		defer func() { _ = recover() }()
		f()
	}()
}

func (e *recoveringExecutor) Stop() {}

// mockReadRing is a minimal ReadRing implementation for testing DoBatch.
// It returns a single healthy instance for any Get call.
type mockReadRing struct {
	inst InstanceDesc
}

func (m *mockReadRing) Get(_ uint32, _ Operation, _ []InstanceDesc, _ []string, _ map[string]int) (ReplicationSet, error) {
	return ReplicationSet{
		Instances: []InstanceDesc{m.inst},
		MaxErrors: 0,
	}, nil
}

func (m *mockReadRing) GetAllHealthy(_ Operation) (ReplicationSet, error) {
	return ReplicationSet{}, nil
}
func (m *mockReadRing) GetAllInstanceDescs(_ Operation) ([]InstanceDesc, []InstanceDesc, error) {
	return nil, nil, nil
}
func (m *mockReadRing) GetInstanceDescsForOperation(_ Operation) (map[string]InstanceDesc, error) {
	return nil, nil
}
func (m *mockReadRing) GetReplicationSetForOperation(_ Operation) (ReplicationSet, error) {
	return ReplicationSet{}, nil
}
func (m *mockReadRing) ReplicationFactor() int { return 1 }
func (m *mockReadRing) InstancesCount() int    { return 1 }
func (m *mockReadRing) ShuffleShard(_ string, _ int) ReadRing {
	return m
}
func (m *mockReadRing) ShuffleShardWithZoneStability(_ string, _ int) ReadRing {
	return m
}
func (m *mockReadRing) GetInstanceState(_ string) (InstanceState, error) {
	return ACTIVE, nil
}
func (m *mockReadRing) GetInstanceIdByAddr(_ string) (string, error) {
	return "", nil
}
func (m *mockReadRing) ShuffleShardWithLookback(_ string, _ int, _ time.Duration, _ time.Time) ReadRing {
	return m
}
func (m *mockReadRing) HasInstance(_ string) bool         { return true }
func (m *mockReadRing) CleanupShuffleShardCache(_ string) {}

func TestDoBatchCleanupCalledOnCallbackPanic(t *testing.T) {
	ring := &mockReadRing{
		inst: InstanceDesc{
			Addr:      "addr-0",
			Timestamp: time.Now().Unix(),
			State:     ACTIVE,
			Tokens:    []uint32{0},
		},
	}

	var cleanupCalled atomic.Bool
	cleanup := func() {
		cleanupCalled.Store(true)
	}

	panicCallback := func(_ InstanceDesc, _ []int) error {
		panic("test panic in callback")
	}

	// Use a context with timeout so DoBatch can return. When the callback
	// panics, tracker.record is never called, so neither tracker.done nor
	// tracker.err is signaled. DoBatch exits via ctx.Done(). The key
	// assertion is that cleanup still runs: with defer wg.Done(), the
	// WaitGroup completes despite the panic, unblocking the cleanup goroutine.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_ = DoBatch(ctx, Write, ring, &recoveringExecutor{}, []uint32{0}, panicCallback, cleanup)

	assert.Eventually(t, func() bool {
		return cleanupCalled.Load()
	}, 5*time.Second, 10*time.Millisecond, "cleanup must be called even when callback panics")
}
