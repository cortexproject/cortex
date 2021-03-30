package alertmanager

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/alertmanager/cluster/clusterpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertspb"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type fakePersistableState struct {
	PersistableState

	position int
	readyc   chan struct{}

	getResult *clusterpb.FullState
	getError  error
}

func (f *fakePersistableState) Position() int {
	return f.position
}

func (f *fakePersistableState) GetFullState() (*clusterpb.FullState, error) {
	return f.getResult, f.getError
}

func newFakePersistableState() *fakePersistableState {
	return &fakePersistableState{
		readyc: make(chan struct{}),
	}
}

func (f *fakePersistableState) WaitReady(ctx context.Context) error {
	<-f.readyc
	return nil
}

type fakeStoreSet struct {
	user string
	desc alertspb.FullStateDesc
}

type fakeStore struct {
	alertstore.AlertStore

	setsMtx sync.Mutex
	sets    []fakeStoreSet
}

func (f *fakeStore) SetFullState(ctx context.Context, user string, desc alertspb.FullStateDesc) error {
	f.setsMtx.Lock()
	defer f.setsMtx.Unlock()
	f.sets = append(f.sets, fakeStoreSet{user, desc})
	return nil
}

func TestStatePersister_Position0ShouldWrite(t *testing.T) {
	state := newFakePersistableState()
	store := &fakeStore{}

	s := newStatePersister("user-1", state, store, log.NewNopLogger())
	s.interval = 1 * time.Second

	require.NoError(t, s.StartAsync(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), s))
	})

	time.Sleep(5 * time.Second)

	// Should not have started until the state becomes ready.
	{
		assert.Equal(t, services.Starting, s.Service.State())
		store.setsMtx.Lock()
		numSets := len(store.sets)
		store.setsMtx.Unlock()
		assert.Equal(t, 0, numSets)
	}

	// Should now start.
	{
		state.getResult = &clusterpb.FullState{
			Parts: []clusterpb.Part{
				{
					Key:  "key",
					Data: []byte("data"),
				},
			},
		}

		close(state.readyc)
		require.NoError(t, s.AwaitRunning(context.Background()))
	}

	// Should receive a write to the store.
	{
		var storeSets []fakeStoreSet
		require.Eventually(t, func() bool {
			store.setsMtx.Lock()
			storeSets = store.sets
			store.setsMtx.Unlock()

			return len(storeSets) == 1
		}, 5*time.Second, 100*time.Millisecond)

		expectedSet := alertspb.FullStateDesc{
			State: &clusterpb.FullState{
				Parts: []clusterpb.Part{
					{
						Key:  "key",
						Data: []byte("data"),
					},
				},
			},
		}

		assert.Equal(t, "user-1", storeSets[0].user)
		assert.Equal(t, expectedSet, storeSets[0].desc)
	}
}

func TestStatePersister_Position1ShouldNotWrite(t *testing.T) {
	state := newFakePersistableState()
	state.position = 1
	store := &fakeStore{}

	s := newStatePersister("user-1", state, store, log.NewNopLogger())
	s.interval = 1 * time.Second

	require.NoError(t, s.StartAsync(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), s))
	})

	// Start the persister.
	{
		state.getResult = &clusterpb.FullState{
			Parts: []clusterpb.Part{
				{
					Key:  "key",
					Data: []byte("data"),
				},
			},
		}

		assert.Equal(t, services.Starting, s.Service.State())
		close(state.readyc)
		require.NoError(t, s.AwaitRunning(context.Background()))
	}

	// Wait for the interval to be hit a few times.
	time.Sleep(5 * time.Second)

	// Should not have stored anything.
	{
		assert.Equal(t, services.Running, s.Service.State())
		store.setsMtx.Lock()
		numSets := len(store.sets)
		store.setsMtx.Unlock()
		assert.Equal(t, 0, numSets)
	}
}
