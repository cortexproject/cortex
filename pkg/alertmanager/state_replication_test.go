package alertmanager

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/alertmanager/cluster/clusterpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertspb"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type fakeState struct {
	binary []byte
	merges [][]byte
}

func (s *fakeState) MarshalBinary() ([]byte, error) {
	return s.binary, nil
}

func (s *fakeState) Merge(data []byte) error {
	s.merges = append(s.merges, data)
	return nil
}

type readStateResult struct {
	res      []*clusterpb.FullState
	err      error
	blocking bool
}

type fakeReplicator struct {
	mtx     sync.Mutex
	results map[string]*clusterpb.Part
	read    readStateResult
}

func newFakeReplicator() *fakeReplicator {
	return &fakeReplicator{
		results: make(map[string]*clusterpb.Part),
	}
}

func (f *fakeReplicator) ReplicateStateForUser(ctx context.Context, userID string, p *clusterpb.Part) error {
	f.mtx.Lock()
	f.results[userID] = p
	f.mtx.Unlock()
	return nil
}

func (f *fakeReplicator) GetPositionForUser(_ string) int {
	return 0
}

func (f *fakeReplicator) ReadFullStateForUser(ctx context.Context, userID string) ([]*clusterpb.FullState, error) {
	if userID != "user-1" {
		return nil, errors.New("Unexpected userID")
	}

	if f.read.blocking {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.read.res, f.read.err
}

type fakeAlertStore struct {
	alertstore.AlertStore

	states map[string]alertspb.FullStateDesc
}

func newFakeAlertStore() *fakeAlertStore {
	return &fakeAlertStore{
		states: make(map[string]alertspb.FullStateDesc),
	}
}

func (f *fakeAlertStore) GetFullState(ctx context.Context, user string) (alertspb.FullStateDesc, error) {
	if result, ok := f.states[user]; ok {
		return result, nil
	}
	return alertspb.FullStateDesc{}, alertspb.ErrNotFound
}

func TestStateReplication(t *testing.T) {
	tc := []struct {
		name              string
		replicationFactor int
		message           *clusterpb.Part
		results           map[string]*clusterpb.Part
	}{
		{
			name:              "with a replication factor of <= 1, state is not replicated.",
			replicationFactor: 1,
			message:           &clusterpb.Part{Key: "nflog", Data: []byte("OK")},
			results:           map[string]*clusterpb.Part{},
		},
		{
			name:              "with a replication factor of > 1, state is broadcasted for replication.",
			replicationFactor: 3,
			message:           &clusterpb.Part{Key: "nflog", Data: []byte("OK")},
			results:           map[string]*clusterpb.Part{"user-1": {Key: "nflog", Data: []byte("OK")}},
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			replicator := newFakeReplicator()
			replicator.read = readStateResult{res: nil, err: nil}
			store := newFakeAlertStore()
			s := newReplicatedStates("user-1", tt.replicationFactor, replicator, store, log.NewNopLogger(), reg)

			require.False(t, s.Ready())
			{
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				require.Equal(t, context.DeadlineExceeded, s.WaitReady(ctx))
			}

			require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), s))
			})

			require.True(t, s.Ready())
			{
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				require.NoError(t, s.WaitReady(ctx))
			}

			ch := s.AddState("nflog", &fakeState{}, reg)

			part := tt.message
			d, err := part.Marshal()
			require.NoError(t, err)
			ch.Broadcast(d)

			require.Eventually(t, func() bool {
				replicator.mtx.Lock()
				defer replicator.mtx.Unlock()
				return len(replicator.results) == len(tt.results)
			}, time.Second, time.Millisecond)

			if tt.replicationFactor > 1 {
				assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP alertmanager_state_fetch_replica_state_failed_total Number of times we have failed to read and merge the full state from another replica.
# TYPE alertmanager_state_fetch_replica_state_failed_total counter
alertmanager_state_fetch_replica_state_failed_total 0
# HELP alertmanager_state_fetch_replica_state_total Number of times we have tried to read and merge the full state from another replica.
# TYPE alertmanager_state_fetch_replica_state_total counter
alertmanager_state_fetch_replica_state_total 1
# HELP alertmanager_partial_state_merges_failed_total Number of times we have failed to merge a partial state received for a key.
# TYPE alertmanager_partial_state_merges_failed_total counter
alertmanager_partial_state_merges_failed_total{key="nflog"} 0
# HELP alertmanager_partial_state_merges_total Number of times we have received a partial state to merge for a key.
# TYPE alertmanager_partial_state_merges_total counter
alertmanager_partial_state_merges_total{key="nflog"} 0
# HELP alertmanager_state_initial_sync_completed_total Number of times we have completed syncing initial state for each possible outcome.
# TYPE alertmanager_state_initial_sync_completed_total counter
alertmanager_state_initial_sync_completed_total{outcome="failed"} 0
alertmanager_state_initial_sync_completed_total{outcome="from-replica"} 1
alertmanager_state_initial_sync_completed_total{outcome="from-storage"} 0
alertmanager_state_initial_sync_completed_total{outcome="user-not-found"} 0
# HELP alertmanager_state_initial_sync_total Number of times we have tried to sync initial state from peers or remote storage.
# TYPE alertmanager_state_initial_sync_total counter
alertmanager_state_initial_sync_total 1
# HELP alertmanager_state_replication_failed_total Number of times we have failed to replicate a state to other alertmanagers.
# TYPE alertmanager_state_replication_failed_total counter
alertmanager_state_replication_failed_total{key="nflog"} 0
# HELP alertmanager_state_replication_total Number of times we have tried to replicate a state to other alertmanagers.
# TYPE alertmanager_state_replication_total counter
alertmanager_state_replication_total{key="nflog"} 1
	`),
					"alertmanager_state_fetch_replica_state_failed_total",
					"alertmanager_state_fetch_replica_state_total",
					"alertmanager_partial_state_merges_failed_total",
					"alertmanager_partial_state_merges_total",
					"alertmanager_state_initial_sync_completed_total",
					"alertmanager_state_initial_sync_total",
					"alertmanager_state_replication_failed_total",
					"alertmanager_state_replication_total",
				))

			}
		})
	}
}

func TestStateReplication_Settle(t *testing.T) {

	tc := []struct {
		name              string
		replicationFactor int
		read              readStateResult
		storeStates       map[string]alertspb.FullStateDesc
		results           map[string][][]byte
	}{
		{
			name:              "with a replication factor of <= 1, no state can be read from peers.",
			replicationFactor: 1,
			read:              readStateResult{},
			results: map[string][][]byte{
				"key1": nil,
				"key2": nil,
			},
		},
		{
			name:              "with a replication factor of > 1, state is read from all peers.",
			replicationFactor: 3,
			read: readStateResult{
				res: []*clusterpb.FullState{
					{Parts: []clusterpb.Part{{Key: "key1", Data: []byte("Datum1")}, {Key: "key2", Data: []byte("Datum2")}}},
					{Parts: []clusterpb.Part{{Key: "key1", Data: []byte("Datum3")}, {Key: "key2", Data: []byte("Datum4")}}},
				},
			},
			results: map[string][][]byte{
				"key1": {[]byte("Datum1"), []byte("Datum3")},
				"key2": {[]byte("Datum2"), []byte("Datum4")},
			},
		},
		{
			name:              "with full state having no parts, nothing is merged.",
			replicationFactor: 3,
			read: readStateResult{
				res: []*clusterpb.FullState{{Parts: []clusterpb.Part{}}},
			},
			results: map[string][][]byte{
				"key1": nil,
				"key2": nil,
			},
		},
		{
			name:              "with an unknown key, parts in the same state are merged.",
			replicationFactor: 3,
			read: readStateResult{
				res: []*clusterpb.FullState{{Parts: []clusterpb.Part{
					{Key: "unknown", Data: []byte("Wow")},
					{Key: "key1", Data: []byte("Datum1")},
				}}},
			},
			results: map[string][][]byte{
				"key1": {[]byte("Datum1")},
				"key2": nil,
			},
		},
		{
			name:              "with an unknown key, parts in other states are merged.",
			replicationFactor: 3,
			read: readStateResult{
				res: []*clusterpb.FullState{
					{Parts: []clusterpb.Part{{Key: "unknown", Data: []byte("Wow")}}},
					{Parts: []clusterpb.Part{{Key: "key1", Data: []byte("Datum1")}}},
				},
			},
			results: map[string][][]byte{
				"key1": {[]byte("Datum1")},
				"key2": nil,
			},
		},
		{
			name:              "when reading from replicas fails, state is read from storage.",
			replicationFactor: 3,
			read:              readStateResult{err: errors.New("Read Error 1")},
			storeStates: map[string]alertspb.FullStateDesc{
				"user-1": {
					State: &clusterpb.FullState{
						Parts: []clusterpb.Part{{Key: "key1", Data: []byte("Datum1")}},
					},
				},
			},
			results: map[string][][]byte{
				"key1": {[]byte("Datum1")},
				"key2": nil,
			},
		},
		{
			name:              "when reading from replicas and from storage fails, still become ready.",
			replicationFactor: 3,
			read:              readStateResult{err: errors.New("Read Error 1")},
			storeStates:       map[string]alertspb.FullStateDesc{},
			results: map[string][][]byte{
				"key1": nil,
				"key2": nil,
			},
		},
		{
			name:              "when reading the full state takes too long, hit timeout but become ready.",
			replicationFactor: 3,
			read:              readStateResult{blocking: true},
			results: map[string][][]byte{
				"key1": nil,
				"key2": nil,
			},
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()

			replicator := newFakeReplicator()
			replicator.read = tt.read
			store := newFakeAlertStore()
			store.states = tt.storeStates
			s := newReplicatedStates("user-1", tt.replicationFactor, replicator, store, log.NewNopLogger(), reg)

			key1State := &fakeState{}
			key2State := &fakeState{}

			s.AddState("key1", key1State, reg)
			s.AddState("key2", key2State, reg)

			s.settleReadTimeout = 1 * time.Second

			assert.False(t, s.Ready())

			require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), s))
			})

			assert.True(t, s.Ready())

			// Note: We don't actually test beyond Merge() here, just that all data is forwarded.
			assert.Equal(t, tt.results["key1"], key1State.merges)
			assert.Equal(t, tt.results["key2"], key2State.merges)
		})
	}
}

func TestStateReplication_GetFullState(t *testing.T) {

	tc := []struct {
		name   string
		data   map[string][]byte
		result *clusterpb.FullState
	}{
		{
			name: "no keys",
			data: map[string][]byte{},
			result: &clusterpb.FullState{
				Parts: []clusterpb.Part{},
			},
		},
		{
			name: "zero length data",
			data: map[string][]byte{
				"key1": {},
			},
			result: &clusterpb.FullState{
				Parts: []clusterpb.Part{
					{Key: "key1", Data: []byte{}},
				},
			},
		},
		{
			name: "keys with data",
			data: map[string][]byte{
				"key1": []byte("Datum1"),
				"key2": []byte("Datum2"),
			},
			result: &clusterpb.FullState{
				Parts: []clusterpb.Part{
					{Key: "key1", Data: []byte("Datum1")},
					{Key: "key2", Data: []byte("Datum2")},
				},
			},
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			s := newReplicatedStates("user-1", 1, nil, nil, log.NewNopLogger(), reg)

			for key, datum := range tt.data {
				state := &fakeState{binary: datum}
				s.AddState(key, state, reg)
			}

			result, err := s.GetFullState()
			require.NoError(t, err)

			// Key ordering is undefined for the code under test.
			sort.Slice(result.Parts, func(i, j int) bool { return result.Parts[i].Key < result.Parts[j].Key })

			assert.Equal(t, tt.result, result)
		})
	}
}
