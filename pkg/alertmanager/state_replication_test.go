package alertmanager

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/alertmanager/cluster/clusterpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-kit/kit/log"

	"github.com/cortexproject/cortex/pkg/util/services"
)

type fakeState struct{}

func (s fakeState) MarshalBinary() ([]byte, error) {
	return []byte{}, nil
}
func (s fakeState) Merge(_ []byte) error {
	return nil
}

type fakeReplicator struct {
	mtx     sync.Mutex
	results map[string]*clusterpb.Part
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
			s := newReplicatedStates("user-1", tt.replicationFactor, replicator, log.NewNopLogger(), reg)

			require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), s))
			})

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
# HELP alertmanager_partial_state_merges_failed_total Number of times we have failed to merge a partial state received for a key.
# TYPE alertmanager_partial_state_merges_failed_total counter
alertmanager_partial_state_merges_failed_total{key="nflog"} 0
# HELP alertmanager_partial_state_merges_total Number of times we have received a partial state to merge for a key.
# TYPE alertmanager_partial_state_merges_total counter
alertmanager_partial_state_merges_total{key="nflog"} 0
# HELP alertmanager_state_replication_failed_total Number of times we have failed to replicate a state to other alertmanagers.
# TYPE alertmanager_state_replication_failed_total counter
alertmanager_state_replication_failed_total{key="nflog"} 0
# HELP alertmanager_state_replication_total Number of times we have tried to replicate a state to other alertmanagers.
# TYPE alertmanager_state_replication_total counter
alertmanager_state_replication_total{key="nflog"} 1
	`)))

			}
		})
	}
}
