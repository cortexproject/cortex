package clientlimiter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func counter(count, windowStart, lastUpdate int64) *ReplicaCounter {
	return &ReplicaCounter{Count: count, WindowStartUnixNano: windowStart, LastUpdateUnixNano: lastUpdate}
}

func desc(clientID, replicaID string, c *ReplicaCounter) *ClientCounterDesc {
	return &ClientCounterDesc{Clients: map[string]*ClientEntry{
		clientID: {Replicas: map[string]*ReplicaCounter{replicaID: c}},
	}}
}

func TestClientCounterDesc_Merge_HigherCountWinsWithinSameWindow(t *testing.T) {
	d := desc("client-a", "replica-1", counter(3, 1000, 1000))
	change, err := d.Merge(desc("client-a", "replica-1", counter(7, 1000, 1001)), false)
	require.NoError(t, err)
	require.NotNil(t, change)
	require.EqualValues(t, 7, d.Clients["client-a"].Replicas["replica-1"].Count)
}

func TestClientCounterDesc_Merge_LowerCountWithinSameWindowIsIgnored(t *testing.T) {
	d := desc("client-a", "replica-1", counter(7, 1000, 1000))
	change, err := d.Merge(desc("client-a", "replica-1", counter(3, 1000, 999)), false)
	require.NoError(t, err)
	require.Nil(t, change)
	require.EqualValues(t, 7, d.Clients["client-a"].Replicas["replica-1"].Count)
}

func TestClientCounterDesc_Merge_NewerWindowWinsEvenWithLowerCount(t *testing.T) {
	d := desc("client-a", "replica-1", counter(9, 1000, 1000))
	change, err := d.Merge(desc("client-a", "replica-1", counter(1, 2000, 2000)), false)
	require.NoError(t, err)
	require.NotNil(t, change)
	require.EqualValues(t, 1, d.Clients["client-a"].Replicas["replica-1"].Count)
	require.EqualValues(t, 2000, d.Clients["client-a"].Replicas["replica-1"].WindowStartUnixNano)
}

func TestClientCounterDesc_Merge_DistinctReplicasAccumulate(t *testing.T) {
	d := desc("client-a", "replica-1", counter(3, 1000, 1000))
	_, err := d.Merge(desc("client-a", "replica-2", counter(5, 1000, 1000)), false)
	require.NoError(t, err)
	require.Len(t, d.Clients["client-a"].Replicas, 2)
	require.EqualValues(t, 3, d.Clients["client-a"].Replicas["replica-1"].Count)
	require.EqualValues(t, 5, d.Clients["client-a"].Replicas["replica-2"].Count)
}

func TestClientCounterDesc_Merge_Idempotent(t *testing.T) {
	other := desc("client-a", "replica-1", counter(5, 1000, 1000))

	d := NewClientCounterDesc()
	_, err := d.Merge(other.Clone().(*ClientCounterDesc), false)
	require.NoError(t, err)

	// Applying the same state a second time must produce no further change.
	change, err := d.Merge(other.Clone().(*ClientCounterDesc), false)
	require.NoError(t, err)
	require.Nil(t, change)
}

func TestClientCounterDesc_Merge_Commutative(t *testing.T) {
	a := desc("client-a", "replica-1", counter(5, 1000, 1000))
	b := desc("client-a", "replica-2", counter(9, 1000, 1000))

	d1 := NewClientCounterDesc()
	_, err := d1.Merge(a.Clone().(*ClientCounterDesc), false)
	require.NoError(t, err)
	_, err = d1.Merge(b.Clone().(*ClientCounterDesc), false)
	require.NoError(t, err)

	d2 := NewClientCounterDesc()
	_, err = d2.Merge(b.Clone().(*ClientCounterDesc), false)
	require.NoError(t, err)
	_, err = d2.Merge(a.Clone().(*ClientCounterDesc), false)
	require.NoError(t, err)

	require.Equal(t, d1.Clients["client-a"].Replicas["replica-1"].Count, d2.Clients["client-a"].Replicas["replica-1"].Count)
	require.Equal(t, d1.Clients["client-a"].Replicas["replica-2"].Count, d2.Clients["client-a"].Replicas["replica-2"].Count)
}

func TestClientCounterDesc_Merge_NilAndEmptyInputs(t *testing.T) {
	d := NewClientCounterDesc()

	change, err := d.Merge(nil, false)
	require.NoError(t, err)
	require.Nil(t, change)

	change, err = d.Merge((*ClientCounterDesc)(nil), false)
	require.NoError(t, err)
	require.Nil(t, change)

	_, err = d.Merge(&ClientCounterDesc{}, false)
	require.NoError(t, err)
}

func TestClientCounterDesc_Clone_IsIndependentCopy(t *testing.T) {
	d := desc("client-a", "replica-1", counter(5, 1000, 1000))
	clone := d.Clone().(*ClientCounterDesc)

	clone.Clients["client-a"].Replicas["replica-1"].Count = 99

	require.EqualValues(t, 5, d.Clients["client-a"].Replicas["replica-1"].Count)
	require.EqualValues(t, 99, clone.Clients["client-a"].Replicas["replica-1"].Count)
}

func TestPruneStaleReplicas(t *testing.T) {
	d := &ClientCounterDesc{Clients: map[string]*ClientEntry{
		"fresh-client": {Replicas: map[string]*ReplicaCounter{
			"replica-1": counter(5, 1000, int64(time.Second)),
		}},
		"stale-client": {Replicas: map[string]*ReplicaCounter{
			"replica-1": counter(5, 1000, 0),
		}},
		"mixed-client": {Replicas: map[string]*ReplicaCounter{
			"fresh": counter(5, 1000, int64(time.Second)),
			"stale": counter(5, 1000, 0),
		}},
	}}

	pruneStaleReplicas(d, int64(time.Second/2))

	require.Contains(t, d.Clients, "fresh-client")
	require.NotContains(t, d.Clients, "stale-client")
	require.Contains(t, d.Clients, "mixed-client")
	require.Contains(t, d.Clients["mixed-client"].Replicas, "fresh")
	require.NotContains(t, d.Clients["mixed-client"].Replicas, "stale")
}
