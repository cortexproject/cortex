package ring

import (
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescFedRing_ZonalOwnershipMatchesWatchedRing(t *testing.T) {
	instances := handPickedInstances(map[string]func(*ownershipTestInstance){"a2": withState(JOINING)})
	watched := newOwnershipTestRing(t, 2, newOwnershipTestDesc(instances))

	cfg := Config{HeartbeatTimeout: ownershipTestTimeout, ReplicationFactor: 2, ZoneAwarenessEnabled: true}
	fed, err := NewDescFedRing(cfg, testRingName, testRingKey, &MockClient{}, log.NewNopLogger())
	require.NoError(t, err)
	fed.UpdateDesc(newOwnershipTestDesc(instances))

	assert.Equal(t, watched.ZonalOwnership(Write), fed.ZonalOwnership(Write))
	assert.Equal(t, watched.ZonalOwnership(WriteNoExtend), fed.ZonalOwnership(WriteNoExtend))
}

func TestDescFedRing_UpdateDescReplacesState(t *testing.T) {
	cfg := Config{HeartbeatTimeout: ownershipTestTimeout, ReplicationFactor: 2, ZoneAwarenessEnabled: true}
	fed, err := NewDescFedRing(cfg, testRingName, testRingKey, &MockClient{}, log.NewNopLogger())
	require.NoError(t, err)

	fed.UpdateDesc(newOwnershipTestDesc(handPickedInstances(nil)))
	require.InDelta(t, 0.25, fed.ZonalOwnership(Write)["a2"], ownershipTolerance)

	fed.UpdateDesc(newOwnershipTestDesc(handPickedInstances(map[string]func(*ownershipTestInstance){"a2": withState(LEAVING)})))
	assert.Zero(t, fed.ZonalOwnership(Write)["a2"])
}

func TestDescFedRing_UpdateDescLeavesDescUnchanged(t *testing.T) {
	desc := NewDesc()
	desc.AddIngester("a-1", "a-1-addr", "a", []uint32{300, 100}, ACTIVE, time.Time{})
	desc.AddIngester("b-1", "b-1-addr", "b", []uint32{400}, ACTIVE, time.Time{})

	cfg := Config{HeartbeatTimeout: ownershipTestTimeout, ReplicationFactor: 1, ExcludedZones: []string{"b"}}
	fed, err := NewDescFedRing(cfg, testRingName, testRingKey, &MockClient{}, log.NewNopLogger())
	require.NoError(t, err)

	fed.UpdateDesc(desc)

	// updateRingState deletes excluded zones and sorts tokens in place.
	assert.Contains(t, desc.Ingesters, "b-1")
	assert.Equal(t, []uint32{300, 100}, desc.Ingesters["a-1"].Tokens)
	assert.NotContains(t, fed.ZonalOwnership(Write), "b-1")
}

func TestDescFedRing_DoesNotRegisterMetrics(t *testing.T) {
	cfg := Config{HeartbeatTimeout: ownershipTestTimeout, ReplicationFactor: 1}
	// Two rings with the same name in one process would panic on duplicate
	// registration if their metrics were registered.
	for range 2 {
		_, err := NewDescFedRing(cfg, testRingName, testRingKey, &MockClient{}, log.NewNopLogger())
		require.NoError(t, err)
	}
}
