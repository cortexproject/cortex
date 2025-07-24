package ring

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func createStartingRing() *Ring {
	// Init the ring.
	ringDesc := &Desc{Ingesters: map[string]InstanceDesc{
		"instance-1": {Addr: "127.0.0.1", State: ACTIVE, Timestamp: time.Now().Unix()},
		"instance-2": {Addr: "127.0.0.2", State: PENDING, Timestamp: time.Now().Unix()},
		"instance-3": {Addr: "127.0.0.3", State: JOINING, Timestamp: time.Now().Unix()},
		"instance-4": {Addr: "127.0.0.4", State: LEAVING, Timestamp: time.Now().Unix()},
		"instance-5": {Addr: "127.0.0.5", State: ACTIVE, Timestamp: time.Now().Unix()},
	}}

	ring := &Ring{
		cfg:                 Config{HeartbeatTimeout: time.Minute},
		ringDesc:            ringDesc,
		ringTokens:          ringDesc.GetTokens(),
		ringTokensByZone:    ringDesc.getTokensByZone(),
		ringInstanceByToken: ringDesc.getTokensInfo(),
		ringZones:           getZones(ringDesc.getTokensByZone()),
		strategy:            NewDefaultReplicationStrategy(),
		KVClient:            &MockClient{},
	}

	return ring
}

func TestWaitRingStability_ShouldReturnAsSoonAsMinStabilityIsReachedOnNoChanges(t *testing.T) {
	t.Parallel()

	const (
		minStability = 2 * time.Second
		maxWaiting   = 10 * time.Second
	)

	ring := createStartingRing()

	startTime := time.Now()
	require.NoError(t, WaitRingStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.GreaterOrEqual(t, elapsedTime, minStability)
	assert.Less(t, elapsedTime, 2*minStability)
}

func TestWaitRingTokensStability_ShouldReturnAsSoonAsMinStabilityIsReachedOnNoChanges(t *testing.T) {
	t.Parallel()

	const (
		minStability = 2 * time.Second
		maxWaiting   = 10 * time.Second
	)

	ring := createStartingRing()

	startTime := time.Now()
	require.NoError(t, WaitRingTokensStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.GreaterOrEqual(t, elapsedTime, minStability)
	assert.Less(t, elapsedTime, 2*minStability)
}

func addInstanceAfterSomeTime(ring *Ring, addInstanceAfter time.Duration) {
	go func() {
		time.Sleep(addInstanceAfter)

		ring.mtx.Lock()
		defer ring.mtx.Unlock()
		ringDesc := ring.ringDesc
		instanceID := fmt.Sprintf("127.0.0.%d", len(ringDesc.Ingesters)+1)
		ringDesc.Ingesters[instanceID] = InstanceDesc{Addr: instanceID, State: ACTIVE, Timestamp: time.Now().Unix()}
		ring.ringDesc = ringDesc
		ring.ringTokens = ringDesc.GetTokens()
		ring.ringTokensByZone = ringDesc.getTokensByZone()
		ring.ringInstanceByToken = ringDesc.getTokensInfo()
		ring.ringZones = getZones(ringDesc.getTokensByZone())
	}()
}

func TestWaitRingStability_ShouldReturnOnceMinStabilityOfInstancesHasBeenReached(t *testing.T) {
	t.Parallel()

	const (
		minStability     = 3 * time.Second
		addInstanceAfter = 2 * time.Second
		maxWaiting       = 15 * time.Second
	)

	ring := createStartingRing()

	// Add 1 new instance after some time.
	addInstanceAfterSomeTime(ring, addInstanceAfter)

	startTime := time.Now()
	require.NoError(t, WaitRingStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.GreaterOrEqual(t, elapsedTime, minStability+addInstanceAfter)
	assert.LessOrEqual(t, elapsedTime, minStability+addInstanceAfter+3*time.Second)
}

func TestWaitRingTokensStability_ShouldReturnOnceMinStabilityOfInstancesHasBeenReached(t *testing.T) {
	t.Parallel()

	const (
		minStability     = 3 * time.Second
		addInstanceAfter = 2 * time.Second
		maxWaiting       = 15 * time.Second
	)

	ring := createStartingRing()

	// Add 1 new instance after some time.
	addInstanceAfterSomeTime(ring, addInstanceAfter)

	startTime := time.Now()
	require.NoError(t, WaitRingTokensStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.GreaterOrEqual(t, elapsedTime, minStability+addInstanceAfter)
	assert.LessOrEqual(t, elapsedTime, minStability+addInstanceAfter+3*time.Second)
}

func addInstancesPeriodically(ring *Ring) chan struct{} {
	// Keep changing the ring.
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Second):
				ring.mtx.Lock()
				ringDesc := ring.ringDesc
				instanceID := fmt.Sprintf("127.0.0.%d", len(ringDesc.Ingesters)+1)
				ringDesc.Ingesters[instanceID] = InstanceDesc{Addr: instanceID, State: ACTIVE, Timestamp: time.Now().Unix()}
				ring.ringDesc = ringDesc
				ring.ringTokens = ringDesc.GetTokens()
				ring.ringTokensByZone = ringDesc.getTokensByZone()
				ring.ringInstanceByToken = ringDesc.getTokensInfo()
				ring.ringZones = getZones(ringDesc.getTokensByZone())

				ring.mtx.Unlock()
			}
		}
	}()
	return done
}

func TestWaitRingStability_ShouldReturnErrorIfInstancesAddedAndMaxWaitingIsReached(t *testing.T) {
	t.Parallel()

	const (
		minStability = 2 * time.Second
		maxWaiting   = 7 * time.Second
	)

	ring := createStartingRing()

	done := addInstancesPeriodically(ring)
	defer close(done)

	startTime := time.Now()
	require.Equal(t, context.DeadlineExceeded, WaitRingStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.GreaterOrEqual(t, elapsedTime, maxWaiting)
}

func TestWaitRingTokensStability_ShouldReturnErrorIfInstancesAddedAndMaxWaitingIsReached(t *testing.T) {
	t.Parallel()

	const (
		minStability = 2 * time.Second
		maxWaiting   = 7 * time.Second
	)

	ring := createStartingRing()

	done := addInstancesPeriodically(ring)
	defer close(done)

	startTime := time.Now()
	require.Equal(t, context.DeadlineExceeded, WaitRingTokensStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.GreaterOrEqual(t, elapsedTime, maxWaiting)
}

// Keep changing the ring in a way to avoid repeating the same set of states for at least 2 sec
func changeStatePeriodically(ring *Ring) chan struct{} {
	done := make(chan struct{})
	go func() {
		instanceToMutate := "instance-1"
		states := []InstanceState{PENDING, JOINING, ACTIVE, LEAVING}
		stateIdx := 0

		for states[stateIdx] != ring.ringDesc.Ingesters[instanceToMutate].State {
			stateIdx++
		}

		for {
			select {
			case <-done:
				return
			case <-time.After(time.Second):
				stateIdx++
				ring.mtx.Lock()
				ringDesc := ring.ringDesc
				desc := InstanceDesc{Addr: "127.0.0.1", State: states[stateIdx%len(states)], Timestamp: time.Now().Unix()}
				ringDesc.Ingesters[instanceToMutate] = desc
				ring.mtx.Unlock()
			}
		}
	}()

	return done
}

func TestWaitRingStability_ShouldReturnErrorIfInstanceStateIsChangingAndMaxWaitingIsReached(t *testing.T) {
	t.Parallel()

	const (
		minStability = 2 * time.Second
		maxWaiting   = 7 * time.Second
	)

	ring := createStartingRing()

	// Keep changing the ring.
	done := changeStatePeriodically(ring)
	defer close(done)

	startTime := time.Now()
	require.Equal(t, context.DeadlineExceeded, WaitRingStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.GreaterOrEqual(t, elapsedTime, maxWaiting)
}

func TestWaitRingTokensStability_ShouldReturnOnceMinStabilityOfInstancesHasBeenReachedWhileStateCanChange(t *testing.T) {
	t.Parallel()

	const (
		minStability = 2 * time.Second
		maxWaiting   = 7 * time.Second
	)

	ring := createStartingRing()

	// Keep changing the ring.
	done := changeStatePeriodically(ring)
	defer close(done)

	startTime := time.Now()
	require.NoError(t, WaitRingTokensStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.GreaterOrEqual(t, elapsedTime, minStability)
	assert.Less(t, elapsedTime, 2*minStability)
}

func TestWaitInstanceState_Timeout(t *testing.T) {
	t.Parallel()

	const (
		instanceID      = "test"
		timeoutDuration = time.Second
	)

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	ring := &RingMock{}
	ring.On("GetInstanceState", mock.Anything, mock.Anything).Return(ACTIVE, nil)

	err := WaitInstanceState(ctx, ring, instanceID, PENDING)

	assert.Equal(t, context.DeadlineExceeded, err)
	ring.AssertCalled(t, "GetInstanceState", instanceID)
}

func TestWaitInstanceState_TimeoutOnError(t *testing.T) {
	t.Parallel()

	const (
		instanceID      = "test"
		timeoutDuration = time.Second
	)

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	ring := &RingMock{}
	ring.On("GetInstanceState", mock.Anything, mock.Anything).Return(PENDING, errors.New("instance not found in the ring"))

	err := WaitInstanceState(ctx, ring, instanceID, ACTIVE)

	assert.Equal(t, context.DeadlineExceeded, err)
	ring.AssertCalled(t, "GetInstanceState", instanceID)
}

func TestWaitInstanceState_ExitsAfterActualStateEqualsState(t *testing.T) {
	t.Parallel()

	const (
		instanceID      = "test"
		timeoutDuration = time.Second
	)

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	ring := &RingMock{}
	ring.On("GetInstanceState", mock.Anything, mock.Anything).Return(ACTIVE, nil)

	err := WaitInstanceState(ctx, ring, instanceID, ACTIVE)

	assert.Nil(t, err)
	ring.AssertNumberOfCalls(t, "GetInstanceState", 1)
}

func TestResetZoneMap(t *testing.T) {
	zoneMap := map[string]int{
		"zone-1": 2,
		"zone-2": 2,
	}

	newZoneMap := resetZoneMap(zoneMap)
	assert.Equal(t, 0, len(newZoneMap))
}

func TestResetZoneMap_NilAsAnArgument(t *testing.T) {
	zoneMap := resetZoneMap(nil)
	zoneMap["zone-1"] = 1 // this should not panic
	assert.Equal(t, 1, len(zoneMap))
}
