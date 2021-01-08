package ring

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateTokens(t *testing.T) {
	tokens := GenerateTokens(1000000, nil)

	dups := make(map[uint32]int)

	for ix, v := range tokens {
		if ox, ok := dups[v]; ok {
			t.Errorf("Found duplicate token %d, tokens[%d]=%d, tokens[%d]=%d", v, ix, tokens[ix], ox, tokens[ox])
		} else {
			dups[v] = ix
		}
	}
}

func TestGenerateTokensIgnoresOldTokens(t *testing.T) {
	first := GenerateTokens(1000000, nil)
	second := GenerateTokens(1000000, first)

	dups := make(map[uint32]bool)

	for _, v := range first {
		dups[v] = true
	}

	for _, v := range second {
		if dups[v] {
			t.Fatal("GenerateTokens returned old token")
		}
	}
}

func TestWaitRingStabilityShouldReturnAsSoonAsMinStabilityIsReachedOnNoChanges(t *testing.T) {
	t.Parallel()

	const (
		minStability = 2 * time.Second
		maxWaiting   = 10 * time.Second
	)

	// Init the ring.
	ringDesc := &Desc{Ingesters: map[string]IngesterDesc{
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
		strategy:            NewDefaultReplicationStrategy(true),
	}

	startTime := time.Now()
	require.NoError(t, WaitRingStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.InDelta(t, minStability, elapsedTime, float64(2*time.Second))
}

func TestWaitRingStabilityShouldReturnOnceMinStabilityHasBeenReached(t *testing.T) {
	t.Parallel()

	const (
		minStability     = 3 * time.Second
		addInstanceAfter = 2 * time.Second
		maxWaiting       = 15 * time.Second
	)

	// Init the ring.
	ringDesc := &Desc{Ingesters: map[string]IngesterDesc{
		"instance-1": {Addr: "instance-1", State: ACTIVE, Timestamp: time.Now().Unix()},
		"instance-2": {Addr: "instance-2", State: PENDING, Timestamp: time.Now().Unix()},
		"instance-3": {Addr: "instance-3", State: JOINING, Timestamp: time.Now().Unix()},
		"instance-4": {Addr: "instance-4", State: LEAVING, Timestamp: time.Now().Unix()},
		"instance-5": {Addr: "instance-5", State: ACTIVE, Timestamp: time.Now().Unix()},
	}}

	ring := &Ring{
		cfg:                 Config{HeartbeatTimeout: time.Minute},
		ringDesc:            ringDesc,
		ringTokens:          ringDesc.GetTokens(),
		ringTokensByZone:    ringDesc.getTokensByZone(),
		ringInstanceByToken: ringDesc.getTokensInfo(),
		ringZones:           getZones(ringDesc.getTokensByZone()),
		strategy:            NewDefaultReplicationStrategy(true),
	}

	// Add 1 new instance after some time.
	go func() {
		time.Sleep(addInstanceAfter)

		ring.mtx.Lock()
		defer ring.mtx.Unlock()

		instanceID := fmt.Sprintf("instance-%d", len(ringDesc.Ingesters)+1)
		ringDesc.Ingesters[instanceID] = IngesterDesc{Addr: instanceID, State: ACTIVE, Timestamp: time.Now().Unix()}
		ring.ringDesc = ringDesc
		ring.ringTokens = ringDesc.GetTokens()
		ring.ringTokensByZone = ringDesc.getTokensByZone()
		ring.ringInstanceByToken = ringDesc.getTokensInfo()
		ring.ringZones = getZones(ringDesc.getTokensByZone())
	}()

	startTime := time.Now()
	require.NoError(t, WaitRingStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.GreaterOrEqual(t, elapsedTime.Milliseconds(), (minStability + addInstanceAfter).Milliseconds())
	assert.LessOrEqual(t, elapsedTime.Milliseconds(), (minStability + addInstanceAfter + 3*time.Second).Milliseconds())
}

func TestWaitRingStabilityShouldReturnErrorIfMaxWaitingIsReached(t *testing.T) {
	t.Parallel()

	const (
		minStability = 2 * time.Second
		maxWaiting   = 7 * time.Second
	)

	// Init the ring.
	ringDesc := &Desc{Ingesters: map[string]IngesterDesc{
		"instance-1": {Addr: "instance-1", State: ACTIVE, Timestamp: time.Now().Unix()},
		"instance-2": {Addr: "instance-2", State: PENDING, Timestamp: time.Now().Unix()},
		"instance-3": {Addr: "instance-3", State: JOINING, Timestamp: time.Now().Unix()},
		"instance-4": {Addr: "instance-4", State: LEAVING, Timestamp: time.Now().Unix()},
		"instance-5": {Addr: "instance-5", State: ACTIVE, Timestamp: time.Now().Unix()},
	}}

	ring := &Ring{
		cfg:                 Config{HeartbeatTimeout: time.Minute},
		ringDesc:            ringDesc,
		ringTokens:          ringDesc.GetTokens(),
		ringTokensByZone:    ringDesc.getTokensByZone(),
		ringInstanceByToken: ringDesc.getTokensInfo(),
		ringZones:           getZones(ringDesc.getTokensByZone()),
		strategy:            NewDefaultReplicationStrategy(true),
	}

	// Keep changing the ring.
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Second):
				ring.mtx.Lock()

				instanceID := fmt.Sprintf("instance-%d", len(ringDesc.Ingesters)+1)
				ringDesc.Ingesters[instanceID] = IngesterDesc{Addr: instanceID, State: ACTIVE, Timestamp: time.Now().Unix()}
				ring.ringDesc = ringDesc
				ring.ringTokens = ringDesc.GetTokens()
				ring.ringTokensByZone = ringDesc.getTokensByZone()
				ring.ringInstanceByToken = ringDesc.getTokensInfo()
				ring.ringZones = getZones(ringDesc.getTokensByZone())

				ring.mtx.Unlock()
			}
		}
	}()

	startTime := time.Now()
	require.Equal(t, context.DeadlineExceeded, WaitRingStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.InDelta(t, maxWaiting, elapsedTime, float64(2*time.Second))
}
