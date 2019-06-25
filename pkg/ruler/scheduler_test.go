package ruler

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/ruler/rulegroup"
	"github.com/prometheus/prometheus/rules"
	"github.com/stretchr/testify/assert"
)

type fakeHasher struct {
	something uint32
	data      *[]byte
}

func (h *fakeHasher) Write(data []byte) (int, error) {
	h.data = &data
	return len(data), nil
}
func (h *fakeHasher) Reset() {
	h.data = nil
}
func (h *fakeHasher) Size() int {
	return 0
}
func (h *fakeHasher) BlockSize() int {
	return 64
}
func (h *fakeHasher) Sum([]byte) []byte {
	return []byte{}
}
func (h *fakeHasher) Sum64() uint64 {
	i, _ := strconv.ParseUint(string(*h.data), 10, 64)
	return i
}

func TestSchedulerComputeNextEvalTime(t *testing.T) {
	h := fakeHasher{}
	// normal intervals are in seconds; this is nanoseconds for the test
	s := scheduler{evaluationInterval: 15}
	evalTime := func(now, hashResult int64) int64 {
		// We use the fake hasher to give us control over the hash output
		// so that we can test the wrap-around behaviour of the modulo
		fakeUserID := strconv.FormatInt(hashResult, 10)
		return computeNextEvalTime(&h, time.Unix(0, now), 15, fakeUserID).UnixNano()
	}
	{
		cycleStartTime := int64(30)
		cycleOffset := int64(0) // cycleStartTime % s.evaluationInterval
		// Check simple case where hash >= current cycle position
		assert.Equal(t, cycleStartTime+0, evalTime(cycleStartTime, cycleOffset+0))
		assert.Equal(t, cycleStartTime+1, evalTime(cycleStartTime, cycleOffset+1))
		assert.Equal(t, cycleStartTime+14, evalTime(cycleStartTime, cycleOffset+14))
		// Check things are cyclic
		assert.Equal(t, evalTime(cycleStartTime, 0), evalTime(cycleStartTime, int64(s.evaluationInterval)))
	}
	{
		midCycleTime := int64(35)
		cycleOffset := int64(5) // midCycleTime % s.evaluationInterval
		// Check case where hash can be either greater or less than current cycle position
		assert.Equal(t, midCycleTime+0, evalTime(midCycleTime, cycleOffset+0))
		assert.Equal(t, midCycleTime+1, evalTime(midCycleTime, cycleOffset+1))
		assert.Equal(t, midCycleTime+9, evalTime(midCycleTime, cycleOffset+9))
		assert.Equal(t, midCycleTime+10, evalTime(midCycleTime, cycleOffset-5))
		assert.Equal(t, midCycleTime+14, evalTime(midCycleTime, cycleOffset-1))
	}
}

func TestSchedulerRulesOverlap(t *testing.T) {
	s := newScheduler(nil, 15, 15, nil)
	userID := "bob"
	groupOne := "test1"
	groupTwo := "test2"
	next := time.Now()

	ruleSetsOne := []configs.RuleGroup{
		rulegroup.NewRuleGroup(groupOne, "default", userID, []rules.Rule{nil}),
	}

	ruleSetsTwo := []configs.RuleGroup{
		rulegroup.NewRuleGroup(groupTwo, "default", userID, []rules.Rule{nil}),
	}
	userChanOne := make(chan struct{})
	userChanTwo := make(chan struct{})

	cfgOne := userConfig{rules: ruleSetsOne, done: userChanOne}
	cfgTwo := userConfig{rules: ruleSetsTwo, done: userChanTwo}

	s.updateUserConfig(context.Background(), cfgOne)
	w0 := workItem{userID: userID, groupName: groupOne, scheduled: next, done: userChanOne}
	s.workItemDone(w0)
	item := s.q.Dequeue().(workItem)
	assert.Equal(t, item.groupName, groupOne)

	// create a new workitem for the updated ruleset
	w1 := workItem{userID: userID, groupName: groupTwo, scheduled: next, done: userChanTwo}

	// Apply the new config, scheduling the previous config to be dropped
	s.updateUserConfig(context.Background(), cfgTwo)

	// Reschedule the old config first, then the new config
	s.workItemDone(w0)
	s.workItemDone(w1)

	// Ensure the old config was dropped due to the done channel being closed
	// when the new user config was updated
	item = s.q.Dequeue().(workItem)
	assert.Equal(t, item.groupName, groupTwo)

	s.q.Close()
	assert.Equal(t, nil, s.q.Dequeue())
}
