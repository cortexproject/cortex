package ruler

import (
	"strconv"
	"testing"
	"time"

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
		return s.computeNextEvalTime(&h, time.Unix(0, now), fakeUserID).UnixNano()
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
