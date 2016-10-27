package ring_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/weaveworks/cortex/ring"
)

const (
	// XXX: This leads to a race condition in the tests. We should instead either:
	// a) pass in "Now"
	// b) separate the timeout layer from the selection layer
	forever = 24 * time.Hour
)

type FakeStateClient struct {
	values chan interface{}
}

// WatchKey implements CoordinationStateClient.
//
// Pulls from the 'values' channel.
func (c FakeStateClient) WatchKey(key string, factory ring.InstanceFactory, done <-chan struct{}, f func(interface{}) bool) {
	for {
		select {
		case <-done:
			return
		case value := <-c.values:
			if !f(value) {
				return
			}
		}
	}
}

func Test_NewRingIsEmptyRing(t *testing.T) {
	r := ring.New(FakeStateClient{})
	defer r.Stop()
	assert.Equal(t, len(r.GetAll(forever)), 0)
}
