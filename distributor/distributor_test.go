package distributor

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/weaveworks/cortex/ring"
)

type mockRing struct {
	prometheus.Counter
}

func (r mockRing) Get(key uint32, n int, op ring.Operation) ([]*ring.IngesterDesc, error) {
	return nil, nil
}

func (r mockRing) BatchGet(keys []uint32, n int, op ring.Operation) ([][]*ring.IngesterDesc, error) {
	return nil, nil
}

func (r mockRing) GetAll() []*ring.IngesterDesc {
	return nil
}

type mockIngester struct {
}

func TestDistributor(t *testing.T) {
	ring := mockRing{
		Counter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "foo",
		}),
	}

	_, err := New(Config{
		ReplicationFactor:   3,
		MinReadSuccesses:    2,
		HeartbeatTimeout:    1 * time.Minute,
		RemoteTimeout:       1 * time.Minute,
		ClientCleanupPeriod: 1 * time.Minute,
		IngestionRateLimit:  10000,
		IngestionBurstSize:  10000,
	}, ring)
	if err != nil {
		t.Fatal(err)
	}

}
