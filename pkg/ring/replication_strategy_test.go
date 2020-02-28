package ring

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestReplicationStrategy(t *testing.T) {
	for i, tc := range []struct {
		RF, LiveIngesters, DeadIngesters int
		op                               Operation // Will default to READ
		ExpectedMaxFailure               int
		ExpectedError                    string
	}{
		// Ensure it works for a single ingester, for local testing.
		{
			RF:                 1,
			LiveIngesters:      1,
			ExpectedMaxFailure: 0,
		},

		{
			RF:            1,
			DeadIngesters: 1,
			ExpectedError: "at least 1 live replicas required, could only find 0",
		},

		// Ensure it works for the default production config.
		{
			RF:                 3,
			LiveIngesters:      3,
			ExpectedMaxFailure: 1,
		},

		{
			RF:                 3,
			LiveIngesters:      2,
			DeadIngesters:      1,
			ExpectedMaxFailure: 0,
		},

		{
			RF:            3,
			LiveIngesters: 1,
			DeadIngesters: 2,
			ExpectedError: "at least 2 live replicas required, could only find 1",
		},

		// Ensure it works when adding / removing nodes.

		// A node is joining or leaving, replica set expands.
		{
			RF:                 3,
			LiveIngesters:      4,
			ExpectedMaxFailure: 1,
		},

		{
			RF:                 3,
			LiveIngesters:      3,
			DeadIngesters:      1,
			ExpectedMaxFailure: 0,
		},

		{
			RF:            3,
			LiveIngesters: 2,
			DeadIngesters: 2,
			ExpectedError: "at least 3 live replicas required, could only find 2",
		},
	} {
		ingesters := []IngesterDesc{}
		for i := 0; i < tc.LiveIngesters; i++ {
			ingesters = append(ingesters, IngesterDesc{
				Timestamp: time.Now().Unix(),
			})
		}
		for i := 0; i < tc.DeadIngesters; i++ {
			ingesters = append(ingesters, IngesterDesc{})
		}
		r, err := New(Config{
			KVStore: kv.Config{
				Mock: consul.NewInMemoryClient(GetCodec()),
			},
			HeartbeatTimeout:  100 * time.Second,
			ReplicationFactor: tc.RF,
		}, "ingester", IngesterRingKey)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
		defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			liveIngesters, maxFailure, err := r.replicationStrategy(ingesters, tc.op)
			if tc.ExpectedError == "" {
				assert.NoError(t, err)
				assert.Equal(t, tc.LiveIngesters, len(liveIngesters))
				assert.Equal(t, tc.ExpectedMaxFailure, maxFailure)
			} else {
				assert.EqualError(t, err, tc.ExpectedError)
			}
		})
	}
}
