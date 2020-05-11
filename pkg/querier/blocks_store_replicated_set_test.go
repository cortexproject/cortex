package querier

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storegateway"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/tls"
)

func Test_findSmallestInstanceSet(t *testing.T) {
	tests := map[string]struct {
		input    []ring.ReplicationSet
		expected []string
	}{
		"empty replication set": {
			input:    nil,
			expected: nil,
		},
		"single replication set": {
			input: []ring.ReplicationSet{
				{Ingesters: []ring.IngesterDesc{{Addr: "instance-1"}, {Addr: "instance-2"}}},
			},
			expected: []string{"instance-1"},
		},
		"no replication": {
			input: []ring.ReplicationSet{
				{Ingesters: []ring.IngesterDesc{{Addr: "instance-1"}}},
				{Ingesters: []ring.IngesterDesc{{Addr: "instance-3"}}},
				{Ingesters: []ring.IngesterDesc{{Addr: "instance-1"}}},
			},
			expected: []string{"instance-1", "instance-3"},
		},
		"all replication sets share the same instance": {
			input: []ring.ReplicationSet{
				{Ingesters: []ring.IngesterDesc{{Addr: "instance-1"}, {Addr: "instance-2"}}},
				{Ingesters: []ring.IngesterDesc{{Addr: "instance-3"}, {Addr: "instance-2"}}},
				{Ingesters: []ring.IngesterDesc{{Addr: "instance-4"}, {Addr: "instance-2"}}},
			},
			expected: []string{"instance-2"},
		},
		"all replication sets share the same instance except one": {
			input: []ring.ReplicationSet{
				{Ingesters: []ring.IngesterDesc{{Addr: "instance-1"}, {Addr: "instance-2"}}},
				{Ingesters: []ring.IngesterDesc{{Addr: "instance-3"}, {Addr: "instance-2"}}},
				{Ingesters: []ring.IngesterDesc{{Addr: "instance-4"}, {Addr: "instance-2"}}},
				{Ingesters: []ring.IngesterDesc{{Addr: "instance-5"}, {Addr: "instance-1"}}},
			},
			expected: []string{"instance-2", "instance-5"},
		},
		"few replication sets share the same instance": {
			input: []ring.ReplicationSet{
				{Ingesters: []ring.IngesterDesc{{Addr: "instance-1"}, {Addr: "instance-2"}}},
				{Ingesters: []ring.IngesterDesc{{Addr: "instance-3"}, {Addr: "instance-4"}}},
				{Ingesters: []ring.IngesterDesc{{Addr: "instance-1"}, {Addr: "instance-5"}}},
				{Ingesters: []ring.IngesterDesc{{Addr: "instance-4"}, {Addr: "instance-6"}}},
			},
			expected: []string{"instance-1", "instance-4"},
		},
		"no replication set share the same instance": {
			input: []ring.ReplicationSet{
				{Ingesters: []ring.IngesterDesc{{Addr: "instance-1"}, {Addr: "instance-2"}}},
				{Ingesters: []ring.IngesterDesc{{Addr: "instance-3"}, {Addr: "instance-4"}}},
				{Ingesters: []ring.IngesterDesc{{Addr: "instance-5"}, {Addr: "instance-6"}}},
			},
			expected: []string{"instance-1", "instance-3", "instance-5"},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, findSmallestInstanceSet(testData.input))
		})
	}
}

func TestBlocksStoreReplicationSet_GetClientsFor(t *testing.T) {
	// The following block IDs have been picked to have increasing hash values
	// in order to simplify the tests.
	block1 := ulid.MustNew(1, nil) // hash: 283204220
	block2 := ulid.MustNew(2, nil) // hash: 444110359
	block3 := ulid.MustNew(5, nil) // hash: 2931974232
	block4 := ulid.MustNew(6, nil) // hash: 3092880371

	block1Hash := cortex_tsdb.HashBlockID(block1)
	block2Hash := cortex_tsdb.HashBlockID(block2)
	block3Hash := cortex_tsdb.HashBlockID(block3)
	block4Hash := cortex_tsdb.HashBlockID(block4)

	tests := map[string]struct {
		replicationFactor int
		setup             func(*ring.Desc)
		queryBlocks       []ulid.ULID
		expectedClients   []string
	}{
		"single instance in the ring with replication factor = 1": {
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE)
			},
			queryBlocks:     []ulid.ULID{block1, block2},
			expectedClients: []string{"127.0.0.1"},
		},
		"single instance in the ring with replication factor = 2": {
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE)
			},
			queryBlocks:     []ulid.ULID{block1, block2},
			expectedClients: []string{"127.0.0.1"},
		},
		"multiple instances in the ring with replication factor = 1": {
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE)
			},
			queryBlocks:     []ulid.ULID{block1, block3, block4},
			expectedClients: []string{"127.0.0.1", "127.0.0.3", "127.0.0.4"},
		},
		"multiple instances in the ring with replication factor = 2": {
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE)
			},
			queryBlocks:     []ulid.ULID{block1, block3, block4},
			expectedClients: []string{"127.0.0.1", "127.0.0.4" /* block4 is also replicated by instance-4 */},
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			// Setup the ring state.
			ringStore := consul.NewInMemoryClient(ring.GetCodec())
			require.NoError(t, ringStore.CAS(ctx, "test", func(in interface{}) (interface{}, bool, error) {
				d := ring.NewDesc()
				testData.setup(d)
				return d, true, nil
			}))

			ringCfg := ring.Config{}
			flagext.DefaultValues(&ringCfg)
			ringCfg.ReplicationFactor = testData.replicationFactor

			r, err := ring.NewWithStoreClientAndStrategy(ringCfg, "test", "test", ringStore, &storegateway.BlocksReplicationStrategy{})
			require.NoError(t, err)

			reg := prometheus.NewPedanticRegistry()
			s, err := newBlocksStoreReplicationSet(r, tls.ClientConfig{}, log.NewNopLogger(), reg)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, s))
			defer services.StopAndAwaitTerminated(ctx, s) //nolint:errcheck

			// Wait until the ring client has initialised the state.
			test.Poll(t, time.Second, true, func() interface{} {
				all, err := r.GetAll(ring.Read)
				return err == nil && len(all.Ingesters) > 0
			})

			clients, err := s.GetClientsFor(testData.queryBlocks)
			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expectedClients, getStoreGatewayClientAddrs(clients))

			assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
				# HELP cortex_storegateway_clients The current number of store-gateway clients in the pool.
				# TYPE cortex_storegateway_clients gauge
				cortex_storegateway_clients{client="querier"} %d
			`, len(testData.expectedClients))), "cortex_storegateway_clients"))
		})
	}
}

func getStoreGatewayClientAddrs(clients []BlocksStoreClient) []string {
	var addrs []string
	for _, c := range clients {
		addrs = append(addrs, c.RemoteAddress())
	}
	return addrs
}
