package clients

import (
	"testing"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/configs/storage/clients/gcp"
	"github.com/cortexproject/cortex/pkg/configs/storage/clients/client"
	"github.com/cortexproject/cortex/pkg/chunk/testutils"
	"github.com/stretchr/testify/require"
)

const (
	userID    = "userID"
	tableName = "test"
)

type configClientTest func(*testing.T, configs.AlertStore, configs.RuleStore)

func forAllFixtures(t *testing.T, storageClientTest storageClientTest) {
	var fixtures []testutils.Fixture
	fixtures = append(fixtures, client.Fixtures...)
	fixtures = append(fixtures, gcp.Fixtures...)

	cassandraFixtures, err := cassandra.Fixtures()
	require.NoError(t, err)
	fixtures = append(fixtures, cassandraFixtures...)

	for _, fixture := range fixtures {
		t.Run(fixture.Name(), func(t *testing.T) {
			indexClient, objectClient, err := testutils.Setup(fixture, tableName)
			require.NoError(t, err)
			defer fixture.Teardown()

			storageClientTest(t, indexClient, objectClient)
		})
	}
}
