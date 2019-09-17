package local

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/local/archive"
	"github.com/cortexproject/cortex/pkg/chunk/local/archive/store"
	"github.com/cortexproject/cortex/pkg/chunk/local/archive/store/local"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

func queryTestBoltdb(t *testing.T, boltdbIndexClient *boltIndexClient, query chunk.IndexQuery) []string {
	resp := []string{}

	require.NoError(t, boltdbIndexClient.query(context.Background(), query, func(batch chunk.ReadBatch) (shouldContinue bool) {
		itr := batch.Iterator()
		for itr.Next() {
			resp = append(resp, string(itr.Value()))
		}
		return true
	}))

	return resp
}

func writeTestData(indexClient chunk.IndexClient, tableName string, startValue, count int) error {
	batch := indexClient.NewWriteBatch()
	for i := 0; i < count; i++ {
		batch.Add(tableName, "hash", []byte(fmt.Sprintf("value%d", startValue+i)), []byte(strconv.Itoa(startValue+i)))
	}

	return indexClient.BatchWrite(context.Background(), batch)

}

func buildTestBoltDBIndexClient(boltdbDir, archiveCacheLocation, ingesterName, archiverLocalStoreLocation string) (*boltIndexClient, error) {
	boltDBConfig := BoltDBConfig{
		Directory: boltdbDir,
	}

	if archiveCacheLocation != "" {
		boltDBConfig.EnableArchive = true
		archiveConfig := archive.Config{}

		flagext.DefaultValues(&archiveConfig)

		archiveConfig.StoreConfig = store.Config{
			Store: "local",
			LocalConfig: local.Config{
				Directory: archiverLocalStoreLocation,
			},
		}

		archiveConfig.CacheLocation = archiveCacheLocation
		archiveConfig.IngesterName = ingesterName
		boltDBConfig.ArchiveConfig = archiveConfig
	}

	indexClient, err := NewBoltDBIndexClient(boltDBConfig)
	if err != nil {
		return nil, err
	}

	return indexClient.(*boltIndexClient), nil
}

func TestBoltIndexClient_QueryWithArchiver(t *testing.T) {
	tempDirForTests, err := ioutil.TempDir("", "test-dir")
	require.NoError(t, err)

	archiverLocalStoreLocation := path.Join(tempDirForTests, "archiver-local-store")

	// Creating boltdb index client to add some test index
	boltDBIndexClient1, err := buildTestBoltDBIndexClient(path.Join(tempDirForTests, "boltdb-directory1"), path.Join(tempDirForTests, "cache1"),
		"test-boltdb-with-archiver", archiverLocalStoreLocation)
	require.NoError(t, err)

	// setting some test index with values from 0-4
	require.NoError(t, writeTestData(boltDBIndexClient1, "1", 0, 5))
	resp := queryTestBoltdb(t, boltDBIndexClient1, chunk.IndexQuery{
		TableName: "1",
		HashValue: "hash",
	})

	// verifying whether index is set properly
	require.Equal(t, 5, len(resp))
	require.Equal(t, []string{"0", "1", "2", "3", "4"}, resp)

	// stopping index client so that index files gets flushed to store
	boltDBIndexClient1.Stop()

	// creating another index client which uses same index store
	boltDBIndexClient2, err := buildTestBoltDBIndexClient(path.Join(tempDirForTests, "boltdb-directory2"), path.Join(tempDirForTests, "cache2"),
		"test-boltdb-with-archiver", archiverLocalStoreLocation)
	require.NoError(t, err)

	// setting new index with same table name using new index client
	require.NoError(t, writeTestData(boltDBIndexClient2, "1", 5, 5))
	resp = queryTestBoltdb(t, boltDBIndexClient2, chunk.IndexQuery{
		TableName: "1",
		HashValue: "hash",
	})

	// verifying whether new index client queried data from both local and archive index store
	require.Equal(t, 10, len(resp))
	require.Equal(t, []string{"5", "6", "7", "8", "9", "0", "1", "2", "3", "4"}, resp)

	// cleaning up
	boltDBIndexClient2.Stop()
	require.NoError(t, os.RemoveAll(tempDirForTests))
}
