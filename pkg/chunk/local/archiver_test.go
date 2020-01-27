package local

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func createTestArchiver(t *testing.T, parentTempDir, ingesterName, localStoreLocation string) *Archiver {
	cacheLocation, err := ioutil.TempDir(parentTempDir, "cache")
	require.NoError(t, err)

	boltdbFilesLocation, err := ioutil.TempDir(parentTempDir, "boltdb-files")
	require.NoError(t, err)

	archiverConfig := ArchiverConfig{
		CacheLocation: cacheLocation,
		CacheTTL:      1 * time.Hour,
		StoreConfig: StoreConfig{
			Store: "local",
			FSConfig: FSConfig{
				Directory: localStoreLocation,
			},
		},
		ResyncInterval: 1 * time.Hour,
		IngesterName:   ingesterName,
		Mode:           ArchiverModeReadWrite,
	}

	archiveStoreClient, err := NewFSObjectClient(archiverConfig.StoreConfig.FSConfig)
	require.NoError(t, err)

	archiver, err := NewArchiver(archiverConfig, boltdbFilesLocation, archiveStoreClient)
	require.NoError(t, err)
	return archiver
}

func TestArchiver_Sync(t *testing.T) {
	tempDirForTests, err := ioutil.TempDir("", "test-dir")
	require.NoError(t, err)

	localStoreLocation, err := ioutil.TempDir(tempDirForTests, "local-store")
	require.NoError(t, err)

	ingesterNameForArchiver1 := "test-archive1"
	ingesterNameForArchiver2 := "test-archive2"
	archiver1 := createTestArchiver(t, tempDirForTests, ingesterNameForArchiver1, localStoreLocation)
	archiver2 := createTestArchiver(t, tempDirForTests, ingesterNameForArchiver2, localStoreLocation)

	// Create a file for archiver1 to test sync
	// Since Archiver does not do boltdb operations using a text file to simplify tests
	table1Name := "table1"
	table1File, err := os.Create(filepath.Join(archiver1.boltDbDirectory, table1Name))
	require.NoError(t, err)

	// Checking whether archiver1 has the above file
	files, err := archiver1.listFiles(archiver1.boltDbDirectory)
	require.NoError(t, err)
	require.Equal(t, 1, len(files))
	require.Equal(t, table1Name, files[0].name)

	// Archive files from archiver1
	require.NoError(t, archiver1.archiveFiles(context.Background()))

	// Sync files from archiver2
	syncedFiles, err := archiver2.Sync(context.Background(), table1Name)
	require.NoError(t, err)

	// Check whether archiver2 started syncing table1
	tablesInSync := archiver2.listDownloadedTables()
	require.Equal(t, []string{table1Name}, tablesInSync)

	// Check whether file created above got synced in archiver2
	require.Equal(t, 1, len(syncedFiles))
	syncedFileForTable1Path := filepath.Join(archiver2.cfg.CacheLocation, table1Name, archiver1.ingesterNameAndStartUpTs)
	require.Equal(t, []string{syncedFileForTable1Path}, syncedFiles)

	// Making changes to file created by archiver1
	time.Sleep(5 * time.Millisecond)
	_, err = table1File.WriteString("test write")
	require.NoError(t, err)

	// Archiving files again from archiver1 to push the updates
	require.NoError(t, archiver1.archiveFiles(context.Background()))

	// Syncing files from archiver2
	require.NoError(t, archiver2.syncTable(context.Background(), table1Name, false))

	// Verifying contents of file
	testFile1Content, err := ioutil.ReadFile(syncedFileForTable1Path)
	require.NoError(t, err)
	require.Equal(t, "test write", string(testFile1Content))

	// Cleanup
	require.NoError(t, table1File.Close())
	require.NoError(t, os.RemoveAll(tempDirForTests))
	archiver1.Stop()
	archiver2.Stop()
}
