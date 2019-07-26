package archive

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk/local/archive/store"
	chunk_util "github.com/cortexproject/cortex/pkg/chunk/util"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
)

const (
	// UpdateTypeFileDownloaded is for file downloaded update
	UpdateTypeFileDownloaded = iota
	// UpdateTypeFileRemoved is for file removed update
	UpdateTypeFileRemoved
	// UpdateTypeTableRemoved is for table removed update i.e all the files for a periodic table were removed
	UpdateTypeTableRemoved

	// ArchiveModeReadWrite is to allow both read and write
	ArchiveModeReadWrite = iota
	// ArchiveModeReadOnly is to allow only read operations
	ArchiveModeReadOnly
	// ArchiveModeWriteOnly is to allow only write operations
	ArchiveModeWriteOnly

	updateChanCapacity = 50
	// ArchiveFileInterval defines interval at which we keep pushing updated or new files
	ArchiveFileInterval            = 15 * time.Minute
	removeFilesMovedToArchiveAfter = 30 * time.Minute // We want to preserve newly moved files to archive for some duration to let other ingesters/queriers catch up
)

// Config is for configuring an archiver
type Config struct {
	ArchiveAfterCount int           `yaml:"archive_after_count"`
	CacheLocation     string        `yaml:"cache_location"`
	CacheTTL          time.Duration `yaml:"cache_ttl"`
	StoreConfig       store.Config  `yaml:"store_config"`
	ResyncInterval    time.Duration `yaml:"resync_interval"`
	IngesterName      string
	Mode              int
}

// RegisterFlags registers flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.ArchiveAfterCount, "boltdb.archive.archive-after-count", 1, "Archive boltDB files except N recent ones")
	f.StringVar(&cfg.CacheLocation, "boltdb.archive.cache-location", "", "Cache location for restoring boltDB files for queries")
	f.DurationVar(&cfg.CacheTTL, "boltdb.archive.cache-ttl", 24*time.Hour, "TTL for boltDB files restored in cache for queries")
	f.DurationVar(&cfg.ResyncInterval, "boltdb.archive.resync-interval", 5*time.Minute, "Resync downloaded files with the store")
}

// Update holds a single update to a file or folder
type Update struct {
	UpdateType int
	TableName  string
	FilePath   string
}

// Archiver holds its configuration and progress of objects in sync
type Archiver struct {
	cfg             Config
	boltDbDirectory string
	storeClient     store.ArchiveStoreClient

	downloadedTables    map[string]map[string]time.Time
	downloadedTablesMtx sync.RWMutex

	tablesInSync    map[string]struct{}
	tablesInSyncMtx sync.RWMutex

	updatesChan        chan Update
	filesArchivedAt    map[string]time.Time
	filesArchivedAtMtx sync.RWMutex
	done               chan struct{}
}

type file struct {
	name  string
	mtime time.Time
	path  string
}

// NewArchiver creates an archiver for syncing local objects with a store
func NewArchiver(cfg Config, boltDbDirectory string) (*Archiver, error) {
	if err := chunk_util.EnsureDirectory(cfg.CacheLocation); err != nil {
		return nil, err
	}

	archiver := Archiver{
		cfg:              cfg,
		boltDbDirectory:  boltDbDirectory,
		downloadedTables: map[string]map[string]time.Time{},
		tablesInSync:     map[string]struct{}{},
		updatesChan:      make(chan Update, updateChanCapacity),
		filesArchivedAt:  map[string]time.Time{},
	}

	var err error
	archiver.storeClient, err = store.NewArchiveStoreClient(cfg.StoreConfig)
	if err != nil {
		return nil, err
	}

	go archiver.loop()

	return &archiver, nil
}

func (a *Archiver) loop() {
	resyncTicker := time.NewTicker(a.cfg.ResyncInterval)
	defer resyncTicker.Stop()

	archiveFileTicker := time.NewTicker(ArchiveFileInterval)
	defer archiveFileTicker.Stop()

	for {
		select {
		case <-resyncTicker.C:
			err := a.syncAllTables(context.Background())
			if err != nil {
				level.Error(util.Logger).Log("msg", "error syncing archived boltdb files with store", "err", err)
			}
		case <-archiveFileTicker.C:
			err := a.archiveFiles(context.Background(), removeFilesMovedToArchiveAfter)
			if err != nil {
				level.Error(util.Logger).Log("msg", "error pushing archivable files to store", "err", err)
			}
		case <-a.done:
			return
		}
	}
}

// Stop the archiver and push all the local files to the store
func (a *Archiver) Stop() {
	close(a.done)

	// Push all boltdb files to archive before returning
	err := a.archiveFiles(context.Background(), time.Second)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error pushing archivable files to store", "err", err)
	}
}

func (a *Archiver) archiveFiles(ctx context.Context, removeFilesAfter time.Duration) error {
	if a.cfg.Mode == ArchiveModeReadOnly {
		return nil
	}

	files, err := a.listFiles(a.boltDbDirectory)
	if err != nil {
		return err
	}

	a.filesArchivedAtMtx.Lock()
	defer a.filesArchivedAtMtx.Unlock()

	for _, file := range files {
		// Checking whether file is updated after last push, if not skipping it
		fileArchivedAt, isOK := a.filesArchivedAt[file.path]
		if isOK && fileArchivedAt.After(file.mtime) {
			continue
		}

		f, err := os.Open(file.path)
		if err != nil {
			return err
		}

		// Files are stored with <periodic-table-name>/<ingester-name>
		objectName := fmt.Sprintf("%s/%s", file.name, a.cfg.IngesterName)
		err = a.storeClient.Put(ctx, objectName, f)
		if err != nil {
			return err
		}
		a.filesArchivedAt[file.path] = time.Now()
	}

	// Finguring out which files are supposed to be deleted based on mtime
	sort.Slice(files, func(i, j int) bool {
		return !files[i].mtime.Before(files[j].mtime)
	})

	time.AfterFunc(removeFilesAfter, func() {
		for _, file := range files[a.cfg.ArchiveAfterCount:] {
			err := os.Remove(file.path)
			if err != nil {
				level.Error(util.Logger).Log("msg", "error removing file moved to archive", "filepath", file.path, "err", err)
			}
		}
	})

	return nil
}

func (a *Archiver) listFiles(path string) ([]file, error) {
	filesInfo, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	files := []file{}
	for _, fileInfo := range filesInfo {
		if fileInfo.IsDir() {
			continue
		}
		files = append(files, file{fileInfo.Name(), fileInfo.ModTime(), filepath.Join(path, fileInfo.Name())})
	}

	return files, nil
}

// Sync all files for a table and keep pull new and updated files
func (a *Archiver) Sync(ctx context.Context, name string) ([]string, error) {
	if a.cfg.Mode == ArchiveModeWriteOnly {
		return nil, nil
	}

	a.downloadedTablesMtx.RLock()

	downloadedFiles, isOk := a.downloadedTables[name]
	if !isOk {
		a.downloadedTablesMtx.RUnlock()
		err := a.syncTable(ctx, name, false)
		if err != nil {
			return nil, err
		}

		// We need to keep this table synced i.e keep downloading all new/updated boltdb files from all the ingesters
		a.tablesInSyncMtx.Lock()
		defer a.tablesInSyncMtx.Unlock()
		a.tablesInSync[name] = struct{}{}

		a.downloadedTablesMtx.RLock()
		downloadedFiles = a.downloadedTables[name]
	}

	indexFiles := make([]string, 0, len(downloadedFiles))
	for downloadedFile := range downloadedFiles {
		indexFiles = append(indexFiles, path.Join(a.cfg.CacheLocation, name, downloadedFile))
	}
	a.downloadedTablesMtx.RUnlock()
	return indexFiles, nil
}

func (a *Archiver) syncAllTables(ctx context.Context) error {
	if a.cfg.Mode == ArchiveModeWriteOnly {
		return nil
	}

	a.tablesInSyncMtx.RLock()
	defer a.tablesInSyncMtx.RUnlock()

	for syncTable := range a.tablesInSync {
		err := a.syncTable(ctx, syncTable, true)
		if err != nil {
			return err
		}
	}

	return nil
}

// returns list of file paths which were newly download or were re-downloaded due to change in mtime
func (a *Archiver) syncTable(ctx context.Context, tableName string, sendUpdates bool) error {
	objectNamesWithMtime, err := a.storeClient.List(ctx, tableName)
	if err != nil {
		return err
	}

	indexDir := path.Join(a.cfg.CacheLocation, tableName)
	err = chunk_util.EnsureDirectory(indexDir)
	if err != nil {
		return err
	}

	_, isOk := a.downloadedTables[tableName]
	if !isOk {
		a.downloadedTables[tableName] = map[string]time.Time{}
	}

	for objectName, objectMtime := range objectNamesWithMtime {
		fileName := strings.Split(objectName, "/")[1]
		filePath := path.Join(indexDir, fileName)

		a.downloadedTablesMtx.RLock()
		isUptoDate := false

		// Checking whether file was updated in the store after we downloaded it, if not skipping download
		downloadedFileMtime, isOk := a.downloadedTables[tableName][fileName]
		if isOk && downloadedFileMtime == objectMtime {
			isUptoDate = true
		}

		a.downloadedTablesMtx.RUnlock()
		if isUptoDate {
			continue
		}

		fileBytes, err := a.storeClient.Get(ctx, objectName)
		if err != nil {
			return err
		}

		f, err := os.Create(filePath)
		if err != nil {
			return err
		}

		_, err = f.Write(fileBytes)
		if err != nil {
			return err
		}

		if sendUpdates {
			a.updatesChan <- Update{UpdateType: UpdateTypeFileDownloaded, TableName: tableName, FilePath: filePath}
		}

		a.downloadedTablesMtx.Lock()
		a.downloadedTables[tableName][fileName] = objectMtime
		a.downloadedTablesMtx.Unlock()
	}

	if sendUpdates {
		// Clean up removed files from the store.
		// We want to clean up removed files only when we are sending updates for taking appropriate action when files are removed
		// This should get handled when re-sync kicks in which sends the updates
		a.downloadedTablesMtx.Lock()
		defer a.downloadedTablesMtx.Unlock()

		downloadedTable := a.downloadedTables[tableName]
		for fileName := range downloadedTable {
			if _, isOK := objectNamesWithMtime[tableName+"/"+fileName]; !isOK {
				filePath := path.Join(indexDir, fileName)
				err := os.Remove(filePath)
				if err != nil {
					return err
				}
				a.updatesChan <- Update{UpdateType: UpdateTypeFileRemoved, TableName: tableName, FilePath: filePath}
			}
		}
	}

	return nil
}

func (a *Archiver) addSync(tableName string) {
	a.tablesInSyncMtx.Lock()
	defer a.tablesInSyncMtx.Unlock()

	a.tablesInSync[tableName] = struct{}{}
}

func (a *Archiver) removeSync(tableName string) {
	a.tablesInSyncMtx.Lock()
	defer a.tablesInSyncMtx.Unlock()

	delete(a.tablesInSync, tableName)
}

func (a *Archiver) removeDownloadedTable(tableName string) error {
	a.removeSync(tableName)
	a.downloadedTablesMtx.Lock()
	defer a.downloadedTablesMtx.Unlock()

	a.updatesChan <- Update{UpdateType: UpdateTypeTableRemoved, TableName: tableName}

	delete(a.downloadedTables, tableName)
	return os.RemoveAll(path.Join(a.cfg.CacheLocation, tableName))
}

// UpdatesChan returns a channel where async updates to files can be received
func (a *Archiver) UpdatesChan() <-chan Update {
	return a.updatesChan
}
