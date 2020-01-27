package local

import (
	"bytes"
	"context"
	"flag"
	"os"
	"path"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"go.etcd.io/bbolt"

	"github.com/cortexproject/cortex/pkg/chunk"
	chunk_util "github.com/cortexproject/cortex/pkg/chunk/util"
	"github.com/cortexproject/cortex/pkg/util"
)

var bucketName = []byte("index")

const (
	separator      = "\000"
	dbReloadPeriod = 10 * time.Minute

	dbOperationRead = iota
	dbOperationWrite
)

type archivedDB struct {
	boltDBs map[string]*bbolt.DB
	sync.RWMutex
}

// BoltDBConfig for a BoltDB index client.
type BoltDBConfig struct {
	Directory      string         `yaml:"directory"`
	EnableArchive  bool           `yaml:"enable_archive"`
	ArchiverConfig ArchiverConfig `yaml:"archiver_config"`
}

// RegisterFlags registers flags.
func (cfg *BoltDBConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.ArchiverConfig.RegisterFlags(f)

	f.StringVar(&cfg.Directory, "boltdb.dir", "", "Location of BoltDB index files.")
	f.BoolVar(&cfg.EnableArchive, "boltdb.enable-archive", false, "Enable archival of files to a store")
}

type boltIndexClient struct {
	cfg      BoltDBConfig
	archiver *Archiver

	dbsMtx sync.RWMutex
	dbs    map[string]*bbolt.DB
	done   chan struct{}
	wait   sync.WaitGroup

	archivedDbsMtx sync.RWMutex
	archivedDbs    map[string]*archivedDB
}

// NewBoltDBIndexClient creates a new IndexClient that used BoltDB.
func NewBoltDBIndexClient(cfg BoltDBConfig, archiver *Archiver) (chunk.IndexClient, error) {
	if err := chunk_util.EnsureDirectory(cfg.Directory); err != nil {
		return nil, err
	}

	indexClient := &boltIndexClient{
		cfg:         cfg,
		dbs:         map[string]*bbolt.DB{},
		done:        make(chan struct{}),
		archivedDbs: map[string]*archivedDB{},
	}

	if archiver != nil {
		indexClient.archiver = archiver
		go indexClient.processArchiverUpdates()
	}

	indexClient.wait.Add(1)
	go indexClient.loop()
	return indexClient, nil
}

func (b *boltIndexClient) loop() {
	defer b.wait.Done()

	ticker := time.NewTicker(dbReloadPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.reload()
		case <-b.done:
			return
		}
	}
}

func (b *boltIndexClient) processArchiverUpdates() {
	updatesChan := b.archiver.UpdatesChan()
	for {
		select {
		case update := <-updatesChan:
			switch update.UpdateType {
			case UpdateTypeFileRemoved:
				err := b.processArchiverFileRemovedUpdate(context.Background(), update.TableName, update.FilePath)
				if err != nil {
					level.Error(util.Logger).Log("msg", "failed to process file delete update from archiver", "update", update, "err", err)
				}
			case UpdateTypeFileDownloaded:
				err := b.processArchiverFileDownloadedUpdate(context.Background(), update.TableName, update.FilePath)
				if err != nil {
					level.Error(util.Logger).Log("msg", "failed to process file added update from archiver", "update", update, "err", err)
				}
			case UpdateTypeTableRemoved:
				err := b.processArchiverTableDeletedUpdate(update.TableName)
				if err != nil {
					level.Error(util.Logger).Log("msg", "failed to process table deleted update from archiver", "update", update, "err", err)
				}
			default:
				level.Error(util.Logger).Log("Invalid update type from archiver", "update", update)
			}
		case <-b.done:
			return
		}
	}
}

func (b *boltIndexClient) processArchiverFileRemovedUpdate(ctx context.Context, tableName, filePath string) error {
	aDB, err := b.getArchivedDB(ctx, tableName)
	if err != nil {
		return err
	}

	aDB.Lock()
	defer aDB.Unlock()

	boltDB, isOK := aDB.boltDBs[filePath]
	if !isOK {
		return nil
	}

	delete(aDB.boltDBs, filePath)

	if err := boltDB.Close(); err != nil {
		return err
	}

	return nil
}

func (b *boltIndexClient) processArchiverFileDownloadedUpdate(ctx context.Context, tableName, filePath string) error {
	aDB, err := b.getArchivedDB(ctx, tableName)
	if err != nil {
		return err
	}

	aDB.Lock()
	defer aDB.Unlock()
	_, isOK := aDB.boltDBs[filePath]
	if isOK {
		return nil
	}

	boltDB, err := openBoltdbFile(filePath)
	if err != nil {
		return err
	}

	aDB.boltDBs[filePath] = boltDB
	return nil
}

func (b *boltIndexClient) processArchiverTableDeletedUpdate(tableName string) error {
	b.archivedDbsMtx.Lock()
	defer b.archivedDbsMtx.Unlock()

	aDB, isOK := b.archivedDbs[tableName]
	if !isOK {
		return nil
	}

	aDB.Lock()
	defer aDB.Unlock()

	for _, boltDB := range aDB.boltDBs {
		err := boltDB.Close()
		if err != nil {
			level.Error(util.Logger).Log("msg", "failed to close removed boltdb", "filepath", boltDB.Path(), "err", err)
		}
	}

	delete(b.archivedDbs, tableName)
	return nil
}

func (b *boltIndexClient) reload() {
	b.dbsMtx.RLock()

	removedDBs := []string{}
	for name := range b.dbs {
		if _, err := os.Stat(path.Join(b.cfg.Directory, name)); err != nil && os.IsNotExist(err) {
			removedDBs = append(removedDBs, name)
			level.Debug(util.Logger).Log("msg", "boltdb file got removed", "filename", name)
			continue
		}
	}
	b.dbsMtx.RUnlock()

	if len(removedDBs) != 0 {
		b.dbsMtx.Lock()
		defer b.dbsMtx.Unlock()

		for _, name := range removedDBs {
			if err := b.dbs[name].Close(); err != nil {
				level.Error(util.Logger).Log("msg", "failed to close removed boltdb", "filename", name, "err", err)
				continue
			}
			delete(b.dbs, name)
		}
	}

}

func (b *boltIndexClient) Stop() {
	close(b.done)

	b.dbsMtx.Lock()
	defer b.dbsMtx.Unlock()
	for _, db := range b.dbs {
		db.Close()
	}

	if b.archiver != nil {
		b.archiver.Stop()
	}
	b.wait.Wait()
}

func (b *boltIndexClient) NewWriteBatch() chunk.WriteBatch {
	return &boltWriteBatch{
		tables: map[string]map[string][]byte{},
	}
}

func (b *boltIndexClient) getDB(name string, operation int) (*bbolt.DB, error) {
	b.dbsMtx.RLock()
	db, ok := b.dbs[name]
	b.dbsMtx.RUnlock()
	if ok {
		return db, nil
	}

	if _, err := os.Stat(path.Join(b.cfg.Directory, name)); err == nil || operation == dbOperationWrite {
		b.dbsMtx.Lock()
		defer b.dbsMtx.Unlock()
		db, ok = b.dbs[name]
		if ok {
			return db, nil
		}

		// Open the database.
		// Set Timeout to avoid obtaining file lock wait indefinitely.
		db, err := openBoltdbFile(path.Join(b.cfg.Directory, name))
		if err != nil {
			return nil, err
		}

		b.dbs[name] = db
		return db, nil
	}

	return nil, nil
}

func (b *boltIndexClient) getArchivedDB(ctx context.Context, name string) (*archivedDB, error) {
	b.archivedDbsMtx.RLock()
	aDB, ok := b.archivedDbs[name]
	b.archivedDbsMtx.RUnlock()
	if ok {
		return aDB, nil
	}

	b.archivedDbsMtx.Lock()
	defer b.archivedDbsMtx.Unlock()

	aDB, ok = b.archivedDbs[name]
	if ok {
		return aDB, nil
	}

	archivedFilePaths, err := b.archiver.Sync(ctx, name)
	if err != nil {
		return nil, err
	}

	aDB = &archivedDB{}

	boltDBs := make(map[string]*bbolt.DB, len(archivedFilePaths))
	for _, archivedFilePath := range archivedFilePaths {
		db, err := openBoltdbFile(archivedFilePath)
		if err != nil {
			return nil, err
		}

		boltDBs[archivedFilePath] = db
	}
	aDB.boltDBs = boltDBs
	b.archivedDbs[name] = aDB

	return aDB, nil
}

func (b *boltIndexClient) BatchWrite(ctx context.Context, batch chunk.WriteBatch) error {
	for table, kvps := range batch.(*boltWriteBatch).tables {
		db, err := b.getDB(table, dbOperationWrite)
		if err != nil {
			return err
		}

		if err := db.Update(func(tx *bbolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists(bucketName)
			if err != nil {
				return err
			}

			for key, value := range kvps {
				if err := b.Put([]byte(key), value); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (b *boltIndexClient) QueryPages(ctx context.Context, queries []chunk.IndexQuery, callback func(chunk.IndexQuery, chunk.ReadBatch) (shouldContinue bool)) error {
	return chunk_util.DoParallelQueries(ctx, b.query, queries, callback)
}

func (b *boltIndexClient) query(ctx context.Context, query chunk.IndexQuery, callback func(chunk.ReadBatch) (shouldContinue bool)) error {
	localDB, err := b.getDB(query.TableName, dbOperationRead)
	if err != nil {
		return err
	}

	var aDB *archivedDB
	if b.archiver != nil {
		aDB, err = b.getArchivedDB(ctx, query.TableName)
		if err != nil {
			return err
		}
		aDB.RLock()
		defer aDB.RUnlock()
	}

	var start []byte
	if len(query.RangeValuePrefix) > 0 {
		start = []byte(query.HashValue + separator + string(query.RangeValuePrefix))
	} else if len(query.RangeValueStart) > 0 {
		start = []byte(query.HashValue + separator + string(query.RangeValueStart))
	} else {
		start = []byte(query.HashValue + separator)
	}

	rowPrefix := []byte(query.HashValue + separator)

	viewFunc := func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return nil
		}

		var batch boltReadBatch
		c := b.Cursor()
		for k, v := c.Seek(start); k != nil; k, v = c.Next() {
			if len(query.ValueEqual) > 0 && !bytes.Equal(v, query.ValueEqual) {
				continue
			}

			if len(query.RangeValuePrefix) > 0 && !bytes.HasPrefix(k, start) {
				break
			}

			if !bytes.HasPrefix(k, rowPrefix) {
				break
			}

			batch.rangeValue = k[len(rowPrefix):]
			batch.value = v
			if !callback(&batch) {
				break
			}
		}

		return nil
	}

	if localDB != nil {
		err = localDB.View(viewFunc)
		if err != nil {
			return err
		}
	}

	if aDB != nil {
		for _, db := range aDB.boltDBs {
			err := db.View(viewFunc)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type boltWriteBatch struct {
	tables map[string]map[string][]byte
}

func (b *boltWriteBatch) Add(tableName, hashValue string, rangeValue []byte, value []byte) {
	table, ok := b.tables[tableName]
	if !ok {
		table = map[string][]byte{}
		b.tables[tableName] = table
	}

	key := hashValue + separator + string(rangeValue)
	table[key] = value
}

type boltReadBatch struct {
	rangeValue []byte
	value      []byte
}

func (b boltReadBatch) Iterator() chunk.ReadBatchIterator {
	return &boltReadBatchIterator{
		boltReadBatch: b,
	}
}

type boltReadBatchIterator struct {
	consumed bool
	boltReadBatch
}

func (b *boltReadBatchIterator) Next() bool {
	if b.consumed {
		return false
	}
	b.consumed = true
	return true
}

func (b *boltReadBatchIterator) RangeValue() []byte {
	return b.rangeValue
}

func (b *boltReadBatchIterator) Value() []byte {
	return b.value
}

func openBoltdbFile(path string) (*bbolt.DB, error) {
	return bbolt.Open(path, 0666, &bbolt.Options{Timeout: 5 * time.Second})
}
