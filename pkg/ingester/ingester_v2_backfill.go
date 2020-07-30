package ingester

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/gate"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
)

// backfillAppender is an appender to ingest old data.
// This _does not_ implement storage.Appender interface.
// The methods of this appender should not be called concurrently.
type backfillAppender struct {
	userID    string
	ingester  *Ingester
	buckets   *tsdbBuckets
	appenders map[int]storage.Appender
}

func (i *Ingester) newBackfillAppender(userID string) *backfillAppender {
	buckets := i.TSDBState.backfillDBs.getBucketsForUser(userID)
	if buckets == nil {
		buckets = i.TSDBState.backfillDBs.getOrCreateNewUser(userID)
	}
	return &backfillAppender{
		userID:    userID,
		ingester:  i,
		buckets:   buckets,
		appenders: make(map[int]storage.Appender),
	}
}

func (a *backfillAppender) add(la []client.LabelAdapter, s client.Sample) (err error) {
	a.buckets.RLock()
	bucket := getBucketForTimestamp(s.TimestampMs, a.buckets.buckets)
	a.buckets.RUnlock()
	if bucket == nil {
		bucket, err = a.ingester.getOrCreateBackfillTSDB(a.buckets, a.userID, s.TimestampMs)
		if err != nil {
			return err
		}
	}

	db := bucket.db
	var app storage.Appender
	if ap, ok := a.appenders[bucket.id]; ok {
		app = ap
	} else {
		app = db.Appender()
		a.appenders[bucket.id] = app
	}

	startAppend := time.Now()
	cachedRef, cachedRefExists := db.refCache.Ref(startAppend, client.FromLabelAdaptersToLabels(la))
	// If the cached reference exists, we try to use it.
	if cachedRefExists {
		err = app.AddFast(cachedRef, s.TimestampMs, s.Value)
		if err != nil && errors.Cause(err) == storage.ErrNotFound {
			cachedRefExists = false
			err = nil
		}
	}

	// If the cached reference doesn't exist, we (re)try without using the reference.
	if !cachedRefExists {
		// Copy the label set because both TSDB and the cache may retain it.
		copiedLabels := client.FromLabelAdaptersToLabelsWithCopy(la)
		if ref, err := app.Add(copiedLabels, s.TimestampMs, s.Value); err == nil {
			db.refCache.SetRef(startAppend, copiedLabels, ref)
		}
	}

	return err
}

func (a *backfillAppender) commit() error {
	var merr tsdb_errors.MultiError
	for _, app := range a.appenders {
		merr.Add(app.Commit())
	}
	return merr.Err()
}

func (a *backfillAppender) rollback() error {
	var merr tsdb_errors.MultiError
	for _, app := range a.appenders {
		merr.Add(app.Rollback())
	}
	return merr.Err()
}

func getBucketForTimestamp(ts int64, userBuckets []*tsdbBucket) *tsdbBucket {
	// As the number of buckets will be small, we are iterating instead of binary search.
	for _, b := range userBuckets {
		if ts >= b.bucketStart && ts < b.bucketEnd {
			return b
		}
	}
	return nil
}

func (i *Ingester) getOrCreateBackfillTSDB(userBuckets *tsdbBuckets, userID string, ts int64) (*tsdbBucket, error) {
	start, end := getBucketRangesForTimestamp(ts, 1)

	userBuckets.RLock()
	for _, b := range userBuckets.buckets {
		if ts >= b.bucketStart && ts < b.bucketEnd {
			userBuckets.RUnlock()
			return b, nil
		}

		//   Existing: |-----------|
		//        New:       |------------|
		// Changed to:             |------| (no overlaps)
		if b.bucketStart < start && start < b.bucketEnd {
			start = b.bucketEnd
		}

		//   Existing:         |-----------|
		//        New:  |------------|
		// Changed to:  |------| (no overlaps)
		if end > b.bucketStart && end < b.bucketEnd {
			end = b.bucketStart
			break
		}

		if b.bucketStart > end {
			break
		}
	}
	userBuckets.RUnlock()

	// No bucket exists for this timestamp, create one.
	userBuckets.Lock()
	defer userBuckets.Unlock()
	// Check again if another goroutine created a bucket for this timestamp between unlocking and locking..
	for _, b := range userBuckets.buckets {
		if ts >= b.bucketStart && ts < b.bucketEnd {
			return b, nil
		}
	}

	db, err := i.createNewTSDB(
		userID, filepath.Join(i.cfg.BlocksStorageConfig.TSDB.BackfillDir, userID, getBucketName(start, end)),
		(end-start)*2, (end-start)*2, prometheus.NewRegistry(),
	)
	if err != nil {
		return nil, err
	}
	bucket := &tsdbBucket{
		db:          db,
		bucketStart: start,
		bucketEnd:   end,
	}
	if len(userBuckets.buckets) > 0 {
		bucket.id = userBuckets.buckets[len(userBuckets.buckets)-1].id + 1
	}
	userBuckets.buckets = append(userBuckets.buckets, bucket)
	sort.Slice(userBuckets.buckets, func(i, j int) bool {
		return userBuckets.buckets[i].bucketStart < userBuckets.buckets[j].bucketStart
	})

	return bucket, nil
}

func (i *Ingester) openExistingBackfillTSDB(ctx context.Context) error {
	level.Info(util.Logger).Log("msg", "opening existing TSDBs")
	wg := &sync.WaitGroup{}
	openGate := gate.New(i.cfg.BlocksStorageConfig.TSDB.MaxTSDBOpeningConcurrencyOnStartup)

	users, err := ioutil.ReadDir(i.cfg.BlocksStorageConfig.TSDB.BackfillDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var runErr error
	for _, u := range users {
		if !u.IsDir() {
			continue
		}

		userID := u.Name()
		userPath := filepath.Join(i.cfg.BlocksStorageConfig.TSDB.BackfillDir, userID)

		bucketNames, err := ioutil.ReadDir(userPath)
		if err != nil {
			level.Error(util.Logger).Log("msg", "unable to open user TSDB dir for backfill", "err", err, "user", u, "path", userPath)
			continue
		}

		for bucketID, bucketName := range bucketNames {
			if !bucketName.IsDir() {
				continue
			}

			dbPath := filepath.Join(userPath, bucketName.Name())
			f, err := os.Open(dbPath)
			if err != nil {
				level.Error(util.Logger).Log("msg", "unable to open user backfill TSDB dir", "err", err, "user", userID, "path", dbPath)
				return filepath.SkipDir
			}
			defer f.Close()

			// If the dir is empty skip it
			if _, err := f.Readdirnames(1); err != nil {
				if err != io.EOF {
					level.Error(util.Logger).Log("msg", "unable to read backfill TSDB dir", "err", err, "user", userID, "path", dbPath)
				}

				return filepath.SkipDir
			}

			// Limit the number of TSDB's opening concurrently. Start blocks until there's a free spot available or the context is cancelled.
			if err := openGate.Start(ctx); err != nil {
				runErr = err
				break
			}

			wg.Add(1)
			go func(bucketID int, userID, bucketName, dbDir string) {
				defer wg.Done()
				defer openGate.Done()
				defer func(ts time.Time) {
					i.TSDBState.walReplayTime.Observe(time.Since(ts).Seconds())
				}(time.Now())

				start, end, err := getBucketRangesForBucketName(bucketName)
				if err != nil {
					level.Error(util.Logger).Log("msg", "unable to get bucket range", "err", err, "user", userID, "bucketName", bucketName)
					return
				}
				db, err := i.createNewTSDB(userID, dbDir, (end-start)*2, (end-start)*2, prometheus.NewRegistry())
				if err != nil {
					level.Error(util.Logger).Log("msg", "unable to open user backfill TSDB", "err", err, "user", userID)
					return
				}

				bucket := &tsdbBucket{
					id:          bucketID,
					db:          db,
					bucketStart: start,
					bucketEnd:   end,
				}

				buckets := i.TSDBState.backfillDBs.getOrCreateNewUser(userID)
				buckets.Lock()
				// We will sort it at the end.
				buckets.buckets = append(buckets.buckets, bucket)
				buckets.Unlock()
			}(bucketID, userID, bucketName.Name(), dbPath)
		}

		if runErr != nil {
			break
		}

	}

	// Wait for all opening routines to finish
	wg.Wait()

	// Sort the buckets within the users.
	i.TSDBState.backfillDBs.tsdbsMtx.Lock()
	for _, buckets := range i.TSDBState.backfillDBs.tsdbs {
		buckets.Lock()
		sort.Slice(buckets.buckets, func(i, j int) bool {
			return buckets.buckets[i].bucketStart < buckets.buckets[j].bucketStart
		})
		buckets.Unlock()
	}
	i.TSDBState.backfillDBs.tsdbsMtx.Unlock()

	return runErr
}

func (i *Ingester) backfillSelect(ctx context.Context, userID string, from, through int64, matchers []*labels.Matcher) ([]storage.SeriesSet, error) {
	buckets := i.TSDBState.backfillDBs.getBucketsForUser(userID)
	if buckets == nil {
		return nil, nil
	}
	var queriers []storage.Querier
	defer func() {
		for _, q := range queriers {
			q.Close()
		}
	}()
	buckets.RLock()
	for _, b := range buckets.buckets {
		if !b.overlaps(from, through) {
			mint, err := b.db.DB.StartTime()
			if err != nil {
				buckets.RUnlock()
				return nil, err
			}
			maxt := b.db.Head().MaxTime()
			if !overlapsOpenInterval(mint, maxt, from, through) {
				continue
			}
		}

		q, err := b.db.Querier(ctx, from, through)
		if err != nil {
			buckets.RUnlock()
			return nil, err
		}

		queriers = append(queriers, q)
	}
	buckets.RUnlock()

	if len(queriers) == 0 {
		return nil, nil
	}

	result := make([]storage.SeriesSet, len(queriers))
	errC := make(chan error, 1)
	var wg sync.WaitGroup
	for i, q := range queriers {
		wg.Add(1)
		go func(i int, q storage.Querier) {
			defer wg.Done()

			ss := q.Select(false, nil, matchers...)
			if ss.Err() != nil {
				select {
				case errC <- ss.Err():
				default:
				}
			}
			result[i] = ss
		}(i, q)
	}

	wg.Wait()
	select {
	case err := <-errC:
		return nil, err
	default:
	}

	return result, nil
}

func (i *Ingester) closeAllBackfillTSDBs() {
	// Snapshotting of in-memory chunks can be considered as a small compaction, hence
	// using that concurrency.
	i.runConcurrentBackfillWorkers(context.Background(), i.cfg.BlocksStorageConfig.TSDB.HeadCompactionConcurrency, func(db *userTSDB) {
		if err := db.Close(); err != nil {
			level.Warn(util.Logger).Log("msg", "unable to close backfill TSDB", "user", db.userID, "bucket_dir", db.Dir(), "err", err)
		}
	})
}

func (i *Ingester) compactAllBackfillTSDBs(ctx context.Context) {
	i.runConcurrentBackfillWorkers(ctx, i.cfg.BlocksStorageConfig.TSDB.ShipConcurrency, func(db *userTSDB) {
		h := db.Head()
		if err := db.CompactHead(tsdb.NewRangeHead(h, h.MinTime(), h.MaxTime())); err != nil {
			level.Error(util.Logger).Log("msg", "unable to compact backfill TSDB", "user", db.userID, "bucket_dir", db.Dir(), "err", err)
		}
	})
}

func (i *Ingester) shipAllBackfillTSDBs(ctx context.Context) {
	i.runConcurrentBackfillWorkers(ctx, i.cfg.BlocksStorageConfig.TSDB.ShipConcurrency, func(db *userTSDB) {
		if db.shipper == nil {
			return
		}
		if uploaded, err := db.shipper.Sync(context.Background()); err != nil {
			level.Warn(util.Logger).Log("msg", "shipper failed to synchronize backfill TSDB blocks with the storage", "user", db.userID, "uploaded", uploaded, "bucket_dir", db.Dir(), "err", err)
		} else {
			level.Debug(util.Logger).Log("msg", "shipper successfully synchronized backfill TSDB blocks with storage", "user", db.userID, "uploaded", uploaded, "bucket_dir", db.Dir())
		}
	})
}

func (i *Ingester) runConcurrentBackfillWorkers(ctx context.Context, concurrency int, userFunc func(*userTSDB)) {
	i.TSDBState.backfillDBs.tsdbsMtx.Lock()
	defer i.TSDBState.backfillDBs.tsdbsMtx.Unlock()

	// Using head compaction concurrency for both head compaction and shipping.
	ch := make(chan *userTSDB, concurrency)

	wg := &sync.WaitGroup{}
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for db := range ch {
				userFunc(db)
			}
		}()
	}

sendLoop:
	for _, buckets := range i.TSDBState.backfillDBs.tsdbs {
		buckets.Lock()
		for _, bucket := range buckets.buckets {
			select {
			case ch <- bucket.db:
				// ok
			case <-ctx.Done():
				buckets.Unlock()
				// don't start new tasks.
				break sendLoop
			}

		}
		buckets.Unlock()
	}
	close(ch)

	wg.Wait()
}

func (i *Ingester) compactOldBackfillTSDBsAndShip(gracePeriod int64) error {
	return i.runOnBucketsBefore(false,
		func(t int64) int64 {
			return t - gracePeriod
		},
		func(db *userTSDB) error {
			// Compact the head first.
			h := db.Head()
			if err := db.CompactHead(tsdb.NewRangeHead(h, h.MinTime(), h.MaxTime())); err != nil {
				return errors.Wrap(err, "compact head")
			}

			if db.shipper == nil {
				return nil
			}
			// Ship the block.
			uploaded, err := db.shipper.Sync(context.Background())
			if err != nil {
				return errors.Wrap(err, "ship block")
			}
			level.Debug(util.Logger).Log("msg", "shipper successfully synchronized backfill TSDB blocks with storage", "user", db.userID, "uploaded", uploaded, "bucket_dir", db.Dir())
			return nil
		},
	)
}

func (i *Ingester) closeOldBackfillTSDBsAndDelete(gracePeriod int64) error {
	nowTimeMs := time.Now().Unix() * 1000
	return i.runOnBucketsBefore(true,
		func(t int64) int64 {
			return nowTimeMs - gracePeriod
		},
		func(db *userTSDB) error {
			// Compact the head if anything is left. Empty head will create no blocks.
			h := db.Head()
			if err := db.CompactHead(tsdb.NewRangeHead(h, h.MinTime(), h.MaxTime())); err != nil {
				return errors.Wrap(err, "compact head")
			}

			// TODO(codesome): check for double closing.
			if err := db.Close(); err != nil {
				return errors.Wrap(err, "close backfill TSDB")
			}

			unshippedBlocks, err := db.getUnshippedBlocksULID()
			if err != nil && errors.Cause(err) == os.ErrNotExist {
				return errors.Wrap(err, "get unshipped blocks")
			}
			if err != nil || len(unshippedBlocks) > 0 {
				// Ship the unshipped blocks.
				uploaded, err := db.shipper.Sync(context.Background())
				if err != nil {
					return errors.Wrap(err, "ship block")
				}
				level.Debug(util.Logger).Log("msg", "shipper successfully synchronized backfill TSDB blocks with storage", "user", db.userID, "uploaded", uploaded, "bucket_dir", db.Dir())
			}

			if err := os.RemoveAll(db.Dir()); err != nil {
				return errors.Wrap(err, "delete backfill TSDB dir")
			}
			return nil
		},
	)
}

func (i *Ingester) runOnBucketsBefore(deleteBucket bool, gracePeriodFunc func(t int64) int64, f func(db *userTSDB) error) error {
	i.TSDBState.backfillDBs.compactShipDeleteMtx.Lock()
	defer i.TSDBState.backfillDBs.compactShipDeleteMtx.Unlock()

	type tempType struct {
		userID     string
		cutoffTime int64
		buckets    *tsdbBuckets
	}

	var usersHavingOldTSDBs []tempType

	// Collecting users who have old TSDBs.
	i.TSDBState.backfillDBs.tsdbsMtx.RLock()
	for userID, userBuckets := range i.TSDBState.backfillDBs.tsdbs {
		cutoffTime := int64(0)
		i.userStatesMtx.RLock()
		mainDB := i.TSDBState.dbs[userID]
		i.userStatesMtx.RUnlock()
		userBuckets.RLock()
		if mainDB != nil {
			cutoffTime = gracePeriodFunc(mainDB.Head().MaxTime())
		} else {
			// There is no main TSDB. So use the maxt of the last bucket.
			cutoffTime = gracePeriodFunc(userBuckets.buckets[len(userBuckets.buckets)-1].db.Head().MaxTime())
		}
		if userBuckets.buckets[0].bucketEnd < cutoffTime {
			usersHavingOldTSDBs = append(usersHavingOldTSDBs, tempType{
				userID:     userID,
				cutoffTime: cutoffTime,
				buckets:    userBuckets,
			})
		}
		userBuckets.RUnlock()
	}
	i.TSDBState.backfillDBs.tsdbsMtx.RUnlock()

	var merr tsdb_errors.MultiError
	for _, user := range usersHavingOldTSDBs {
		idx := 0
		for {
			user.buckets.RLock()
			if len(user.buckets.buckets) == 0 || idx == len(user.buckets.buckets) {
				user.buckets.RUnlock()
				break
			}
			bucket := user.buckets.buckets[idx]
			if bucket.bucketEnd >= user.cutoffTime {
				user.buckets.RUnlock()
				break
			}
			user.buckets.RUnlock()

			if err := f(bucket.db); err != nil {
				merr.Add(err)
				break
			}
			idx++
			if deleteBucket {
				user.buckets.Lock()
				user.buckets.buckets = user.buckets.buckets[1:]
				user.buckets.Unlock()
				idx--
			}
		}

		if deleteBucket {
			// Check for empty buckets.
			user.buckets.Lock()
			i.TSDBState.backfillDBs.tsdbsMtx.Lock()
			if len(user.buckets.buckets) == 0 {
				delete(i.TSDBState.backfillDBs.tsdbs, user.userID)
			}
			i.TSDBState.backfillDBs.tsdbsMtx.Unlock()
			user.buckets.Unlock()
		}
	}

	return merr.Err()
}

// Assumes 1h bucket range for . TODO(codesome): protect stuff with locks.
type backfillTSDBs struct {
	tsdbsMtx             sync.RWMutex
	compactShipDeleteMtx sync.Mutex
	tsdbs                map[string]*tsdbBuckets
}

func newBackfillTSDBs() *backfillTSDBs {
	return &backfillTSDBs{
		tsdbs: make(map[string]*tsdbBuckets),
	}
}

func (b *backfillTSDBs) getBucketsForUser(userID string) *tsdbBuckets {
	b.tsdbsMtx.RLock()
	defer b.tsdbsMtx.RUnlock()
	return b.tsdbs[userID]
}

func (b *backfillTSDBs) getOrCreateNewUser(userID string) *tsdbBuckets {
	b.tsdbsMtx.Lock()
	defer b.tsdbsMtx.Unlock()
	buckets, ok := b.tsdbs[userID]
	if !ok {
		buckets = &tsdbBuckets{}
		b.tsdbs[userID] = buckets
	}
	return buckets
}

type tsdbBuckets struct {
	sync.RWMutex
	buckets []*tsdbBucket
}

type tsdbBucket struct {
	id                     int // This is any number but should be unique among all buckets of a user.
	db                     *userTSDB
	bucketStart, bucketEnd int64
}

func (b *tsdbBucket) overlaps(mint, maxt int64) bool {
	return overlapsOpenInterval(b.bucketStart, b.bucketEnd, mint, maxt)
}

func overlapsOpenInterval(mint1, maxt1, mint2, maxt2 int64) bool {
	return mint1 < maxt2 && mint2 < maxt1
}

// getBucketName returns the string representation of the bucket.
// YYYY_MM_DD_HH_YYYY_MM_DD_HH
func getBucketName(start, end int64) string {
	startTime := model.Time(start).Time().UTC()
	endTime := model.Time(end).Time().UTC()

	return fmt.Sprintf(
		"%04d_%02d_%02d_%02d_%04d_%02d_%02d_%02d",
		startTime.Year(), startTime.Month(), startTime.Day(), startTime.Hour(),
		endTime.Year(), endTime.Month(), endTime.Day(), endTime.Hour(),
	)
}

func getBucketRangesForTimestamp(ts int64, bucketSize int) (int64, int64) {
	// TODO(codesome): Replace this entire thing with 1-2 simple equations
	// to align ts with the nearest hours which also aligns with bucketSize.
	t := time.Unix(ts/1000, 0).UTC()
	yyyy := t.Year()
	mm := t.Month()
	dd := t.Day()
	hh := bucketSize * (t.Hour() / bucketSize)
	t = time.Date(yyyy, mm, dd, hh, 0, 0, 0, time.UTC)

	start := t.Unix() * 1000
	end := start + int64(time.Duration(bucketSize)*time.Hour/time.Millisecond)

	return start, end
}

func getBucketRangesForBucketName(bucketName string) (int64, int64, error) {
	// TODO(codesome) use time.Parse.

	// YYYY_MM_DD_HH_YYYY_MM_DD_HH
	// 012345678901234567890123456
	if len(bucketName) != 27 {
		return 0, 0, errors.New("Invalid bucket name")
	}

	startYYYY, err := strconv.Atoi(bucketName[0:4])
	if err != nil {
		return 0, 0, err
	}
	startMM, err := strconv.Atoi(bucketName[5:7])
	if err != nil {
		return 0, 0, err
	}
	startDD, err := strconv.Atoi(bucketName[8:10])
	if err != nil {
		return 0, 0, err
	}
	startHH, err := strconv.Atoi(bucketName[11:13])
	if err != nil {
		return 0, 0, err
	}

	endYYYY, err := strconv.Atoi(bucketName[14:18])
	if err != nil {
		return 0, 0, err
	}
	endMM, err := strconv.Atoi(bucketName[19:21])
	if err != nil {
		return 0, 0, err
	}
	endDD, err := strconv.Atoi(bucketName[22:24])
	if err != nil {
		return 0, 0, err
	}
	endHH, err := strconv.Atoi(bucketName[25:27])
	if err != nil {
		return 0, 0, err
	}

	startTime := time.Date(startYYYY, time.Month(startMM), startDD, startHH, 0, 0, 0, time.UTC)
	endTime := time.Date(endYYYY, time.Month(endMM), endDD, endHH, 0, 0, 0, time.UTC)

	start := startTime.Unix() * 1000
	end := endTime.Unix() * 1000

	return start, end, nil
}
