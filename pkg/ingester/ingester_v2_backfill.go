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
	buckets   []*tsdbBucket
	appenders map[int]storage.Appender
}

func (i *Ingester) newBackfillAppender(userID string) *backfillAppender {
	return &backfillAppender{
		userID:    userID,
		ingester:  i,
		buckets:   i.TSDBState.backfillDBs.getBucketsForUser(userID),
		appenders: make(map[int]storage.Appender),
	}
}

func (a *backfillAppender) add(la []client.LabelAdapter, s client.Sample) (err error) {
	bucket := getBucketForTimestamp(s.TimestampMs, a.buckets)
	if bucket == nil {
		var userBuckets []*tsdbBucket
		bucket, userBuckets, err = a.ingester.getOrCreateBackfillTSDB(a.userID, s.TimestampMs)
		if err != nil {
			return err
		}
		a.buckets = userBuckets
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

func (i *Ingester) getOrCreateBackfillTSDB(userID string, ts int64) (*tsdbBucket, []*tsdbBucket, error) {
	i.TSDBState.backfillDBs.tsdbsMtx.Lock()
	defer i.TSDBState.backfillDBs.tsdbsMtx.Unlock()

	userBuckets := i.TSDBState.backfillDBs.tsdbs[userID]

	start, end := getBucketRangesForTimestamp(ts, 1)

	insertIdx := len(userBuckets)
	for idx, b := range userBuckets {
		if ts >= b.bucketStart && ts < b.bucketEnd {
			fmt.Println("bucket found")
			return b, userBuckets, nil
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
			insertIdx = idx
			break
		}

		if b.bucketStart > end {
			insertIdx = idx
			break
		}
	}

	tsdb, err := i.createNewTSDB(
		userID, filepath.Join(i.cfg.TSDBConfig.BackfillDir, userID, getBucketName(start, end)),
		(end-start)*2, (end-start)*2, prometheus.NewRegistry(),
	)
	if err != nil {
		return nil, nil, err
	}
	bucket := &tsdbBucket{
		db:          tsdb,
		bucketStart: start,
		bucketEnd:   end,
	}
	if len(userBuckets) > 0 {
		bucket.id = userBuckets[len(userBuckets)-1].id + 1
	}
	userBuckets = append(userBuckets[:insertIdx], append([]*tsdbBucket{bucket}, userBuckets[insertIdx:]...)...)

	i.TSDBState.backfillDBs.tsdbs[userID] = userBuckets

	return bucket, userBuckets, nil
}

func (i *Ingester) openExistingBackfillTSDB(ctx context.Context) error {
	level.Info(util.Logger).Log("msg", "opening existing TSDBs")
	wg := &sync.WaitGroup{}
	openGate := gate.New(i.cfg.TSDBConfig.MaxTSDBOpeningConcurrencyOnStartup)

	users, err := ioutil.ReadDir(i.cfg.TSDBConfig.BackfillDir)
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
		userPath := filepath.Join(i.cfg.TSDBConfig.BackfillDir, userID)

		bucketNames, err := ioutil.ReadDir(userPath)
		if err != nil {
			level.Error(util.Logger).Log("msg", "unable to open user TSDB dir for backfill", "err", err, "user", u, "path", userPath)
			continue
		}

		for bucketID, bucketName := range bucketNames {
			if bucketName.IsDir() {
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
			go func(bucketID int, userID, bucketName string) {
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
				db, err := i.createNewTSDB(userID, filepath.Join(userID, bucketName), (end-start)*2, (end-start)*2, prometheus.NewRegistry())
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

				i.TSDBState.backfillDBs.tsdbsMtx.Lock()
				// Append at the end, we will sort it at the end.
				i.TSDBState.backfillDBs.tsdbs[userID] = append(i.TSDBState.backfillDBs.tsdbs[userID], bucket)
				i.TSDBState.backfillDBs.tsdbsMtx.Unlock()
			}(bucketID, userID, bucketName.Name())
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
		sort.Slice(buckets, func(i, j int) bool {
			return buckets[i].bucketStart < buckets[i].bucketEnd
		})
	}
	i.TSDBState.backfillDBs.tsdbsMtx.Unlock()

	return runErr
}

func (i *Ingester) backfillSelect(ctx context.Context, userID string, from, through int64, matchers []*labels.Matcher) ([]storage.SeriesSet, error) {
	buckets := i.TSDBState.backfillDBs.getBucketsForUser(userID)

	var queriers []storage.Querier
	defer func() {
		for _, q := range queriers {
			q.Close()
		}
	}()
	for _, b := range buckets {
		if !b.overlaps(from, through) {
			mint, err := b.db.DB.StartTime()
			if err != nil {
				return nil, err
			}
			maxt := b.db.Head().MaxTime()
			if !overlapsOpenInterval(mint, maxt, from, through) {
				continue
			}
		}

		q, err := b.db.Querier(ctx, from, through)
		if err != nil {
			return nil, err
		}

		queriers = append(queriers, q)
	}

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

// Assumes 1h bucket range for . TODO(codesome): protect stuff with locks.
type backfillTSDBs struct {
	tsdbsMtx sync.RWMutex
	// TODO(codesome): have more granular locks.
	tsdbs map[string][]*tsdbBucket
}

func newBackfillTSDBs() *backfillTSDBs {
	return &backfillTSDBs{
		tsdbs: make(map[string][]*tsdbBucket),
	}
}

func (b *backfillTSDBs) getBucketsForUser(userID string) []*tsdbBucket {
	b.tsdbsMtx.RLock()
	defer b.tsdbsMtx.RUnlock()
	return b.tsdbs[userID]
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
	startTime := model.Time(start).Time()
	endTime := model.Time(end).Time()

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
