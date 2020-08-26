package ingester

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/weaveworks/common/mtime"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
)

type backfillTSDB struct {
	userID      string
	backfillAge time.Duration
	metrics     *ingesterMetrics

	// dbs[0] is the one which would be overlapping with the main TSDB. "newer" TSDB.
	// dbs[1] is the older TSDB among the two here. "older" TSDB.
	// If the dbs[1] goes beyond the backfill age, it will be compacted
	// and shipped and dbs[0] is moved to dbs[1]. During this process,
	// there is only 1 TSDB ingesting old data, so there can be gaps till the compaction
	// and shipping is going on.
	// TODO(codesome): Avoid gaps by using a separate queue to compact and ship blocks.
	dbs [2]*backfillTSDBWrapper

	mtx sync.RWMutex
	// We need this lock to safeguard force compactions from moving around of TSDBs between dbs array.
	compactMtx sync.Mutex
}

type backfillTSDBWrapper struct {
	db *userTSDB
	// Time boundaries of the TSDBs in ms. Start is inclusive and End is exclusive.
	start, end int64
}

func newBackfillTSDB(userID string, backfillAge time.Duration, metrics *ingesterMetrics) *backfillTSDB {
	return &backfillTSDB{
		userID:      userID,
		backfillAge: backfillAge,
		metrics:     metrics,
	}
}

func (b *backfillTSDB) appender(
	createNewTSDB func(userID, dbDir string, minBlockDuration, maxBlockDuration int64, reg *prometheus.Registry) (*userTSDB, error),
	backfillBlocksDir func(userID string) string,
) *backfillAppender {
	return &backfillAppender{
		createNewTSDB:     createNewTSDB,
		backfillBlocksDir: backfillBlocksDir,
		backfillTSDB:      b,
	}
}

// backfillAppender is an appender to ingest old data.
// This _does not_ implement storage.Appender interface.
// The methods of this appender should not be called concurrently.
type backfillAppender struct {
	backfillTSDB                 *backfillTSDB
	newerAppender, olderAppender *backfillRangeAppender

	// Keeping this methods of Ingester here to avoid the appender having a reference to the ingester.
	createNewTSDB func(userID, dbDir string, minBlockDuration, maxBlockDuration int64, reg *prometheus.Registry) (*userTSDB, error)
	// We are keeping the method instead of string for backfill dir because if there is a surge in backfill writes,
	// it will create a new string for every push while we want it only to open a new TSDB.
	backfillBlocksDir func(userID string) string
}

type backfillRangeAppender struct {
	app        storage.Appender
	db         *userTSDB
	start, end int64
}

func newBackfillRangeAppender(db *backfillTSDBWrapper) *backfillRangeAppender {
	return &backfillRangeAppender{
		app:   db.db.Appender(),
		db:    db.db,
		start: db.start,
		end:   db.end,
	}
}

// add requires the samples to be within the backfill age.
// Note: This method should not be called concurrently.
func (a *backfillAppender) add(la []client.LabelAdapter, s client.Sample) (err error) {
	app, db, err := a.getAppender(s)
	if err != nil {
		return err
	}
	startAppend := time.Now()

	cachedRef, cachedRefExists := db.refCache.Ref(startAppend, client.FromLabelAdaptersToLabels(la))
	// If the cached reference exists, we try to use it.
	if cachedRefExists {
		err = app.AddFast(cachedRef, s.TimestampMs, s.Value)
		if err == nil || errors.Cause(err) != storage.ErrNotFound {
			return err
		}
	}

	// Copy the label set because both TSDB and the cache may retain it.
	copiedLabels := client.FromLabelAdaptersToLabelsWithCopy(la)
	ref, err := app.Add(copiedLabels, s.TimestampMs, s.Value)
	if err == nil {
		db.refCache.SetRef(startAppend, copiedLabels, ref)
	}

	return err
}

func (a *backfillAppender) getAppender(s client.Sample) (storage.Appender, *userTSDB, error) {
	if !a.backfillTSDB.isWithinBackfillAge(s.TimestampMs) {
		return nil, nil, storage.ErrOutOfBounds
	}

	// Fast path to not take the lock.
	if a.newerAppender != nil && s.TimestampMs >= a.newerAppender.start && s.TimestampMs < a.newerAppender.end {
		return a.newerAppender.app, a.newerAppender.db, nil
	} else if a.olderAppender != nil && s.TimestampMs >= a.olderAppender.start && s.TimestampMs < a.olderAppender.end {
		return a.olderAppender.app, a.olderAppender.db, nil
	}

	// We could use RLock here to check the TSDBs, but because we create new TSDB if none exists,
	// it's easier and cleaner to handle races by taking the write lock.
	// TODO(codesome): If we see performance issues, explore ways to read lock here.
	// TODO(codesome): IMPORTANT: fix the issue where the compaction moves the TSDB around between 2 appends to avoid using wrong appenders in older and newer spots.
	a.backfillTSDB.mtx.Lock()
	defer a.backfillTSDB.mtx.Unlock()

	// Check if we already have TSDB created and use it.
	if a.backfillTSDB.dbs[0] != nil && s.TimestampMs >= a.backfillTSDB.dbs[0].start && s.TimestampMs < a.backfillTSDB.dbs[0].end {
		a.newerAppender = newBackfillRangeAppender(a.backfillTSDB.dbs[0])
		return a.newerAppender.app, a.backfillTSDB.dbs[0].db, nil
	} else if a.backfillTSDB.dbs[1] != nil && s.TimestampMs >= a.backfillTSDB.dbs[1].start && s.TimestampMs < a.backfillTSDB.dbs[1].end {
		a.olderAppender = newBackfillRangeAppender(a.backfillTSDB.dbs[1])
		return a.olderAppender.app, a.backfillTSDB.dbs[1].db, nil
	}

	// We need to open a new TSDB.

	start, end := a.timeRangesForTimestamp(s.TimestampMs)
	isNewerTSDB := end >= mtime.Now().Add(-time.Hour).Unix()*1000 // If the TSDB would touch the main TSDB.
	if (isNewerTSDB && a.backfillTSDB.dbs[0] != nil) || (!isNewerTSDB && a.backfillTSDB.dbs[1] != nil) {
		// The compaction needs to clean up these spots before we can create new ones.
		// Cleaning up the spots includes compacting older TSDB and/or moving newer TSDB
		// to older TSDB's spot.
		if isNewerTSDB {
			return nil, nil, errors.New("cannot create another backfill TSDB, newer TSDB already exists")
		} else {
			return nil, nil, errors.New("cannot create another backfill TSDB, older TSDB already exists")
		}
	}

	db, err := a.createNewTSDB(
		a.backfillTSDB.userID,
		filepath.Join(
			a.backfillBlocksDir(a.backfillTSDB.userID),
			getTSDBName(start, end),
		),
		(end-start)*2, (end-start)*2, prometheus.NewRegistry(),
	)
	if err != nil {
		return nil, nil, err
	}

	newDB := &backfillTSDBWrapper{db: db, start: start, end: end}
	app := newBackfillRangeAppender(newDB)
	if isNewerTSDB {
		a.backfillTSDB.dbs[0] = newDB
		a.newerAppender = app
	} else {
		a.backfillTSDB.dbs[1] = newDB
		a.olderAppender = app
	}

	a.backfillTSDB.metrics.numBackfillTSDBsPerUser.WithLabelValues(a.backfillTSDB.userID).Inc()
	if a.backfillTSDB.dbs[0] == nil || a.backfillTSDB.dbs[1] == nil {
		// This user did not have a backfill TSDB before.
		a.backfillTSDB.metrics.numUsersWithBackfillTSDBs.Inc()
	}

	return app.app, app.db, nil
}

func (a *backfillAppender) commit() error {
	var merr tsdb_errors.MultiError
	if a.newerAppender != nil {
		merr.Add(a.newerAppender.app.Commit())
	}
	if a.olderAppender != nil {
		merr.Add(a.olderAppender.app.Commit())
	}
	return merr.Err()
}

func (a *backfillAppender) rollback() error {
	var merr tsdb_errors.MultiError
	if a.newerAppender != nil {
		merr.Add(a.newerAppender.app.Rollback())
	}
	if a.olderAppender != nil {
		merr.Add(a.olderAppender.app.Rollback())
	}
	return merr.Err()
}

func (u *userTSDB) backfillSelect(ctx context.Context, from, through int64, matchers []*labels.Matcher) ([]storage.SeriesSet, error) {
	var queriers []storage.Querier
	defer func() {
		for _, q := range queriers {
			q.Close()
		}
	}()

	u.backfillTSDB.mtx.RLock()
	for _, db := range u.backfillTSDB.dbs {
		if db != nil && overlapsOpenInterval(db.start, db.end, from, through) {
			mint := db.db.Head().MinTime()
			maxt := db.db.Head().MaxTime()
			if overlapsOpenInterval(mint, maxt, from, through) {
				q, err := db.db.Querier(ctx, from, through)
				if err != nil {
					u.backfillTSDB.mtx.RUnlock()
					return nil, err
				}

				queriers = append(queriers, q)
			}
		}
	}
	u.backfillTSDB.mtx.RUnlock()

	if len(queriers) == 0 {
		return nil, nil
	}

	result := make([]storage.SeriesSet, len(queriers))
	for i, q := range queriers {
		ss := q.Select(true, nil, matchers...)
		if ss.Err() != nil {
			return nil, ss.Err()
		}
		result[i] = ss
	}

	return result, nil
}

// compactAndShipAndDelete compacts, ships, closes, and deletes any TSDB that has
// gone beyond the backfill age. Error in deletion of the TSDB directory is not returned.
// force=true does the above operation irrespective of backfill age.
func (b *backfillTSDB) compactAndShipAndDelete(force bool) (err error) {
	b.compactMtx.Lock()
	defer b.compactMtx.Unlock()

	var merr tsdb_errors.MultiError
	moveNewerTSDB := false
	for i := range []int{1, 0} {
		b.mtx.RLock()
		db := b.dbs[i]
		b.mtx.RUnlock()

		if db == nil || (!force && b.isWithinBackfillAge(db.end)) {
			// DB is either nil or not outside backfill age yet.
			// It might be a forced compaction, so we continue instead of breaking.
			continue
		}

		if err := b.compactAndShipAndCloseDB(i); err != nil {
			merr.Add(err)
			// It might be a forced compaction, so we continue instead of breaking.
			continue
		}

		if i == 1 {
			// The older TSDB was compacted, hence the newer among them now becomes the oldest.
			moveNewerTSDB = true
		}

		if err := os.RemoveAll(db.db.Dir()); err != nil {
			// TODO(codesome): Add a metric for this to alert on.
			level.Error(util.Logger).Log("msg", "failed to delete backfill TSDB dir in compact", "user", db.db.userID, "dir", db.db.Dir())
		}
	}

	b.mtx.RLock()
	if b.dbs[1] == nil && b.dbs[0] != nil && !b.isWithinBackfillAge(b.dbs[0].start) {
		// There is no older TSDB and the newer TSDB's start now crosses the backfill age, hence
		// has to be moved to be the older TSDB.
		moveNewerTSDB = true
	}
	b.mtx.RUnlock()

	if moveNewerTSDB {
		b.mtx.Lock()
		b.dbs[1] = b.dbs[0]
		b.dbs[0] = nil
		b.mtx.Unlock()
	}

	return merr.Err()
}

// isWithinBackfillAge returns true if the given timestamp is within the backfill age.
func (b *backfillTSDB) isWithinBackfillAge(ts int64) bool {
	// The backfillAge is backfill time beyond what is already possible by main TSDB.
	// Hence the -time.Hour.
	return ts > mtime.Now().Add(-b.backfillAge-time.Hour).Unix()*1000
}

// compactAndShipDB compacts and ships the backfill TSDB if it is beyond the backfill age.
// If there was an error, the boolean is always false.
// NOTE: This is intended to be used by member functions of backfillTSDB only.
func (b *backfillTSDB) compactAndShipAndCloseDB(idx int) (err error) {
	b.mtx.Lock()
	db := b.dbs[idx]
	if db == nil {
		// While the caller does the nil check, we have this check here to avoid any regression.
		b.mtx.Unlock()
		return nil
	}
	// So that we don't get any samples after compaction.
	b.dbs[idx] = nil
	b.mtx.Unlock()

	defer func() {
		if err == nil {
			if cerr := db.db.Close(); cerr != nil {
				err = cerr
			}
		}

		b.mtx.Lock()
		if err != nil {
			b.dbs[idx] = db
		} else {
			if b.dbs[0] == nil && b.dbs[1] == nil {
				b.metrics.numBackfillTSDBsPerUser.DeleteLabelValues(b.userID)
				b.metrics.numUsersWithBackfillTSDBs.Dec()
			} else {
				b.metrics.numBackfillTSDBsPerUser.WithLabelValues(b.userID).Dec()
			}
		}
		b.mtx.Unlock()
	}()

	h := db.db.Head()
	if err := db.db.CompactHead(tsdb.NewRangeHead(h, h.MinTime(), h.MaxTime())); err != nil {
		return errors.Wrapf(err, "compact backfill TSDB, dir:%s", db.db.Dir())
	}

	if db.db.shipper != nil {
		uploaded, err := db.db.shipper.Sync(context.Background())
		if err != nil {
			return errors.Wrapf(err, "ship backfill TSDB, uploaded:%d, dir:%s", uploaded, db.db.Dir())
		}
		level.Debug(util.Logger).Log("msg", "shipper successfully synchronized backfill TSDB blocks with storage", "user", db.db.userID, "uploaded", uploaded, "backfill_dir", db.db.Dir())
	}

	return nil
}

func (b *backfillTSDB) isIdle(timeout time.Duration) bool {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	idle := false
	for i := 0; i < 2; i++ {
		if b.dbs[i] != nil {
			idle = idle || b.dbs[i].db.isIdle(mtime.Now(), timeout)
		}
	}
	return idle
}

func (b *backfillTSDB) compactAndShipIdleTSDBs(timeout time.Duration) error {
	b.compactMtx.Lock()
	defer b.compactMtx.Unlock()

	b.mtx.RLock()
	defer b.mtx.RUnlock()

	var merr tsdb_errors.MultiError
	for i := 0; i < 2; i++ {
		b.mtx.RLock()
		if b.dbs[i] == nil || !b.dbs[i].db.isIdle(mtime.Now(), timeout) {
			b.mtx.RUnlock()
			continue
		}
		db := b.dbs[i]
		b.mtx.RUnlock()

		err := b.compactAndShipAndCloseDB(i)
		if err != nil {
			merr.Add(err)
			continue
		}

		if err := os.RemoveAll(db.db.Dir()); err != nil {
			// TODO(codesome): Add a metric for this to alert on.
			level.Error(util.Logger).Log("msg", "failed to delete backfill TSDB dir in compactAndShipIdleTSDBs", "user", db.db.userID, "dir", db.db.Dir())
		}
	}

	return merr.Err()
}

func (a *backfillAppender) timeRangesForTimestamp(ts int64) (int64, int64) {
	step := a.backfillTSDB.backfillAge.Milliseconds()
	start := (ts / step) * step
	end := start + step
	return start, end
}

// getBucketName returns the string representation of the bucket.
// YYYY_MM_DD_HH_YYYY_MM_DD_HH
func getTSDBName(start, end int64) string {
	startTime := model.Time(start).Time().UTC()
	endTime := model.Time(end).Time().UTC()

	return fmt.Sprintf(
		"%04d_%02d_%02d_%02d_%04d_%02d_%02d_%02d",
		startTime.Year(), startTime.Month(), startTime.Day(), startTime.Hour(),
		endTime.Year(), endTime.Month(), endTime.Day(), endTime.Hour(),
	)
}

func overlapsOpenInterval(mint1, maxt1, mint2, maxt2 int64) bool {
	return mint1 < maxt2 && mint2 < maxt1
}

func getBackfillTSDBRanges(tsdbName string) (int64, int64, error) {
	// TODO(codesome) use time.Parse.

	// YYYY_MM_DD_HH_YYYY_MM_DD_HH
	// 012345678901234567890123456
	if len(tsdbName) != 27 {
		return 0, 0, errors.New("Invalid bucket name")
	}

	startYYYY, err := strconv.Atoi(tsdbName[0:4])
	if err != nil {
		return 0, 0, err
	}
	startMM, err := strconv.Atoi(tsdbName[5:7])
	if err != nil {
		return 0, 0, err
	}
	startDD, err := strconv.Atoi(tsdbName[8:10])
	if err != nil {
		return 0, 0, err
	}
	startHH, err := strconv.Atoi(tsdbName[11:13])
	if err != nil {
		return 0, 0, err
	}

	endYYYY, err := strconv.Atoi(tsdbName[14:18])
	if err != nil {
		return 0, 0, err
	}
	endMM, err := strconv.Atoi(tsdbName[19:21])
	if err != nil {
		return 0, 0, err
	}
	endDD, err := strconv.Atoi(tsdbName[22:24])
	if err != nil {
		return 0, 0, err
	}
	endHH, err := strconv.Atoi(tsdbName[25:27])
	if err != nil {
		return 0, 0, err
	}

	startTime := time.Date(startYYYY, time.Month(startMM), startDD, startHH, 0, 0, 0, time.UTC)
	endTime := time.Date(endYYYY, time.Month(endMM), endDD, endHH, 0, 0, 0, time.UTC)

	start := startTime.Unix() * 1000
	end := endTime.Unix() * 1000

	return start, end, nil
}
