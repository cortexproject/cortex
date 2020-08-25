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

	// dbs[0] is the one which would be overlapping with the main TSDB.
	// dbs[1] is the older TSDB among the two here.
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

func (b *backfillTSDB) appender(i *Ingester) *backfillAppender {
	return &backfillAppender{
		ingester:     i,
		backfillTSDB: b,
	}
}

// backfillAppender is an appender to ingest old data.
// This _does not_ implement storage.Appender interface.
// The methods of this appender should not be called concurrently.
type backfillAppender struct {
	ingester                      *Ingester
	backfillTSDB                  *backfillTSDB
	firstAppender, secondAppender *backfillRangeAppender
}

type backfillRangeAppender struct {
	app        storage.Appender
	db         *userTSDB
	start, end int64
}

// add requires the samples to be within the backfill age.
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

func (a *backfillAppender) getAppender(s client.Sample) (storage.Appender, *userTSDB, error) {
	var app storage.Appender
	var db *userTSDB

	// Fast path.
	if a.firstAppender != nil && s.TimestampMs >= a.firstAppender.start && s.TimestampMs < a.firstAppender.end {
		return a.firstAppender.app, a.firstAppender.db, nil
	} else if a.secondAppender != nil && s.TimestampMs >= a.secondAppender.start && s.TimestampMs < a.secondAppender.end {
		return a.secondAppender.app, a.secondAppender.db, nil
	}

	// We could use RLock here to check the TSDBs, but because we create new TSDB if none exists,
	// it's easier and cleaner to handle races by taking the write lock.
	// TODO(codesome): If we see performance issues, explore ways to read lock here.
	// TODO(codesome): IMPORTANT: fix the issue where the compaction moves the TSDB around between 2 appends to avoid using wrong appenders in first and second.
	a.backfillTSDB.mtx.Lock()
	defer a.backfillTSDB.mtx.Unlock()
	// Check if we already have TSDB created and use it.
	if a.backfillTSDB.dbs[0] != nil && s.TimestampMs >= a.backfillTSDB.dbs[0].start && s.TimestampMs < a.backfillTSDB.dbs[0].end {
		if a.firstAppender == nil {
			a.firstAppender = &backfillRangeAppender{
				app:   a.backfillTSDB.dbs[0].db.Appender(),
				db:    a.backfillTSDB.dbs[0].db,
				start: a.backfillTSDB.dbs[0].start,
				end:   a.backfillTSDB.dbs[0].end,
			}
		}
		app = a.firstAppender.app
		db = a.backfillTSDB.dbs[0].db
	} else if a.backfillTSDB.dbs[1] != nil && s.TimestampMs >= a.backfillTSDB.dbs[1].start && s.TimestampMs < a.backfillTSDB.dbs[1].end {
		if a.secondAppender == nil {
			a.secondAppender = &backfillRangeAppender{
				app:   a.backfillTSDB.dbs[1].db.Appender(),
				db:    a.backfillTSDB.dbs[1].db,
				start: a.backfillTSDB.dbs[1].start,
				end:   a.backfillTSDB.dbs[1].end,
			}
		}
		app = a.secondAppender.app
		db = a.backfillTSDB.dbs[1].db
	} else if s.TimestampMs > mtime.Now().Add(-a.backfillTSDB.backfillAge-time.Hour).Unix()*1000 {
		// The sample is in the backfill range.
		if a.backfillTSDB.dbs[0] != nil && a.backfillTSDB.dbs[1] != nil {
			// This can happen if the dbs[1] is running compaction/shipping.
			return nil, nil, errors.New("cannot create another backfill TSDB, 2 already exists")
		}

		var err error
		start, end := a.timeRangesForTimestamp(s.TimestampMs)
		db, err = a.ingester.createNewTSDB(
			a.backfillTSDB.userID,
			filepath.Join(
				a.ingester.cfg.BlocksStorageConfig.TSDB.BackfillBlocksDir(a.backfillTSDB.userID),
				getTSDBName(start, end),
			),
			(end-start)*2, (end-start)*2, prometheus.NewRegistry(),
		)
		if err != nil {
			return nil, nil, err
		}

		// TODO(codesome): close db after an error below.
		newDB := &backfillTSDBWrapper{
			db:    db,
			start: start,
			end:   end,
		}
		if end >= mtime.Now().Add(-time.Hour).Unix()*1000 {
			// The TSDB would touch the main TSDB. Hence this is the first TSDB.
			if a.backfillTSDB.dbs[0] != nil {
				// Check if we have to move this TSDB to the next position.
				if a.backfillTSDB.dbs[0].start <= mtime.Now().Add(-a.backfillTSDB.backfillAge-time.Hour).Unix()*1000 {
					a.backfillTSDB.dbs[1] = a.backfillTSDB.dbs[0]
					a.backfillTSDB.dbs[0] = nil
				} else {
					// This should not happen.
					return nil, nil, errors.New("cannot create another backfill TSDB, cannot move TSDB")
				}
			}
			a.backfillTSDB.dbs[0] = newDB
			app = db.Appender()
			a.firstAppender = &backfillRangeAppender{app: app, db: db, start: start, end: end}
		} else {
			if a.backfillTSDB.dbs[1] != nil {
				// This should not happen.
				return nil, nil, errors.New("cannot create another backfill TSDB, older TSDB already exists")
			}
			a.backfillTSDB.dbs[1] = newDB
			app = db.Appender()
			a.secondAppender = &backfillRangeAppender{app: app, db: db, start: start, end: end}
		}

		a.ingester.metrics.numBackfillTSDBsPerUser.WithLabelValues(a.backfillTSDB.userID).Inc()
		if a.backfillTSDB.dbs[0] == nil || a.backfillTSDB.dbs[1] == nil {
			// This user did not have a backfill TSDB before.
			a.ingester.metrics.numUsersWithBackfillTSDBs.Inc()
		}
	}
	if app == nil {
		return nil, nil, storage.ErrOutOfBounds
	}

	return app, db, nil
}

func (a *backfillAppender) commit() error {
	var merr tsdb_errors.MultiError
	if a.firstAppender != nil {
		merr.Add(a.firstAppender.app.Commit())
	}
	if a.secondAppender != nil {
		merr.Add(a.secondAppender.app.Commit())
	}
	return merr.Err()
}

func (a *backfillAppender) rollback() error {
	var merr tsdb_errors.MultiError
	if a.firstAppender != nil {
		merr.Add(a.firstAppender.app.Rollback())
	}
	if a.secondAppender != nil {
		merr.Add(a.secondAppender.app.Rollback())
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
		ss := q.Select(false, nil, matchers...)
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
	secondCompacted := false
	for i := range []int{1, 0} {
		b.mtx.RLock()
		db := b.dbs[i]
		b.mtx.RUnlock()

		if db == nil || (!force && db.end > mtime.Now().Add(-b.backfillAge-time.Hour).Unix()*1000) {
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
			secondCompacted = true
		}

		if err := os.RemoveAll(db.db.Dir()); err != nil {
			// TODO(codesome): Add a metric for this to alert on.
			level.Error(util.Logger).Log("msg", "failed to delete backfill TSDB dir in compact", "user", db.db.userID, "dir", db.db.Dir())
		}
	}

	if secondCompacted {
		b.mtx.Lock()
		// Move the TSDB because if the older TSDB was compacted,
		// then the newer among them now becomes the oldest.
		b.dbs[1] = b.dbs[0]
		b.dbs[0] = nil
		b.mtx.Unlock()
	}

	return merr.Err()
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
