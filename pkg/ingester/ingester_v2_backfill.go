package ingester

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
)

type backfillTSDB struct {
	userID      string
	backfillAge time.Duration

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

func newBackfillTSDB(userID string, backfillAge time.Duration) *backfillTSDB {
	return &backfillTSDB{userID: userID, backfillAge: backfillAge}
}

func (b *backfillTSDB) appender(i *Ingester) *backfillAppender {
	return &backfillAppender{
		ingester:     i,
		backfillTSDB: b,
	}
}

// compactAndShipAndDelete compacts, ships, closes, and deletes any TSDB that has
// gone beyond the backfill age. Error in deletion of the TSDB directory is not returned.
// force=true does the above operation irrespective of backfill age.
func (b *backfillTSDB) compactAndShipAndDelete(force bool) (err error) {
	b.compactMtx.Lock()
	defer b.compactMtx.Unlock()

	b.mtx.RLock()
	// There is no second TSDB. Try compaction for first TSDB.
	if b.dbs[1] == nil {
		firstDB := b.dbs[0]
		b.mtx.RUnlock()

		shipped, err := b.compactAndShipAndCloseDB(0, force)
		if err != nil || !shipped {
			return err
		}

		if err := os.RemoveAll(firstDB.db.Dir()); err != nil {
			// TODO(codesome): Add a metric for this to alert on.
			level.Error(util.Logger).Log("msg", "failed to delete backfill TSDB dir in compact", "user", firstDB.db.userID, "dir", firstDB.db.Dir())
		}
	}
	b.mtx.RUnlock()

	// There is second TSDB.
	// This loop takes care of clearing both TSDBs if they were beyond backfill age.
	for i := 0; i < 2; i++ {
		b.mtx.RLock()
		secondDB := b.dbs[1]
		b.mtx.RUnlock()

		shipped, err := b.compactAndShipAndCloseDB(1, force)
		if err != nil || !shipped {
			return err
		}

		b.mtx.Lock()
		b.dbs[1] = b.dbs[0]
		b.dbs[0] = nil
		b.mtx.Unlock()

		if err := os.RemoveAll(secondDB.db.Dir()); err != nil {
			// TODO(codesome): Add a metric for this to alert on.
			level.Error(util.Logger).Log("msg", "failed to delete backfill TSDB dir in compact", "user", secondDB.db.userID, "dir", secondDB.db.Dir())
		}
	}

	return nil
}

// compactAndShipDB compacts and ships the backfill TSDB if it is beyond the backfill age.
// If there was an error, the boolean is always false.
func (b *backfillTSDB) compactAndShipAndCloseDB(idx int, force bool) (shipped bool, err error) {
	b.mtx.RLock()
	db := b.dbs[idx]
	defer func() {
		b.mtx.RUnlock()
		if err != nil || !shipped {
			b.mtx.Lock()
			b.dbs[idx] = db
			b.mtx.Unlock()
			return
		}

		if cerr := db.db.Close(); cerr != nil {
			b.mtx.Lock()
			b.dbs[idx] = db
			b.mtx.Unlock()
			err = cerr
		}
	}()

	if db == nil || (!force && db.end > time.Now().Add(-b.backfillAge-time.Hour).Unix()*1000) {
		// Still inside backfill age (or nil).
		return false, nil
	}

	b.mtx.Lock()
	// So that we don't get any samples after compaction.
	b.dbs[idx] = nil
	b.mtx.Unlock()

	h := db.db.Head()
	if err := db.db.CompactHead(tsdb.NewRangeHead(h, h.MinTime(), h.MaxTime())); err != nil {
		return false, errors.Wrapf(err, "compact backfill TSDB, dir:%s", db.db.Dir())
	}

	if db.db.shipper != nil {
		if uploaded, err := db.db.shipper.Sync(context.Background()); err != nil {
			return false, errors.Wrapf(err, "ship backfill TSDB, uploaded:%d, dir:%s", uploaded, db.db.Dir())
		} else {
			level.Debug(util.Logger).Log("msg", "shipper successfully synchronized backfill TSDB blocks with storage", "user", db.db.userID, "uploaded", uploaded, "backfill_dir", db.db.Dir())
		}
	}

	return true, nil
}

func (b *backfillTSDB) isIdle(timeout time.Duration) bool {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	idle := false
	for i := 0; i < 2; i++ {
		if b.dbs[i] != nil {
			idle = idle || b.dbs[i].db.isIdle(time.Now(), timeout)
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
		if b.dbs[i] == nil || !b.dbs[i].db.isIdle(time.Now(), timeout) {
			b.mtx.RUnlock()
			continue
		}
		db := b.dbs[i]
		b.mtx.RUnlock()

		_, err := b.compactAndShipAndCloseDB(i, true)
		merr.Add(err)

		if err := os.RemoveAll(db.db.Dir()); err != nil {
			// TODO(codesome): Add a metric for this to alert on.
			level.Error(util.Logger).Log("msg", "failed to delete backfill TSDB dir in compactAndShipIdleTSDBs", "user", db.db.userID, "dir", db.db.Dir())
		}
	}
	if b.dbs[0] != nil && b.dbs[0].db.isIdle(time.Now(), timeout) {
		_, err := b.compactAndShipAndCloseDB(0, true)
		merr.Add(err)
	}

	return merr.Err()
}

// backfillAppender is an appender to ingest old data.
// This _does not_ implement storage.Appender interface.
// The methods of this appender should not be called concurrently.
type backfillAppender struct {
	ingester                      *Ingester
	backfillTSDB                  *backfillTSDB
	firstAppender, secondAppender storage.Appender
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

	a.backfillTSDB.mtx.Lock()
	// Check if we already have TSDB created and use it.
	if a.backfillTSDB.dbs[0] != nil && s.TimestampMs >= a.backfillTSDB.dbs[0].start && s.TimestampMs < a.backfillTSDB.dbs[0].end {
		if a.firstAppender != nil {
			a.firstAppender = a.backfillTSDB.dbs[0].db.Appender()
		}
		app = a.firstAppender
		db = a.backfillTSDB.dbs[0].db
	} else if a.backfillTSDB.dbs[1] != nil && s.TimestampMs >= a.backfillTSDB.dbs[1].start && s.TimestampMs < a.backfillTSDB.dbs[1].end {
		if a.secondAppender != nil {
			a.secondAppender = a.backfillTSDB.dbs[1].db.Appender()
		}
		app = a.secondAppender
		db = a.backfillTSDB.dbs[1].db
	} else if s.TimestampMs >= time.Now().Add(-a.backfillTSDB.backfillAge-time.Hour).Unix()*1000 {
		// The sample is in the backfill range.
		if a.backfillTSDB.dbs[0] != nil && a.backfillTSDB.dbs[1] != nil {
			// This can happen if the dbs[1] is running compaction/shipping.
			return nil, nil, errors.New("cannot find backfill TSDB")
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
			a.backfillTSDB.mtx.Unlock()
			return nil, nil, err
		}

		newDB := &backfillTSDBWrapper{
			db:    db,
			start: start,
			end:   end,
		}
		if end >= time.Now().Add(-time.Hour).Unix()*1000 {
			// The TSDB would touch the main TSDB. Hence this is the first TSDB.
			a.backfillTSDB.dbs[0] = newDB
		} else {
			a.backfillTSDB.dbs[1] = newDB
		}
		app = db.Appender()
	}
	a.backfillTSDB.mtx.Unlock()

	if app == nil {
		return nil, nil, storage.ErrOutOfBounds
	}

	return app, db, nil
}

func (a *backfillAppender) commit() error {
	var merr tsdb_errors.MultiError
	if a.firstAppender != nil {
		merr.Add(a.firstAppender.Commit())
	}
	if a.secondAppender != nil {
		merr.Add(a.secondAppender.Commit())
	}
	return merr.Err()
}

func (a *backfillAppender) rollback() error {
	var merr tsdb_errors.MultiError
	if a.firstAppender != nil {
		merr.Add(a.firstAppender.Rollback())
	}
	if a.secondAppender != nil {
		merr.Add(a.secondAppender.Rollback())
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

func (u *userTSDB) backfillSelect(ctx context.Context, from, through int64, matchers []*labels.Matcher) ([]storage.SeriesSet, error) {
	var queriers []storage.Querier
	defer func() {
		for _, q := range queriers {
			q.Close()
		}
	}()

	u.backfillTSDB.mtx.RLock()
	for _, db := range []*backfillTSDBWrapper{u.backfillTSDB.dbs[0], u.backfillTSDB.dbs[1]} {
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
