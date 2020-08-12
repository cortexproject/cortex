package ingester

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"

	"github.com/cortexproject/cortex/pkg/ingester/client"
)

type backfillTSDB struct {
	userID      string
	backfillAge time.Duration

	mtx sync.RWMutex
	// firstTSDB is the one which would be overlapping with the main TSDB.
	// secondTSDB is the older TSDB among the two here.
	// If the secondTSDB goes beyond the backfill age, it will be compacted
	// and shipped and firstTSDB is moved to secondTSDB. During this process,
	// there is only 1 TSDB ingesting old data, so there can be gaps till the compaction
	// and shipping is going on.
	// TODO(codesome): Avoid gaps by using a separate queue to compact and ship blocks.
	firstTSDB, secondTSDB *backfillTSDBWrapper
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
	if a.backfillTSDB.firstTSDB != nil && s.TimestampMs >= a.backfillTSDB.firstTSDB.start && s.TimestampMs < a.backfillTSDB.firstTSDB.end {
		if a.firstAppender != nil {
			a.firstAppender = a.backfillTSDB.firstTSDB.db.Appender()
		}
		app = a.firstAppender
		db = a.backfillTSDB.firstTSDB.db
	} else if a.backfillTSDB.secondTSDB != nil && s.TimestampMs >= a.backfillTSDB.secondTSDB.start && s.TimestampMs < a.backfillTSDB.secondTSDB.end {
		if a.secondAppender != nil {
			a.secondAppender = a.backfillTSDB.secondTSDB.db.Appender()
		}
		app = a.secondAppender
		db = a.backfillTSDB.secondTSDB.db
	} else if s.TimestampMs >= time.Now().Add(-a.backfillTSDB.backfillAge-time.Hour).Unix()*1000 {
		// The sample is in the backfill range.
		if a.backfillTSDB.firstTSDB != nil && a.backfillTSDB.secondTSDB != nil {
			// This can happen if the secondTSDB is running compaction/shipping.
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
			a.backfillTSDB.firstTSDB = newDB
		} else {
			a.backfillTSDB.secondTSDB = newDB
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
	for _, db := range []*backfillTSDBWrapper{u.backfillTSDB.firstTSDB, u.backfillTSDB.secondTSDB} {
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
