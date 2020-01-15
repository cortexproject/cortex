package ingester

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/gate"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"golang.org/x/net/context"
	old_ctx "golang.org/x/net/context"
)

const (
	errTSDBCreateIncompatibleState = "cannot create a new TSDB while the ingester is not in active state (current state: %s)"
)

// TSDBState holds data structures used by the TSDB storage engine
type TSDBState struct {
	dbs    map[string]*tsdb.DB // tsdb sharded by userID
	bucket objstore.Bucket

	// Keeps count of in-flight requests
	inflightWriteReqs sync.WaitGroup

	// Used to run only once operations at shutdown, during the blocks/wal
	// transferring to a joining ingester
	transferOnce sync.Once
}

// NewV2 returns a new Ingester that uses prometheus block storage instead of chunk storage
func NewV2(cfg Config, clientConfig client.Config, limits *validation.Overrides, registerer prometheus.Registerer) (*Ingester, error) {
	bucketClient, err := cortex_tsdb.NewBucketClient(context.Background(), cfg.TSDBConfig, "cortex", util.Logger)
	if err != nil {
		return nil, err
	}

	i := &Ingester{
		cfg:          cfg,
		clientConfig: clientConfig,
		metrics:      newIngesterMetrics(registerer),
		limits:       limits,
		chunkStore:   nil,
		quit:         make(chan struct{}),

		TSDBState: TSDBState{
			dbs:    make(map[string]*tsdb.DB),
			bucket: bucketClient,
		},
	}

	i.lifecycler, err = ring.NewLifecycler(cfg.LifecyclerConfig, i, "ingester", ring.IngesterRingKey)
	if err != nil {
		return nil, err
	}

	// Init the limter and instantiate the user states which depend on it
	i.limiter = NewSeriesLimiter(limits, i.lifecycler, cfg.LifecyclerConfig.RingConfig.ReplicationFactor, cfg.ShardByAllLabels)
	i.userStates = newUserStates(i.limiter, cfg)

	// Scan and open TSDB's that already exist on disk
	if err := i.openExistingTSDB(context.Background()); err != nil {
		return nil, err
	}

	// Now that user states have been created, we can start the lifecycler
	i.lifecycler.Start()

	return i, nil
}

// v2Push adds metrics to a block
func (i *Ingester) v2Push(ctx old_ctx.Context, req *client.WriteRequest) (*client.WriteResponse, error) {
	var firstPartialErr error

	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, fmt.Errorf("no user id")
	}

	db, err := i.getOrCreateTSDB(userID, false)
	if err != nil {
		return nil, wrapWithUser(err, userID)
	}

	// Ensure the ingester shutdown procedure hasn't started
	i.userStatesMtx.RLock()

	if i.stopped {
		i.userStatesMtx.RUnlock()
		return nil, fmt.Errorf("ingester stopping")
	}

	// Keep track of in-flight requests, in order to safely start blocks transfer
	// (at shutdown) only once all in-flight write requests have completed.
	// It's important to increase the number of in-flight requests within the lock
	// (even if sync.WaitGroup is thread-safe), otherwise there's a race condition
	// with the TSDB transfer, which - after the stopped flag is set to true - waits
	// until all in-flight requests to reach zero.
	i.TSDBState.inflightWriteReqs.Add(1)
	i.userStatesMtx.RUnlock()
	defer i.TSDBState.inflightWriteReqs.Done()

	// Keep track of some stats which are tracked only if the samples will be
	// successfully committed
	succeededSamplesCount := 0
	failedSamplesCount := 0

	// Walk the samples, appending them to the users database
	app := db.Appender()
	for _, ts := range req.Timeseries {
		// Convert labels to the type expected by TSDB
		lset := client.FromLabelAdaptersToLabelsWithCopy(ts.Labels)

		for _, s := range ts.Samples {
			_, err := app.Add(lset, s.TimestampMs, s.Value)
			if err == nil {
				succeededSamplesCount++
				continue
			}

			failedSamplesCount++

			// Check if the error is a soft error we can proceed on. If so, we keep track
			// of it, so that we can return it back to the distributor, which will return a
			// 400 error to the client. The client (Prometheus) will not retry on 400, and
			// we actually ingested all samples which haven't failed.
			if err == tsdb.ErrOutOfBounds || err == tsdb.ErrOutOfOrderSample || err == tsdb.ErrAmendSample {
				if firstPartialErr == nil {
					firstPartialErr = errors.Wrapf(err, "series=%s", lset.String())
				}

				continue
			}

			// The error looks an issue on our side, so we should rollback
			if rollbackErr := app.Rollback(); rollbackErr != nil {
				level.Warn(util.Logger).Log("msg", "failed to rollback on error", "user", userID, "err", rollbackErr)
			}

			return nil, wrapWithUser(err, userID)
		}
	}
	if err := app.Commit(); err != nil {
		return nil, wrapWithUser(err, userID)
	}

	// Increment metrics only if the samples have been successfully committed.
	// If the code didn't reach this point, it means that we returned an error
	// which will be converted into an HTTP 5xx and the client should/will retry.
	i.metrics.ingestedSamples.Add(float64(succeededSamplesCount))
	i.metrics.ingestedSamplesFail.Add(float64(failedSamplesCount))

	client.ReuseSlice(req.Timeseries)

	if firstPartialErr != nil {
		return &client.WriteResponse{}, httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(firstPartialErr, userID).Error())
	}
	return &client.WriteResponse{}, nil
}

func (i *Ingester) v2Query(ctx old_ctx.Context, req *client.QueryRequest) (*client.QueryResponse, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	from, through, matchers, err := client.FromQueryRequest(req)
	if err != nil {
		return nil, err
	}

	i.metrics.queries.Inc()

	db := i.getTSDB(userID)
	if db == nil {
		return &client.QueryResponse{}, nil
	}

	q, err := db.Querier(int64(from), int64(through))
	if err != nil {
		return nil, err
	}
	defer q.Close()

	ss, err := q.Select(matchers...)
	if err != nil {
		return nil, err
	}

	numSamples := 0

	result := &client.QueryResponse{}
	for ss.Next() {
		series := ss.At()

		ts := client.TimeSeries{
			Labels: client.FromLabelsToLabelAdapters(series.Labels()),
		}

		it := series.Iterator()
		for it.Next() {
			t, v := it.At()
			ts.Samples = append(ts.Samples, client.Sample{Value: v, TimestampMs: t})
		}

		numSamples += len(ts.Samples)
		result.Timeseries = append(result.Timeseries, ts)
	}

	i.metrics.queriedSeries.Observe(float64(len(result.Timeseries)))
	i.metrics.queriedSamples.Observe(float64(numSamples))

	return result, ss.Err()
}

func (i *Ingester) v2LabelValues(ctx old_ctx.Context, req *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return &client.LabelValuesResponse{}, nil
	}

	through := time.Now()
	from := through.Add(-i.cfg.TSDBConfig.Retention)
	q, err := db.Querier(from.Unix()*1000, through.Unix()*1000)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	vals, err := q.LabelValues(req.LabelName)
	if err != nil {
		return nil, err
	}

	return &client.LabelValuesResponse{
		LabelValues: vals,
	}, nil
}

func (i *Ingester) v2LabelNames(ctx old_ctx.Context, req *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return &client.LabelNamesResponse{}, nil
	}

	through := time.Now()
	from := through.Add(-i.cfg.TSDBConfig.Retention)
	q, err := db.Querier(from.Unix()*1000, through.Unix()*1000)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	names, err := q.LabelNames()
	if err != nil {
		return nil, err
	}

	return &client.LabelNamesResponse{
		LabelNames: names,
	}, nil
}

func (i *Ingester) v2MetricsForLabelMatchers(ctx old_ctx.Context, req *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return &client.MetricsForLabelMatchersResponse{}, nil
	}

	// Parse the request
	from, to, matchersSet, err := client.FromMetricsForLabelMatchersRequest(req)
	if err != nil {
		return nil, err
	}

	// Create a new instance of the TSDB querier
	q, err := db.Querier(int64(from), int64(to))
	if err != nil {
		return nil, err
	}
	defer q.Close()

	// Run a query for each matchers set and collect all the results
	added := map[string]struct{}{}
	result := &client.MetricsForLabelMatchersResponse{
		Metric: make([]*client.Metric, 0),
	}

	for _, matchers := range matchersSet {
		seriesSet, err := q.Select(matchers...)
		if err != nil {
			return nil, err
		}

		for seriesSet.Next() {
			if seriesSet.Err() != nil {
				break
			}

			// Given the same series can be matched by multiple matchers and we want to
			// return the unique set of matching series, we do check if the series has
			// already been added to the result
			ls := seriesSet.At().Labels()
			key := ls.String()
			if _, ok := added[key]; ok {
				continue
			}

			result.Metric = append(result.Metric, &client.Metric{
				Labels: client.FromLabelsToLabelAdapters(ls),
			})

			added[key] = struct{}{}
		}

		// In case of any error while iterating the series, we break
		// the execution and return it
		if err := seriesSet.Err(); err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (i *Ingester) getTSDB(userID string) *tsdb.DB {
	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	db, _ := i.TSDBState.dbs[userID]
	return db
}

func (i *Ingester) getOrCreateTSDB(userID string, force bool) (*tsdb.DB, error) {
	db := i.getTSDB(userID)
	if db != nil {
		return db, nil
	}

	i.userStatesMtx.Lock()
	defer i.userStatesMtx.Unlock()

	// Check again for DB in the event it was created in-between locks
	var ok bool
	db, ok = i.TSDBState.dbs[userID]
	if ok {
		return db, nil
	}

	// We're ready to create the TSDB, however we must be sure that the ingester
	// is in the ACTIVE state, otherwise it may conflict with the transfer in/out.
	// The TSDB is created when the first series is pushed and this shouldn't happen
	// to a non-ACTIVE ingester, however we want to protect from any bug, cause we
	// may have data loss or TSDB WAL corruption if the TSDB is created before/during
	// a transfer in occurs.
	if ingesterState := i.lifecycler.GetState(); !force && ingesterState != ring.ACTIVE {
		return nil, fmt.Errorf(errTSDBCreateIncompatibleState, ingesterState)
	}

	// Create the database and a shipper for a user
	db, err := i.createTSDB(userID)
	if err != nil {
		return nil, err
	}

	// Add the db to list of user databases
	i.TSDBState.dbs[userID] = db
	return db, nil
}

// createTSDB creates a TSDB for a given userID, and returns the created db.
func (i *Ingester) createTSDB(userID string) (*tsdb.DB, error) {
	udir := i.cfg.TSDBConfig.BlocksDir(userID)

	// Create a new user database
	db, err := tsdb.Open(udir, util.Logger, nil, &tsdb.Options{
		RetentionDuration: uint64(i.cfg.TSDBConfig.Retention / time.Millisecond),
		BlockRanges:       i.cfg.TSDBConfig.BlockRanges.ToMillisecondRanges(),
		NoLockfile:        true,
	})
	if err != nil {
		return nil, err
	}

	// Thanos shipper requires at least 1 external label to be set. For this reason,
	// we set the tenant ID as external label and we'll filter it out when reading
	// the series from the storage.
	l := labels.Labels{
		{
			Name:  cortex_tsdb.TenantIDExternalLabel,
			Value: userID,
		},
	}

	// Create a new shipper for this database
	if i.cfg.TSDBConfig.ShipInterval > 0 {
		s := shipper.New(util.Logger, nil, udir, &Bucket{userID, i.TSDBState.bucket}, func() labels.Labels { return l }, metadata.ReceiveSource)
		i.done.Add(1)
		go func() {
			defer i.done.Done()
			if err := runutil.Repeat(i.cfg.TSDBConfig.ShipInterval, i.quit, func() error {
				if uploaded, err := s.Sync(context.Background()); err != nil {
					level.Warn(util.Logger).Log("err", err, "uploaded", uploaded)
				}
				return nil
			}); err != nil {
				level.Warn(util.Logger).Log("err", err)
			}
		}()
	}

	return db, nil
}

func (i *Ingester) closeAllTSDB() {
	i.userStatesMtx.Lock()

	wg := &sync.WaitGroup{}
	wg.Add(len(i.TSDBState.dbs))

	// Concurrently close all users TSDB
	for userID, db := range i.TSDBState.dbs {
		userID := userID

		go func(db *tsdb.DB) {
			defer wg.Done()

			if err := db.Close(); err != nil {
				level.Warn(util.Logger).Log("msg", "unable to close TSDB", "err", err, "user", userID)
				return
			}

			// Now that the TSDB has been closed, we should remove it from the
			// set of open ones. This lock acquisition doesn't deadlock with the
			// outer one, because the outer one is released as soon as all go
			// routines are started.
			i.userStatesMtx.Lock()
			delete(i.TSDBState.dbs, userID)
			i.userStatesMtx.Unlock()
		}(db)
	}

	// Wait until all Close() completed
	i.userStatesMtx.Unlock()
	wg.Wait()
}

// openExistingTSDB walks the user tsdb dir, and opens a tsdb for each user. This may start a WAL replay, so we limit the number of
// concurrently opening TSDB.
func (i *Ingester) openExistingTSDB(ctx context.Context) error {
	level.Info(util.Logger).Log("msg", "opening existing TSDBs")
	wg := &sync.WaitGroup{}
	openGate := gate.New(i.cfg.TSDBConfig.MaxTSDBOpeningConcurrencyOnStartup)

	err := filepath.Walk(i.cfg.TSDBConfig.Dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return filepath.SkipDir
		}

		// Skip root dir and all other files
		if path == i.cfg.TSDBConfig.Dir || !info.IsDir() {
			return nil
		}

		// Top level directories are assumed to be user TSDBs
		userID := info.Name()
		f, err := os.Open(path)
		if err != nil {
			level.Error(util.Logger).Log("msg", "unable to open user TSDB dir", "err", err, "user", userID, "path", path)
			return filepath.SkipDir
		}
		defer f.Close()

		// If the dir is empty skip it
		if _, err := f.Readdirnames(1); err != nil {
			if err != io.EOF {
				level.Error(util.Logger).Log("msg", "unable to read TSDB dir", "err", err, "user", userID, "path", path)
			}

			return filepath.SkipDir
		}

		// Limit the number of TSDB's opening concurrently. Start blocks until there's a free spot available or the context is cancelled.
		if err := openGate.Start(ctx); err != nil {
			return err
		}

		wg.Add(1)
		go func(userID string) {
			defer wg.Done()
			defer openGate.Done()
			db, err := i.createTSDB(userID)
			if err != nil {
				level.Error(util.Logger).Log("msg", "unable to open user TSDB", "err", err, "user", userID)
				return
			}

			// Add the database to the map of user databases
			i.userStatesMtx.Lock()
			i.TSDBState.dbs[userID] = db
			i.userStatesMtx.Unlock()

		}(userID)

		return filepath.SkipDir // Don't descend into directories
	})

	// Wait for all opening routines to finish
	wg.Wait()
	if err != nil {
		level.Error(util.Logger).Log("msg", "error while opening existing TSDBs")
	} else {
		level.Info(util.Logger).Log("msg", "successfully opened existing TSDBs")
	}
	return err
}
