package ingester

import (
	"fmt"
	"net/http"
	"time"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	lbls "github.com/prometheus/prometheus/tsdb/labels"
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

	i.lifecycler, err = ring.NewLifecycler(cfg.LifecyclerConfig, i, "ingester")
	if err != nil {
		return nil, err
	}

	// Init the limter and instantiate the user states which depend on it
	i.limiter = NewSeriesLimiter(limits, i.lifecycler, cfg.LifecyclerConfig.RingConfig.ReplicationFactor, cfg.ShardByAllLabels)
	i.userStates = newUserStates(i.limiter, cfg)

	// Now that user states have been created, we can start the lifecycler
	i.lifecycler.Start()

	return i, nil
}

// v2Push adds metrics to a block
func (i *Ingester) v2Push(ctx old_ctx.Context, req *client.WriteRequest) (*client.WriteResponse, error) {
	var lastPartialErr error

	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, fmt.Errorf("no user id")
	}

	db, err := i.getOrCreateTSDB(userID)
	if err != nil {
		return nil, err
	}

	// Keep track of some stats which are tracked only if the samples will be
	// successfully committed
	succeededSamplesCount := 0
	failedSamplesCount := 0

	// Walk the samples, appending them to the users database
	app := db.Appender()
	for _, ts := range req.Timeseries {
		// Convert labels to the type expected by TSDB
		lset := cortex_tsdb.FromLabelAdaptersToLabels(ts.Labels)

		for _, s := range ts.Samples {
			if i.stopped {
				return nil, fmt.Errorf("ingester stopping")
			}

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
				lastPartialErr = httpgrpc.Errorf(http.StatusBadRequest, err.Error())
				continue
			}

			// The error looks an issue on our side, so we should rollback
			if rollbackErr := app.Rollback(); rollbackErr != nil {
				level.Warn(util.Logger).Log("failed to rollback on error", "userID", userID, "err", rollbackErr)
			}

			return nil, err
		}
	}
	if err := app.Commit(); err != nil {
		return nil, err
	}

	// Increment metrics only if the samples have been successfully committed.
	// If the code didn't reach this point, it means that we returned an error
	// which will be converted into an HTTP 5xx and the client should/will retry.
	i.metrics.ingestedSamples.Add(float64(succeededSamplesCount))
	i.metrics.ingestedSamplesFail.Add(float64(failedSamplesCount))

	client.ReuseSlice(req.Timeseries)

	return &client.WriteResponse{}, lastPartialErr
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

	// two different versions of the labels package are being used, converting matchers must be done
	var converted []lbls.Matcher
	for _, m := range matchers {
		switch m.Type {
		case labels.MatchEqual:
			converted = append(converted, lbls.NewEqualMatcher(m.Name, m.Value))
		case labels.MatchNotEqual:
			converted = append(converted, lbls.Not(lbls.NewEqualMatcher(m.Name, m.Value)))
		case labels.MatchRegexp:
			rm, err := lbls.NewRegexpMatcher(m.Name, "^(?:"+m.Value+")$")
			if err != nil {
				return nil, err
			}
			converted = append(converted, rm)
		case labels.MatchNotRegexp:
			rm, err := lbls.NewRegexpMatcher(m.Name, "^(?:"+m.Value+")$")
			if err != nil {
				return nil, err
			}
			converted = append(converted, lbls.Not(rm))
		}
	}
	ss, err := q.Select(converted...)
	if err != nil {
		return nil, err
	}

	result := &client.QueryResponse{}
	for ss.Next() {
		series := ss.At()

		// convert labels to LabelAdapter
		var adapters []client.LabelAdapter
		for _, l := range series.Labels() {
			adapters = append(adapters, client.LabelAdapter(l))
		}
		ts := client.TimeSeries{
			Labels: adapters,
		}

		it := series.Iterator()
		for it.Next() {
			t, v := it.At()
			ts.Samples = append(ts.Samples, client.Sample{Value: v, TimestampMs: t})
		}

		result.Timeseries = append(result.Timeseries, ts)
	}

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

func (i *Ingester) getTSDB(userID string) *tsdb.DB {
	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	db, _ := i.TSDBState.dbs[userID]
	return db
}

func (i *Ingester) getOrCreateTSDB(userID string) (*tsdb.DB, error) {
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
	ingesterState := i.lifecycler.GetState()
	if ingesterState != ring.ACTIVE {
		return nil, fmt.Errorf(errTSDBCreateIncompatibleState, ingesterState)
	}

	udir := i.cfg.TSDBConfig.BlocksDir(userID)

	// Create a new user database
	var err error
	db, err = tsdb.Open(udir, util.Logger, nil, &tsdb.Options{
		RetentionDuration: uint64(i.cfg.TSDBConfig.Retention / time.Millisecond),
		BlockRanges:       i.cfg.TSDBConfig.BlockRanges.ToMillisecondRanges(),
		NoLockfile:        true,
	})
	if err != nil {
		return nil, err
	}

	// Create a new shipper for this database
	l := lbls.Labels{
		{
			Name:  "user",
			Value: userID,
		},
	}
	s := shipper.New(util.Logger, nil, udir, &Bucket{userID, i.TSDBState.bucket}, func() lbls.Labels { return l }, metadata.ReceiveSource)
	i.done.Add(1)
	go func() {
		defer i.done.Done()
		runutil.Repeat(i.cfg.TSDBConfig.ShipInterval, i.quit, func() error {
			if uploaded, err := s.Sync(context.Background()); err != nil {
				level.Warn(util.Logger).Log("err", err, "uploaded", uploaded)
			}
			return nil
		})
	}()

	i.TSDBState.dbs[userID] = db

	return db, nil
}
