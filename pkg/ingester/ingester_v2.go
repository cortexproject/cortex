package ingester

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	lbls "github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/weaveworks/common/user"
	"golang.org/x/net/context"
	old_ctx "golang.org/x/net/context"
)

// V2Config is the config for IngesterV2
type V2Config struct {
	Enabled      bool                // indicates that the prometheus block storage should be used
	dbs          map[string]*tsdb.DB // tsdb sharded by userID
	bucket       objstore.Bucket
	TSDBDir      string
	BlockRanges  time.Duration
	Retention    time.Duration
	ShipInterval time.Duration
	S3Endpoint   string
	S3Bucket     string
	S3Secret     string
	S3Key        string
	S3Insecure   bool // useful for test clusters without https
}

// NewV2 returns a new Ingester that uses prometheus block storage instead of chunk storage
func NewV2(cfg Config, clientConfig client.Config, limits *validation.Overrides, chunkStore ChunkStore, registerer prometheus.Registerer) (*Ingester, error) {

	var bkt *s3.Bucket
	s3Cfg := s3.Config{
		Bucket:    cfg.V2.S3Bucket,
		Endpoint:  cfg.V2.S3Endpoint,
		AccessKey: cfg.V2.S3Key,
		SecretKey: cfg.V2.S3Secret,
		Insecure:  cfg.V2.S3Insecure,
	}
	var err error
	bkt, err = s3.NewBucketWithConfig(util.Logger, s3Cfg, "cortex")
	if err != nil {
		return nil, err
	}

	i := &Ingester{
		cfg:          cfg,
		clientConfig: clientConfig,

		metrics: newIngesterMetrics(registerer),

		limits:     limits,
		chunkStore: chunkStore,
		userStates: newUserStates(limits, cfg),
		quit:       make(chan struct{}),

		V2: V2Config{
			Enabled:      cfg.V2.Enabled,
			dbs:          make(map[string]*tsdb.DB),
			bucket:       bkt,
			BlockRanges:  cfg.V2.BlockRanges,
			Retention:    cfg.V2.Retention,
			TSDBDir:      cfg.V2.TSDBDir,
			ShipInterval: cfg.V2.ShipInterval,
		},
	}

	i.lifecycler, err = ring.NewLifecycler(cfg.LifecyclerConfig, i, "ingester")
	if err != nil {
		return nil, err
	}

	return i, nil
}

// v2Push adds metrics to a block
func (i *Ingester) v2Push(ctx old_ctx.Context, req *client.WriteRequest) (*client.WriteResponse, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, fmt.Errorf("no user id")
	}

	db, err := i.getOrCreateTSDB(userID)
	if err != nil {
		return nil, err
	}

	// Walk the samples, appending them to the users database
	app := db.Appender()
	for _, ts := range req.Timeseries {
		for _, s := range ts.Samples {
			if i.stopped {
				return nil, fmt.Errorf("ingester stopping")
			}
			lset := make(lbls.Labels, len(ts.Labels))
			for i := range ts.Labels {
				lset[i] = lbls.Label{
					Name:  ts.Labels[i].Name,
					Value: ts.Labels[i].Value,
				}
			}
			if _, err := app.Add(lset, s.TimestampMs, s.Value); err != nil {
				if err := app.Rollback(); err != nil {
					level.Warn(util.Logger).Log("failed to rollback on error", "userID", userID, "err", err)
				}
				return nil, err
			}
		}
	}
	if err := app.Commit(); err != nil {
		return nil, err
	}

	client.ReuseSlice(req.Timeseries)

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

	db, err := i.getOrCreateTSDB(userID)
	if err != nil {
		return nil, fmt.Errorf("failed to find/create user db")
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

	db, err := i.getOrCreateTSDB(userID)
	if err != nil {
		return nil, fmt.Errorf("failed to find/create user db")
	}

	through := time.Now()
	from := through.Add(-i.V2.Retention)
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

	db, err := i.getOrCreateTSDB(userID)
	if err != nil {
		return nil, fmt.Errorf("failed to find/create user db")
	}

	through := time.Now()
	from := through.Add(-i.V2.Retention)
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
	db, _ := i.V2.dbs[userID]
	return db
}

func (i *Ingester) getOrCreateTSDB(userID string) (*tsdb.DB, error) {
	db := i.getTSDB(userID)
	if db == nil {
		i.userStatesMtx.Lock()
		defer i.userStatesMtx.Unlock()

		// Check again for DB in the event it was created in-between locks
		var ok bool
		db, ok = i.V2.dbs[userID]
		if !ok {

			udir := i.userDir(userID)

			// Create a new user database
			var err error
			db, err = tsdb.Open(udir, util.Logger, nil, &tsdb.Options{
				RetentionDuration: uint64(i.V2.Retention / time.Millisecond),
				BlockRanges:       []int64{int64(i.V2.BlockRanges / time.Millisecond)},
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
			s := shipper.New(util.Logger, nil, udir, &Bucket{userID, i.V2.bucket}, func() lbls.Labels { return l }, metadata.ReceiveSource)
			i.done.Add(1)
			go func() {
				defer i.done.Done()
				runutil.Repeat(i.V2.ShipInterval, i.quit, func() error {
					if uploaded, err := s.Sync(context.Background()); err != nil {
						level.Warn(util.Logger).Log("err", err, "uploaded", uploaded)
					}
					return nil
				})
			}()

			i.V2.dbs[userID] = db
		}
	}

	return db, nil
}

func (i *Ingester) userDir(userID string) string { return filepath.Join(i.V2.TSDBDir, userID) }
