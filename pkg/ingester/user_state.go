package ingester

import (
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"golang.org/x/net/context"

	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/util"
	"github.com/weaveworks/cortex/pkg/util/extract"
)

var (
	memSeries = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ingester_memory_series",
		Help: "The current number of series in memory.",
	})
	memUsers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ingester_memory_users",
		Help: "The current number of users in memory.",
	})
	memSeriesCreatedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_memory_series_created_total",
		Help: "The total number of series that were created per user.",
	}, []string{"user"})
	memSeriesRemovedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_memory_series_removed_total",
		Help: "The total number of series that were removed per user.",
	}, []string{"user"})
)

type userStates struct {
	states sync.Map
	cfg    *UserStatesConfig
}

type userState struct {
	userID          string
	fpLocker        *fingerprintLocker
	fpToSeries      *seriesMap
	mapper          *fpMapper
	index           *invertedIndex
	ingestedSamples *ewmaRate

	seriesInMetricMtx sync.Mutex
	seriesInMetric    map[model.LabelValue]int
}

// UserStatesConfig configures userStates properties.
type UserStatesConfig struct {
	RateUpdatePeriod   time.Duration
	MaxSeriesPerUser   int
	MaxSeriesPerMetric int
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *UserStatesConfig) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.RateUpdatePeriod, "ingester.rate-update-period", 15*time.Second, "Period with which to update the per-user ingestion rates.")
	f.IntVar(&cfg.MaxSeriesPerUser, "ingester.max-series-per-user", 5000000, "Maximum number of active series per user.")
	f.IntVar(&cfg.MaxSeriesPerMetric, "ingester.max-series-per-metric", 50000, "Maximum number of active series per metric name.")
}

func newUserStates(cfg *UserStatesConfig) *userStates {
	return &userStates{
		cfg: cfg,
	}
}

func (us *userStates) cp() map[string]*userState {
	states := map[string]*userState{}
	us.states.Range(func(key, value interface{}) bool {
		states[key.(string)] = value.(*userState)
		return true
	})
	return states
}

func (us *userStates) gc() {
	us.states.Range(func(key, value interface{}) bool {
		state := value.(*userState)
		if state.fpToSeries.length() == 0 {
			us.states.Delete(key)
		}
		return true
	})
}

func (us *userStates) updateRates() {
	us.states.Range(func(key, value interface{}) bool {
		state := value.(*userState)
		state.ingestedSamples.tick()
		return true
	})
}

func (us *userStates) get(userID string) (*userState, bool) {
	state, ok := us.states.Load(userID)
	if !ok {
		return nil, ok
	}
	return state.(*userState), ok
}

func (us *userStates) getViaContext(ctx context.Context) (*userState, bool, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("no user id")
	}
	state, ok := us.get(userID)
	return state, ok, nil
}

func (us *userStates) getOrCreateSeries(ctx context.Context, metric model.Metric) (*userState, model.Fingerprint, *memorySeries, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("no user id")
	}

	state, ok := us.get(userID)
	if !ok {
		// Speculatively create a userState object and try to store it
		// in the map.  Another goroutine may have got there before
		// us, in which case this userState will be discarded
		state = &userState{
			userID:          userID,
			fpToSeries:      newSeriesMap(),
			fpLocker:        newFingerprintLocker(16),
			index:           newInvertedIndex(),
			ingestedSamples: newEWMARate(0.2, us.cfg.RateUpdatePeriod),
			seriesInMetric:  map[model.LabelValue]int{},
		}
		state.mapper = newFPMapper(state.fpToSeries)
		stored, ok := us.states.LoadOrStore(userID, state)
		if !ok {
			memUsers.Inc()
		}
		state = stored.(*userState)
	}

	fp, series, err := state.getSeries(metric, us.cfg)
	return state, fp, series, err
}

func (u *userState) getSeries(metric model.Metric, cfg *UserStatesConfig) (model.Fingerprint, *memorySeries, error) {
	rawFP := metric.FastFingerprint()
	u.fpLocker.Lock(rawFP)
	fp := u.mapper.mapFP(rawFP, metric)
	if fp != rawFP {
		u.fpLocker.Unlock(rawFP)
		u.fpLocker.Lock(fp)
	}

	series, ok := u.fpToSeries.get(fp)
	if ok {
		return fp, series, nil
	}

	// There's theoretically a relatively harmless race here if multiple
	// goroutines get the length of the series map at the same time, then
	// all proceed to add a new series. This is likely not worth addressing,
	// as this should happen rarely (all samples from one push are added
	// serially), and the overshoot in allowed series would be minimal.
	if u.fpToSeries.length() >= cfg.MaxSeriesPerUser {
		u.fpLocker.Unlock(fp)
		return fp, nil, httpgrpc.Errorf(http.StatusTooManyRequests, "per-user series limit (%d) exceeded", cfg.MaxSeriesPerUser)
	}

	metricName, err := extract.MetricNameFromMetric(metric)
	if err != nil {
		u.fpLocker.Unlock(fp)
		return fp, nil, err
	}

	if !u.canAddSeriesFor(metricName, cfg) {
		u.fpLocker.Unlock(fp)
		return fp, nil, httpgrpc.Errorf(http.StatusTooManyRequests, "per-metric series limit (%d) exceeded for %s: %s", cfg.MaxSeriesPerMetric, metricName, metric)
	}

	util.Event().Log("msg", "new series", "userID", u.userID, "fp", fp, "series", metric)
	memSeriesCreatedTotal.WithLabelValues(u.userID).Inc()
	memSeries.Inc()

	series = newMemorySeries(metric)
	u.fpToSeries.put(fp, series)
	u.index.add(metric, fp)

	return fp, series, nil
}

func (u *userState) canAddSeriesFor(metric model.LabelValue, cfg *UserStatesConfig) bool {
	u.seriesInMetricMtx.Lock()
	defer u.seriesInMetricMtx.Unlock()

	if u.seriesInMetric[metric] >= cfg.MaxSeriesPerMetric {
		return false
	}
	u.seriesInMetric[metric]++
	return true
}

func (u *userState) removeSeries(fp model.Fingerprint, metric model.Metric) {
	u.fpToSeries.del(fp)
	u.index.delete(metric, fp)

	metricName, err := extract.MetricNameFromMetric(metric)
	if err != nil {
		// Series without a metric name should never be able to make it into
		// the ingester's memory storage.
		panic(err)
	}

	u.seriesInMetricMtx.Lock()
	defer u.seriesInMetricMtx.Unlock()

	u.seriesInMetric[metricName]--
	if u.seriesInMetric[metricName] == 0 {
		delete(u.seriesInMetric, metricName)
	}

	memSeriesRemovedTotal.WithLabelValues(u.userID).Inc()
	memSeries.Dec()
}

// forSeriesMatching passes all series matching the given matchers to the provided callback.
// Deals with locking and the quirks of zero-length matcher values.
func (u *userState) forSeriesMatching(allMatchers []*labels.Matcher, maxSeries int, callback func(model.Fingerprint, *memorySeries) error) error {
	filters, matchers := util.SplitFiltersAndMatchers(allMatchers)
	fps := u.index.lookup(matchers)
	if len(fps) > maxSeries {
		return httpgrpc.Errorf(http.StatusRequestEntityTooLarge, "exceeded maximum number of series in a query")
	}

	// fps is sorted, lock them in order to prevent deadlocks
outer:
	for _, fp := range fps {
		u.fpLocker.Lock(fp)
		series, ok := u.fpToSeries.get(fp)
		if !ok {
			u.fpLocker.Unlock(fp)
			continue
		}

		for _, filter := range filters {
			if !filter.Matches(string(series.metric[model.LabelName(filter.Name)])) {
				u.fpLocker.Unlock(fp)
				continue outer
			}
		}

		err := callback(fp, series)
		u.fpLocker.Unlock(fp)
		if err != nil {
			return err
		}
	}

	return nil
}
