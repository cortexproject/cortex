package ingester

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
	"golang.org/x/net/context"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/util"
)

type userStates struct {
	mtx    sync.RWMutex
	states map[string]*userState
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
	f.IntVar(&cfg.MaxSeriesPerUser, "ingester.max-series-per-user", DefaultMaxSeriesPerUser, "Maximum number of active series per user.")
	f.IntVar(&cfg.MaxSeriesPerMetric, "ingester.max-series-per-metric", DefaultMaxSeriesPerMetric, "Maximum number of active series per metric name.")
}

func newUserStates(cfg *UserStatesConfig) *userStates {
	return &userStates{
		states: map[string]*userState{},
		cfg:    cfg,
	}
}

func (us *userStates) cp() map[string]*userState {
	us.mtx.RLock()
	defer us.mtx.RUnlock()
	states := make(map[string]*userState, len(us.states))
	for id, state := range us.states {
		states[id] = state
	}
	return states
}

func (us *userStates) gc() {
	us.mtx.Lock()
	defer us.mtx.Unlock()

	for id, state := range us.states {
		if state.fpToSeries.length() == 0 {
			delete(us.states, id)
		}
	}
}

func (us *userStates) updateRates() {
	us.mtx.RLock()
	defer us.mtx.RUnlock()

	for _, state := range us.states {
		state.ingestedSamples.tick()
	}
}

func (us *userStates) numUsers() int {
	us.mtx.RLock()
	defer us.mtx.RUnlock()

	return len(us.states)
}

func (us *userStates) numSeries() int {
	us.mtx.RLock()
	defer us.mtx.RUnlock()

	numSeries := 0
	for _, state := range us.states {
		numSeries += state.fpToSeries.length()
	}
	return numSeries
}

func (us *userStates) get(userID string) (*userState, bool) {
	us.mtx.RLock()
	state, ok := us.states[userID]
	us.mtx.RUnlock()
	return state, ok
}

func (us *userStates) getOrCreate(ctx context.Context) (*userState, error) {
	userID, err := user.Extract(ctx)
	if err != nil {
		return nil, fmt.Errorf("no user id")
	}

	us.mtx.RLock()
	state, ok := us.states[userID]
	us.mtx.RUnlock()
	if ok {
		return state, nil
	}

	us.mtx.Lock()
	defer us.mtx.Unlock()
	return us.unlockedGetOrCreate(userID), nil
}

func (us *userStates) getOrCreateSeries(ctx context.Context, metric model.Metric) (*userState, model.Fingerprint, *memorySeries, error) {
	userID, err := user.Extract(ctx)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("no user id")
	}

	var (
		state  *userState
		ok     bool
		fp     model.Fingerprint
		series *memorySeries
	)

	us.mtx.RLock()
	state, ok = us.states[userID]
	if ok {
		fp, series, err = state.unlockedGet(metric, us.cfg)
		if err != nil {
			us.mtx.RUnlock()
			return nil, fp, nil, err
		}
	}
	us.mtx.RUnlock()
	if ok {
		return state, fp, series, nil
	}

	us.mtx.Lock()
	defer us.mtx.Unlock()
	state = us.unlockedGetOrCreate(userID)
	fp, series, err = state.unlockedGet(metric, us.cfg)
	return state, fp, series, err
}

func (us *userStates) unlockedGetOrCreate(userID string) *userState {
	state, ok := us.states[userID]
	if !ok {
		state = &userState{
			userID:          userID,
			fpToSeries:      newSeriesMap(),
			fpLocker:        newFingerprintLocker(16),
			index:           newInvertedIndex(),
			ingestedSamples: newEWMARate(0.2, us.cfg.RateUpdatePeriod),
			seriesInMetric:  map[model.LabelValue]int{},
		}
		state.mapper = newFPMapper(state.fpToSeries)
		us.states[userID] = state
	}
	return state
}

func (u *userState) unlockedGet(metric model.Metric, cfg *UserStatesConfig) (model.Fingerprint, *memorySeries, error) {
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
		return fp, nil, util.ErrUserSeriesLimitExceeded
	}

	metricName, err := util.ExtractMetricNameFromMetric(metric)
	if err != nil {
		u.fpLocker.Unlock(fp)
		return fp, nil, err
	}

	if !u.canAddSeriesFor(metricName, cfg) {
		u.fpLocker.Unlock(fp)
		return fp, nil, util.ErrMetricSeriesLimitExceeded
	}

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

	metricName, err := util.ExtractMetricNameFromMetric(metric)
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
}

// forSeriesMatching passes all series matching the given matchers to the provided callback.
// Deals with locking and the quirks of zero-length matcher values.
func (u *userState) forSeriesMatching(allMatchers []*metric.LabelMatcher, callback func(model.Fingerprint, *memorySeries) error) error {
	filters, matchers := util.SplitFiltersAndMatchers(allMatchers)
	fps := u.index.lookup(matchers)

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
			if !filter.Match(series.metric[filter.Name]) {
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
