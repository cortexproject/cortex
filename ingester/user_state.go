package ingester

import (
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
	"golang.org/x/net/context"

	"github.com/weaveworks/cortex/user"
	"github.com/weaveworks/cortex/util"
)

type userStates struct {
	mtx              sync.RWMutex
	states           map[string]*userState
	rateUpdatePeriod time.Duration
}

type userState struct {
	userID          string
	fpLocker        *fingerprintLocker
	fpToSeries      *seriesMap
	mapper          *fpMapper
	index           *invertedIndex
	ingestedSamples *ewmaRate
}

func newUserStates(rateUpdatePeriod time.Duration) userStates {
	return userStates{
		states:           map[string]*userState{},
		rateUpdatePeriod: rateUpdatePeriod,
	}
}

func (us *userStates) cp() map[string]*userState {
	us.mtx.Lock()
	states := make(map[string]*userState, len(us.states))
	for id, state := range us.states {
		states[id] = state
	}
	us.mtx.Unlock()
	return states
}

func (us *userStates) gc() {
	us.mtx.Lock()
	for id, state := range us.states {
		if state.fpToSeries.length() == 0 {
			delete(us.states, id)
		}
	}
	us.mtx.Unlock()
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
	userID, err := user.GetID(ctx)
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
	userID, err := user.GetID(ctx)
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
		fp, series = state.unlockedGet(metric)
	}
	us.mtx.RUnlock()
	if ok {
		return state, fp, series, nil
	}

	us.mtx.Lock()
	defer us.mtx.Unlock()
	state = us.unlockedGetOrCreate(userID)
	fp, series = state.unlockedGet(metric)
	return state, fp, series, nil
}

func (us *userStates) unlockedGetOrCreate(userID string) *userState {
	state, ok := us.states[userID]
	if !ok {
		state = &userState{
			userID:          userID,
			fpToSeries:      newSeriesMap(),
			fpLocker:        newFingerprintLocker(16),
			index:           newInvertedIndex(),
			ingestedSamples: newEWMARate(0.2, us.rateUpdatePeriod),
		}
		state.mapper = newFPMapper(state.fpToSeries)
		us.states[userID] = state
	}
	return state
}

func (u *userState) unlockedGet(metric model.Metric) (model.Fingerprint, *memorySeries) {
	rawFP := metric.FastFingerprint()
	u.fpLocker.Lock(rawFP)
	fp := u.mapper.mapFP(rawFP, metric)
	if fp != rawFP {
		u.fpLocker.Unlock(rawFP)
		u.fpLocker.Lock(fp)
	}

	series, ok := u.fpToSeries.get(fp)
	if ok {
		return fp, series
	}

	series = newMemorySeries(metric)
	u.fpToSeries.put(fp, series)
	u.index.add(metric, fp)
	return fp, series
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
			if _, ok := series.metric[filter.Name]; ok {
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
