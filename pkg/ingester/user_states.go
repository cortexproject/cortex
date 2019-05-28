package ingester

import (
	"fmt"
	"sync"

	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/user"
	"golang.org/x/net/context"
)

const metricCounterShards = 128

var (
	memUsers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ingester_memory_users",
		Help: "The current number of users in memory.",
	})
)

type userStates struct {
	states sync.Map
	limits *validation.Overrides
	cfg    Config
}

type metricCounterShard struct {
	mtx sync.Mutex
	m   map[string]int
}

func newUserStates(limits *validation.Overrides, cfg Config) *userStates {
	return &userStates{
		limits: limits,
		cfg:    cfg,
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
		state.ingestedAPISamples.tick()
		state.ingestedRuleSamples.tick()
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

func (us *userStates) getOrCreate(ctx context.Context) (*userState, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, fmt.Errorf("no user id")
	}
	state, ok := us.get(userID)
	if !ok {
		state := newUserState(us.cfg, userID, us.limits)
		stored, ok := us.states.LoadOrStore(userID, state)
		if !ok {
			memUsers.Inc()
		}
		state = stored.(*userState)
	}

	return state, nil
}
