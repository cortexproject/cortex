package ruler

import (
	"context"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/jonboulle/clockwork"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/rules"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/weaveworks/common/instrument"
)

var backoffConfig = util.BackoffConfig{
	// Backoff for loading initial configuration set.
	MinBackoff: 100 * time.Millisecond,
	MaxBackoff: 2 * time.Second,
}

const (
	timeLogFormat = "2006-01-02T15:04:05"
)

var (
	totalConfigs = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "configs",
		Help:      "How many configs the scheduler knows about.",
	})
	configUpdates = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "scheduler_config_updates_total",
		Help:      "How many config updates the scheduler has made.",
	})
	configsRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "configs_request_duration_seconds",
		Help:      "Time spent requesting configs.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"operation", "status_code"})
)

func init() {
	prometheus.MustRegister(configsRequestDuration)
	prometheus.MustRegister(totalConfigs)
	prometheus.MustRegister(configUpdates)
}

type workItem struct {
	userID     string
	groupName  string
	group      *group
	scheduled  time.Time
	generation configs.ID // a monotonically increasing number used to spot out of date work items
}

// Key implements ScheduledItem
func (w workItem) Key() string {
	return w.userID + ":" + w.groupName
}

// Scheduled implements ScheduledItem
func (w workItem) Scheduled() time.Time {
	return w.scheduled
}

// Defer returns a work item with updated rules, rescheduled to a later time.
func (w workItem) Defer(interval time.Duration) workItem {
	return workItem{w.userID, w.groupName, w.group, w.scheduled.Add(interval), w.generation}
}

func (w workItem) String() string {
	return fmt.Sprintf("%s:%s:%d@%s", w.userID, w.groupName, len(w.group.Rules()), w.scheduled.Format(timeLogFormat))
}

type userConfig struct {
	rules      map[string][]rules.Rule
	generation configs.ID // a monotonically increasing number used to spot out of date work items
}

type groupFactory func(userID string, groupName string, rls []rules.Rule) (*group, error)

type scheduler struct {
	rulesAPI           RulesAPI
	evaluationInterval time.Duration // how often we re-evaluate each rule set
	q                  *SchedulingQueue

	pollInterval time.Duration // how often we check for new config

	cfgs         map[string]userConfig // all rules for all users
	latestConfig configs.ID            // # of last update received from config
	groupFn      groupFactory          // function to create a new group
	sync.RWMutex

	stop chan struct{}
	done chan struct{}
}

// newScheduler makes a new scheduler.
func newScheduler(rulesAPI RulesAPI, evaluationInterval, pollInterval time.Duration, groupFn groupFactory) scheduler {
	return scheduler{
		rulesAPI:           rulesAPI,
		evaluationInterval: evaluationInterval,
		pollInterval:       pollInterval,
		q:                  NewSchedulingQueue(clockwork.NewRealClock()),
		cfgs:               map[string]userConfig{},
		groupFn:            groupFn,

		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
}

// Run polls the source of configurations for changes.
func (s *scheduler) Run() {
	level.Debug(util.Logger).Log("msg", "scheduler started")
	defer close(s.done)
	// Load initial set of all configurations before polling for new ones.
	s.addNewConfigs(time.Now(), s.loadAllConfigs())
	ticker := time.NewTicker(s.pollInterval)
	for {
		select {
		case now := <-ticker.C:
			err := s.updateConfigs(now)
			if err != nil {
				level.Warn(util.Logger).Log("msg", "scheduler: error updating configs", "err", err)
			}
		case <-s.stop:
			ticker.Stop()
			return
		}
	}
}

func (s *scheduler) Stop() {
	close(s.stop)
	s.q.Close()
	<-s.done
	level.Debug(util.Logger).Log("msg", "scheduler stopped")
}

// Load the full set of configurations from the server, retrying with backoff
// until we can get them.
func (s *scheduler) loadAllConfigs() map[string]configs.VersionedRulesConfig {
	backoff := util.NewBackoff(context.Background(), backoffConfig)
	for {
		cfgs, err := s.poll()
		if err == nil {
			level.Debug(util.Logger).Log("msg", "scheduler: initial configuration load", "num_configs", len(cfgs))
			return cfgs
		}
		level.Warn(util.Logger).Log("msg", "scheduler: error fetching all configurations, backing off", "err", err)
		backoff.Wait()
	}
}

func (s *scheduler) updateConfigs(now time.Time) error {
	cfgs, err := s.poll()
	if err != nil {
		return err
	}
	s.addNewConfigs(now, cfgs)
	return nil
}

// poll the configuration server. Not re-entrant.
func (s *scheduler) poll() (map[string]configs.VersionedRulesConfig, error) {
	s.Lock()
	configID := s.latestConfig
	s.Unlock()
	var cfgs map[string]configs.VersionedRulesConfig
	err := instrument.TimeRequestHistogram(context.Background(), "Configs.GetConfigs", configsRequestDuration, func(_ context.Context) error {
		var err error
		cfgs, err = s.rulesAPI.GetConfigs(configID) // Warning: this will produce an incorrect result if the configID ever overflows
		return err
	})
	if err != nil {
		level.Warn(util.Logger).Log("msg", "scheduler: configs server poll failed", "err", err)
		return nil, err
	}
	s.Lock()
	s.latestConfig = getLatestConfigID(cfgs, configID)
	s.Unlock()
	return cfgs, nil
}

// computeNextEvalTime Computes when a user's rules should be next evaluated, based on how far we are through an evaluation cycle
func (s *scheduler) computeNextEvalTime(hasher hash.Hash64, now time.Time, userID string) time.Time {
	intervalNanos := float64(s.evaluationInterval.Nanoseconds())
	// Compute how far we are into the current evaluation cycle
	currentEvalCyclePoint := math.Mod(float64(now.UnixNano()), intervalNanos)

	hasher.Reset()
	hasher.Write([]byte(userID))
	offset := math.Mod(
		// We subtract our current point in the cycle to cause the entries
		// before 'now' to wrap around to the end.
		// We don't want this to come out negative, so we add the interval to it
		float64(hasher.Sum64())+intervalNanos-currentEvalCyclePoint,
		intervalNanos)
	return now.Add(time.Duration(int64(offset)))
}

func (s *scheduler) addNewConfigs(now time.Time, cfgs map[string]configs.VersionedRulesConfig) {
	// TODO: instrument how many configs we have, both valid & invalid.
	level.Debug(util.Logger).Log("msg", "adding configurations", "num_configs", len(cfgs))
	hasher := fnv.New64a()
	s.Lock()
	generation := s.latestConfig
	s.Unlock()

	for userID, config := range cfgs {
		s.addUserConfig(now, hasher, generation, userID, config)
	}

	configUpdates.Add(float64(len(cfgs)))
	s.Lock()
	lenCfgs := len(s.cfgs)
	s.Unlock()
	totalConfigs.Set(float64(lenCfgs))
}

func (s *scheduler) addUserConfig(now time.Time, hasher hash.Hash64, generation configs.ID, userID string, config configs.VersionedRulesConfig) {
	rulesByGroup, err := config.Config.Parse()
	if err != nil {
		// XXX: This means that if a user has a working configuration and
		// they submit a broken one, we'll keep processing the last known
		// working configuration, and they'll never know.
		// TODO: Provide a way of deleting / cancelling recording rules.
		level.Warn(util.Logger).Log("msg", "scheduler: invalid Cortex configuration", "user_id", userID, "err", err)
		return
	}

	level.Info(util.Logger).Log("msg", "scheduler: updating rules for user", "user_id", userID, "num_groups", len(rulesByGroup), "is_deleted", config.IsDeleted())
	s.Lock()
	// if deleted remove from map, otherwise - update map
	if config.IsDeleted() {
		delete(s.cfgs, userID)
	} else {
		s.cfgs[userID] = userConfig{rules: rulesByGroup, generation: generation}
	}
	s.Unlock()
	if !config.IsDeleted() {
		evalTime := s.computeNextEvalTime(hasher, now, userID)
		workItems := []workItem{}
		for group, rules := range rulesByGroup {
			level.Debug(util.Logger).Log("msg", "scheduler: updating rules for user and group", "user_id", userID, "group", group, "num_rules", len(rules))
			g, err := s.groupFn(userID, group, rules)
			if err != nil {
				// XXX: similarly to above if a user has a working configuration and
				// for some reason we cannot create a group for the new one we'll use
				// the last known working configuration
				level.Warn(util.Logger).Log("msg", "scheduler: failed to create group for user", "user_id", userID, "group", group, "err", err)
				return
			}
			workItems = append(workItems, workItem{userID, group, g, evalTime, generation})
		}

		for _, i := range workItems {
			s.addWorkItem(i)
		}
	}
}

func (s *scheduler) addWorkItem(i workItem) {
	// The queue is keyed by userID+groupName, so items for existing userID+groupName will be replaced.
	s.q.Enqueue(i)
	level.Debug(util.Logger).Log("msg", "scheduler: work item added", "item", i)
}

// Get the next scheduled work item, blocking if none.
//
// Call `workItemDone` on the returned item to indicate that it is ready to be
// rescheduled.
func (s *scheduler) nextWorkItem() *workItem {
	level.Debug(util.Logger).Log("msg", "scheduler: work item requested, pending...")
	// TODO: We are blocking here on the second Dequeue event. Write more
	// tests for the scheduling queue.
	op := s.q.Dequeue()
	if op == nil {
		level.Info(util.Logger).Log("msg", "queue closed; no more work items")
		return nil
	}
	item := op.(workItem)
	level.Debug(util.Logger).Log("msg", "scheduler: work item granted", "item", item)
	return &item
}

// workItemDone marks the given item as being ready to be rescheduled.
func (s *scheduler) workItemDone(i workItem) {
	s.Lock()
	config, found := s.cfgs[i.userID]
	var currentRules []rules.Rule
	if found {
		currentRules = config.rules[i.groupName]
	}
	s.Unlock()
	if !found || len(currentRules) == 0 || i.generation < config.generation {
		// Warning: this test will produce an incorrect result if the generation ever overflows
		level.Debug(util.Logger).Log("msg", "scheduler: stopping item", "user_id", i.userID, "group", i.groupName, "found", found, "len", len(currentRules))
		return
	}
	next := i.Defer(s.evaluationInterval)
	level.Debug(util.Logger).Log("msg", "scheduler: work item rescheduled", "item", i, "time", next.scheduled.Format(timeLogFormat))
	s.addWorkItem(next)
}
