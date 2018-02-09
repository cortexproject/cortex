package ruler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/jonboulle/clockwork"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/rules"

	"github.com/weaveworks/common/instrument"
	"github.com/weaveworks/cortex/pkg/configs"
	"github.com/weaveworks/cortex/pkg/util"
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
	userID    string
	rules     []rules.Rule
	scheduled time.Time
}

// Key implements ScheduledItem
func (w workItem) Key() string {
	return w.userID
}

// Scheduled implements ScheduledItem
func (w workItem) Scheduled() time.Time {
	return w.scheduled
}

// Defer returns a work item with updated rules, rescheduled to a later time.
func (w workItem) Defer(interval time.Duration, currentRules []rules.Rule) workItem {
	return workItem{w.userID, currentRules, w.scheduled.Add(interval)}
}

func (w workItem) String() string {
	return fmt.Sprintf("%s:%d@%s", w.userID, len(w.rules), w.scheduled.Format(timeLogFormat))
}

type scheduler struct {
	rulesAPI           RulesAPI
	evaluationInterval time.Duration // how often we re-evaluate each rule set
	q                  *SchedulingQueue

	pollInterval time.Duration // how often we check for new config

	cfgs         map[string][]rules.Rule // all rules for all users
	latestConfig configs.ID              // # of last update received from config
	sync.RWMutex

	stop chan struct{}
	done chan struct{}
}

// newScheduler makes a new scheduler.
func newScheduler(rulesAPI RulesAPI, evaluationInterval, pollInterval time.Duration) scheduler {
	return scheduler{
		rulesAPI:           rulesAPI,
		evaluationInterval: evaluationInterval,
		pollInterval:       pollInterval,
		q:                  NewSchedulingQueue(clockwork.NewRealClock()),
		cfgs:               map[string][]rules.Rule{},

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
		cfgs, err = s.rulesAPI.GetConfigs(configID)
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

func (s *scheduler) addNewConfigs(now time.Time, cfgs map[string]configs.VersionedRulesConfig) {
	// TODO: instrument how many configs we have, both valid & invalid.
	level.Debug(util.Logger).Log("msg", "adding configurations", "num_configs", len(cfgs))
	for userID, config := range cfgs {
		rules, err := config.Config.Parse()
		if err != nil {
			// XXX: This means that if a user has a working configuration and
			// they submit a broken one, we'll keep processing the last known
			// working configuration, and they'll never know.
			// TODO: Provide a way of deleting / cancelling recording rules.
			level.Warn(util.Logger).Log("msg", "scheduler: invalid Cortex configuration", "user_id", userID, "err", err)
			continue
		}
		level.Info(util.Logger).Log("msg", "scheduler: updating rules for user", "user_id", userID, "num_rules", len(rules), "is_deleted", config.IsDeleted())
		s.Lock()
		// if deleted remove from map, otherwise - update map
		if config.IsDeleted() {
			delete(s.cfgs, userID)
		} else {
			s.cfgs[userID] = rules
		}
		s.Unlock()
		s.addWorkItem(workItem{userID, rules, now})
	}
	configUpdates.Add(float64(len(cfgs)))
	s.Lock()
	lenCfgs := len(s.cfgs)
	s.Unlock()
	totalConfigs.Set(float64(lenCfgs))
}

func (s *scheduler) addWorkItem(i workItem) {
	// The queue is keyed by user ID, so items for existing user IDs will be replaced.
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
	currentRules, found := s.cfgs[i.userID]
	s.Unlock()
	if !found {
		level.Debug(util.Logger).Log("msg", "scheduler: no more work configured for user", "user_id", i.userID)
		return
	}
	next := i.Defer(s.evaluationInterval, currentRules)
	level.Debug(util.Logger).Log("msg", "scheduler: work item rescheduled", "item", i, "time", next.scheduled.Format(timeLogFormat))
	s.addWorkItem(next)
}
