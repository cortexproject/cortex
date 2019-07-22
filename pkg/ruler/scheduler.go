package ruler

import (
	"context"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/ruler/store"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	timeLogFormat = "2006-01-02T15:04:05"
)

var (
	totalConfigs = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "scheduler_configs_total",
		Help:      "How many user configs the scheduler knows about.",
	})
	totalRuleGroups = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "scheduler_groups_total",
		Help:      "How many rule groups the scheduler is currently evaluating",
	})
	evalLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "group_evaluation_latency_seconds",
		Help:      "How far behind the target time each rule group executed.",
		Buckets:   []float64{.025, .05, .1, .25, .5, 1, 2.5, 5, 10, 25, 60},
	})
	iterationsMissed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "rule_group_iterations_missed_total",
		Help:      "The total number of rule group evaluations missed due to slow rule group evaluation.",
	}, []string{"user"})
)

type workItem struct {
	userID    string
	groupID   string
	hash      uint32
	group     *wrappedGroup
	scheduled time.Time

	done chan struct{}
}

// Key implements ScheduledItem
func (w workItem) Key() string {
	return w.userID + ":" + w.groupID
}

// Scheduled implements ScheduledItem
func (w workItem) Scheduled() time.Time {
	return w.scheduled
}

func (w workItem) String() string {
	return fmt.Sprintf("%s:%s:%d@%s", w.userID, w.groupID, len(w.group.Rules()), w.scheduled.Format(timeLogFormat))
}

type userConfig struct {
	done  chan struct{}
	id    string
	rules []store.RuleGroup
}

type groupFactory func(context.Context, store.RuleGroup) (*wrappedGroup, error)

type scheduler struct {
	poller             store.RulePoller
	evaluationInterval time.Duration // how often we re-evaluate each rule set
	q                  *SchedulingQueue

	pollInterval time.Duration // how often we check for new config

	cfgs    map[string]userConfig // all rules for all users
	groupFn groupFactory          // function to create a new group
	sync.RWMutex
	done chan struct{}
}

// newScheduler makes a new scheduler.
func newScheduler(poller store.RulePoller, evaluationInterval, pollInterval time.Duration, groupFn groupFactory) *scheduler {
	return &scheduler{
		poller:             poller,
		evaluationInterval: evaluationInterval,
		pollInterval:       pollInterval,
		q:                  NewSchedulingQueue(clockwork.NewRealClock()),
		cfgs:               map[string]userConfig{},
		groupFn:            groupFn,

		done: make(chan struct{}),
	}
}

// Run polls the source of configurations for changes.
func (s *scheduler) Run() {
	level.Debug(util.Logger).Log("msg", "scheduler started")

	err := s.updateConfigs(context.TODO())
	if err != nil {
		level.Error(util.Logger).Log("msg", "scheduler: error updating rule groups", "err", err)
	}

	ticker := time.NewTicker(s.pollInterval)
	for {
		select {
		case <-ticker.C:
			err := s.updateConfigs(context.TODO())
			if err != nil {
				level.Warn(util.Logger).Log("msg", "scheduler: error updating configs", "err", err)
			}
		case <-s.done:
			ticker.Stop()
			level.Debug(util.Logger).Log("msg", "scheduler config polling stopped")
			return
		}
	}
}

func (s *scheduler) Stop() {
	s.poller.Stop()
	close(s.done)
	s.q.Close()
	level.Debug(util.Logger).Log("msg", "scheduler stopped")
}

func (s *scheduler) updateConfigs(ctx context.Context) error {
	cfgs, err := s.poller.PollRules(ctx)
	if err != nil {
		return err
	}

	for user, cfg := range cfgs {
		s.addUserConfig(ctx, user, cfg)
	}

	totalConfigs.Set(float64(len(s.cfgs)))
	return nil
}

func (s *scheduler) addUserConfig(ctx context.Context, userID string, rgs []store.RuleGroup) {
	level.Info(util.Logger).Log("msg", "scheduler: updating rules for user", "user_id", userID, "num_groups", len(rgs))

	// create a new userchan for rulegroups of this user
	userChan := make(chan struct{})

	ringHasher := fnv.New32a()
	workItems := []workItem{}
	evalTime := s.determineEvalTime(userID)

	for _, rg := range rgs {
		level.Debug(util.Logger).Log("msg", "scheduler: updating rules for user and group", "user_id", userID, "group", rg.ID())
		grp, err := s.groupFn(ctx, rg)
		if err != nil {
			level.Error(util.Logger).Log("msg", "scheduler: failed to create group for user", "user_id", userID, "group", rg.ID(), "err", err)
			return
		}

		ringHasher.Reset()
		_, err = ringHasher.Write([]byte(rg.ID()))
		if err != nil {
			level.Error(util.Logger).Log("msg", "scheduler: failed to create group for user", "user_id", userID, "group", rg.ID(), "err", err)
			return
		}

		hash := ringHasher.Sum32()
		workItems = append(workItems, workItem{userID, rg.ID(), hash, grp, evalTime, userChan})
	}

	s.updateUserConfig(ctx, userConfig{
		id:    userID,
		rules: rgs,
		done:  userChan,
	})

	for _, i := range workItems {
		s.addWorkItem(i)
	}

	totalRuleGroups.Add(float64(len(workItems)))
}

func (s *scheduler) updateUserConfig(ctx context.Context, cfg userConfig) {
	// Retrieve any previous configuration and update to the new configuration
	s.Lock()
	curr, exists := s.cfgs[cfg.id]
	s.cfgs[cfg.id] = cfg
	s.Unlock()

	if exists {
		close(curr.done) // If a previous configuration exists, ensure it is closed
	}
}

func (s *scheduler) determineEvalTime(userID string) time.Time {
	now := time.Now()
	hasher := fnv.New64a()
	return computeNextEvalTime(hasher, now, float64(s.evaluationInterval.Nanoseconds()), userID)
}

// computeNextEvalTime Computes when a user's rules should be next evaluated, based on how far we are through an evaluation cycle
func computeNextEvalTime(hasher hash.Hash64, now time.Time, intervalNanos float64, userID string) time.Time {
	// Compute how far we are into the current evaluation cycle
	currentEvalCyclePoint := math.Mod(float64(now.UnixNano()), intervalNanos)

	hasher.Reset()
	_, err := hasher.Write([]byte(userID))
	if err != nil {
		// if an error occurs just return the current time plus a minute
		return now.Add(time.Minute)
	}
	offset := math.Mod(
		// We subtract our current point in the cycle to cause the entries
		// before 'now' to wrap around to the end.
		// We don't want this to come out negative, so we add the interval to it
		float64(hasher.Sum64())+intervalNanos-currentEvalCyclePoint,
		intervalNanos)
	return now.Add(time.Duration(int64(offset)))
}

func (s *scheduler) addWorkItem(i workItem) {
	select {
	case <-s.done:
		level.Debug(util.Logger).Log("msg", "scheduler: work item not added, scheduler stoped", "item", i)
		return
	default:
		// The queue is keyed by userID+groupID, so items for existing userID+groupID will be replaced.
		s.q.Enqueue(i)
		level.Debug(util.Logger).Log("msg", "scheduler: work item added", "item", i)
	}
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

	// Record the latency of the items evaluation here
	latency := time.Since(item.scheduled)
	evalLatency.Observe(latency.Seconds())
	level.Debug(util.Logger).Log("msg", "sheduler: returning item", "item", item, "latency", latency.String())

	return &item
}

// workItemDone marks the given item as being ready to be rescheduled.
func (s *scheduler) workItemDone(i workItem) {
	select {
	case <-i.done:
		// Unschedule the work item
		level.Debug(util.Logger).Log("msg", "scheduler: work item dropped", "item", i)
		return
	default:
		// If the evaluation of the item took longer than it's evaluation interval, skip to the next valid interval
		// and record any evaluation misses. This must be differentiated from lateness due to scheduling which is
		// caused by the overall workload, not the result of latency within a single rule group.
		missed := (time.Since(i.scheduled) / s.evaluationInterval) - 1
		if missed > 0 {
			level.Warn(util.Logger).Log("msg", "scheduler: work item missed evaluation", "item", i)
			iterationsMissed.WithLabelValues(i.userID).Add(float64(missed))
		}

		i.scheduled = i.scheduled.Add((missed + 1) * s.evaluationInterval)
		level.Debug(util.Logger).Log("msg", "scheduler: work item rescheduled", "item", i, "time", i.scheduled.Format(timeLogFormat))
		s.addWorkItem(i)
	}
}
