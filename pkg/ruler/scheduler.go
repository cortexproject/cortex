package ruler

import (
	"context"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
	totalRuleGroups = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "scheduler_groups_total",
		Help:      "How many rule groups the scheduler is currently evaluating",
	})
	scheduleFailures = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "scheduler_update_failures_total",
		Help:      "Number of failures when updating rule groups",
	})
)

type workItem struct {
	userID    string
	groupName string
	hash      uint32
	group     *wrappedGroup
	scheduled time.Time

	done chan struct{}
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
	return workItem{w.userID, w.groupName, w.hash, w.group, w.scheduled.Add(interval), w.done}
}

func (w workItem) String() string {
	return fmt.Sprintf("%s:%s:%d@%s", w.userID, w.groupName, len(w.group.Rules()), w.scheduled.Format(timeLogFormat))
}

type userConfig struct {
	done  chan struct{}
	id    string
	rules []RuleGroup
}

type groupFactory func(context.Context, RuleGroup) (*wrappedGroup, error)

type scheduler struct {
	store              RulePoller
	evaluationInterval time.Duration // how often we re-evaluate each rule set
	q                  *SchedulingQueue

	pollInterval time.Duration // how often we check for new config

	cfgs    map[string]userConfig // all rules for all users
	groupFn groupFactory          // function to create a new group
	sync.RWMutex
	done chan struct{}
}

// newScheduler makes a new scheduler.
func newScheduler(store RulePoller, evaluationInterval, pollInterval time.Duration, groupFn groupFactory) *scheduler {
	return &scheduler{
		store:              store,
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
	close(s.done)
	s.q.Close()
	level.Debug(util.Logger).Log("msg", "scheduler stopped")
}

func (s *scheduler) updateConfigs(ctx context.Context) error {
	cfgs, err := s.store.PollRules(ctx)
	if err != nil {
		return err
	}

	for user, cfg := range cfgs {
		s.addUserConfig(ctx, user, cfg)
	}

	return nil
}

func (s *scheduler) addUserConfig(ctx context.Context, userID string, rgs []RuleGroup) {
	level.Info(util.Logger).Log("msg", "scheduler: updating rules for user", "user_id", userID, "num_groups", len(rgs))

	// create a new userchan for rulegroups of this user
	userChan := make(chan struct{})

	ringHasher := fnv.New32a()
	workItems := []workItem{}
	evalTime := s.determineEvalTime(userID)

	for _, rg := range rgs {
		level.Debug(util.Logger).Log("msg", "scheduler: updating rules for user and group", "user_id", userID, "group", rg.Name())
		grp, err := s.groupFn(ctx, rg)
		if err != nil {
			level.Error(util.Logger).Log("msg", "scheduler: failed to create group for user", "user_id", userID, "group", rg.Name(), "err", err)
			return
		}

		ringHasher.Reset()
		ringHasher.Write([]byte(rg.Name()))
		hash := ringHasher.Sum32()
		workItems = append(workItems, workItem{userID, rg.Name(), hash, grp, evalTime, userChan})
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

	return
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
	hasher.Write([]byte(userID))
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
		// The queue is keyed by userID+groupName, so items for existing userID+groupName will be replaced.
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
		next := i.Defer(s.evaluationInterval)
		level.Debug(util.Logger).Log("msg", "scheduler: work item rescheduled", "item", i, "time", next.scheduled.Format(timeLogFormat))
		s.addWorkItem(next)
	}
}
