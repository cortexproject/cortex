package ruler

import (
	"sync"
	"time"

	"github.com/prometheus/common/log"
	"github.com/prometheus/prometheus/rules"

	"github.com/weaveworks/cortex/util"
)

const (
	skewCorrection = 1 * time.Minute
	// Backoff for loading initial configuration set.
	minBackoff = 100 * time.Millisecond
	maxBackoff = 2 * time.Second
)

type workItem struct {
	userID    string
	rules     []rules.Rule
	scheduled time.Time
}

// Key implements Op
func (w workItem) Key() string {
	return w.userID
}

// Priority implements Op
func (w workItem) Priority() int64 {
	return -w.scheduled.Unix()
}

// Defer returns a copy of this work item, rescheduled to a later time.
func (w workItem) Defer(interval time.Duration) workItem {
	return workItem{w.userID, w.rules, w.scheduled.Add(interval)}
}

type scheduler struct {
	configsAPI         configsAPI // XXX: Maybe make this an interface ConfigSource or similar.
	evaluationInterval time.Duration
	q                  *util.PriorityQueue

	pollInterval time.Duration
	lastPolled   time.Time
	pollMutex    sync.RWMutex

	quit chan struct{}
	wait sync.WaitGroup
}

// newScheduler makes a new scheduler.
func newScheduler(configsAPI configsAPI, evaluationInterval, pollInterval time.Duration) scheduler {
	return scheduler{
		configsAPI:         configsAPI,
		evaluationInterval: evaluationInterval,
		pollInterval:       pollInterval,
		q:                  util.NewPriorityQueue(),
	}
}

// Run polls the source of configurations for changes.
func (s *scheduler) Run() {
	s.wait.Add(1)
	defer s.wait.Done()
	// Load initial set of all configurations before polling for new ones.
	s.addNewConfigs(time.Now(), s.loadAllConfigs())
	ticker := time.NewTicker(s.pollInterval)
	for {
		select {
		case now := <-ticker.C:
			err := s.updateConfigs(now)
			if err != nil {
				log.Warnf("Error updating configs: %v", err)
			}
		case <-s.quit:
			ticker.Stop()
		}
	}
}

func (s *scheduler) Stop() {
	close(s.quit)
	s.q.Close()
	s.wait.Wait()
}

// Load the full set of configurations from the server, retrying with backoff
// until we can get them.
func (s *scheduler) loadAllConfigs() map[string]cortexConfig {
	backoff := minBackoff
	dawnOfTime := time.Unix(0, 0)
	for {
		cfgs, err := s.poll(dawnOfTime)
		if err == nil {
			return cfgs
		}
		log.Warnf("Error fetching all configurations, backing off: %v", err)
		time.Sleep(backoff)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (s *scheduler) updateConfigs(now time.Time) error {
	cfgs, err := s.poll(now)
	if err != nil {
		return err
	}
	s.addNewConfigs(now, cfgs)
	return nil
}

func (s *scheduler) poll(now time.Time) (map[string]cortexConfig, error) {
	s.pollMutex.RLock()
	interval := now.Add(-skewCorrection).Sub(s.lastPolled)
	s.pollMutex.RUnlock()
	cfgs, err := s.configsAPI.getOrgConfigs(interval)
	if err != nil {
		return nil, err
	}
	s.pollMutex.Lock()
	s.lastPolled = now
	s.pollMutex.Unlock()
	return cfgs, nil
}

func (s *scheduler) addNewConfigs(now time.Time, cfgs map[string]cortexConfig) {
	for userID, config := range cfgs {
		rules, err := config.GetRules()
		if err != nil {
			// TODO: instrument this
			// XXX: This means that if a user has a working configuration and
			// they submit a broken one, we'll keep processing the last known
			// working configuration.
			log.Warnf("Invalid Cortex configuration for %v", userID)
			continue
		}

		// XXX: New / changed configs go to the back of the queue.
		s.addWorkItem(workItem{userID, rules, now})
	}
}

func (s *scheduler) addWorkItem(i workItem) {
	s.q.Enqueue(i)
}

// Get the next scheduled work item, blocking if none
func (s *scheduler) nextWorkItem(now time.Time) *workItem {
	op := s.q.Dequeue()
	if op == nil {
		return nil
	}
	item := op.(workItem)
	// XXX: If the item takes longer than `evaluationInterval` to be
	// processed, it will get processed concurrently.
	s.addWorkItem(item.Defer(s.evaluationInterval))
	// XXX: If another older item appears while we are sleeping, we won't
	// notice it. Can't figure out how to do this without having a buffered
	// channel & thus introducing arbitrary blocking points.
	if item.scheduled.After(now) {
		time.Sleep(item.scheduled.Sub(now))
	}
	return &item
}
