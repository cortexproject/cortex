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
	q                  util.PriorityQueue
	lastPolled         time.Time
	pollMutex          sync.RWMutex

	quit chan struct{}
	wait sync.WaitGroup
}

// Run polls the source of configurations for changes.
func (s *scheduler) Run(interval time.Duration) {
	s.wait.Add(1)
	defer s.wait.Done()
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			err := s.updateConfigs(time.Now())
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
	s.addWorkItem(item.Defer(s.evaluationInterval))
	// XXX: If another older item appears while we are sleeping, we won't
	// notice it. Can't figure out how to do this without having a buffered
	// channel & thus introducing arbitrary blocking points.
	if item.scheduled.After(now) {
		time.Sleep(item.scheduled.Sub(now))
	}
	return &item
}
