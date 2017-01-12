package ruler

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/prometheus/rules"
	"github.com/weaveworks/scope/common/instrument"
	"golang.org/x/net/context"

	"github.com/weaveworks/cortex/util"
)

const (
	skewCorrection = 1 * time.Minute
	// Backoff for loading initial configuration set.
	minBackoff = 100 * time.Millisecond
	maxBackoff = 2 * time.Second
)

var (
	queueLength = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "rules_queue_length",
		Help:      "The length of the rules queue.",
	})
	blockedWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "blocked_workers",
		Help:      "How many workers are waiting on an item to be ready.",
	})
	totalConfigs = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "configs",
		Help:      "How many configs the scheduler knows about.",
	})
	configsRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "configs_request_duration_seconds",
		Help:      "Time spent requesting configs.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"operation", "status_code"})
)

func init() {
	prometheus.MustRegister(queueLength)
	prometheus.MustRegister(blockedWorkers)
	prometheus.MustRegister(configsRequestDuration)
	prometheus.MustRegister(totalConfigs)
}

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

	// All the configurations that we have. Only used for instrumentation.
	cfgs map[string]cortexConfig

	pollInterval time.Duration

	latestConfig configID
	latestMutex  *sync.RWMutex

	done       chan struct{}
	terminated chan struct{}
}

// newScheduler makes a new scheduler.
func newScheduler(configsAPI configsAPI, evaluationInterval, pollInterval time.Duration) scheduler {
	return scheduler{
		configsAPI:         configsAPI,
		evaluationInterval: evaluationInterval,
		pollInterval:       pollInterval,
		q:                  util.NewPriorityQueue(),
		cfgs:               map[string]cortexConfig{},
	}
}

// Run polls the source of configurations for changes.
func (s *scheduler) Run() {
	log.Debugf("Scheduler started")
	defer close(s.terminated)
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
		case <-s.done:
			ticker.Stop()
		}
	}
}

func (s *scheduler) Stop() {
	close(s.done)
	s.q.Close()
	<-s.terminated
	log.Debugf("Scheduler stopped")
}

// Load the full set of configurations from the server, retrying with backoff
// until we can get them.
func (s *scheduler) loadAllConfigs() map[string]cortexConfig {
	backoff := minBackoff
	for {
		cfgs, err := s.poll()
		if err == nil {
			log.Debugf("Found %d configurations in initial load", len(cfgs))
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
	cfgs, err := s.poll()
	if err != nil {
		return err
	}
	s.addNewConfigs(now, cfgs)
	return nil
}

func (s *scheduler) poll() (map[string]cortexConfig, error) {
	s.latestMutex.RLock()
	configID := s.latestConfig
	s.latestMutex.RUnlock()
	var cfgs map[string]cortexConfig
	err := instrument.TimeRequestHistogram(context.Background(), "Configs.GetOrgConfigs", configsRequestDuration, func(_ context.Context) error {
		var err error
		cfgs, err = s.configsAPI.getOrgConfigs(configID)
		return err
	})
	if err != nil {
		log.Warnf("configs server poll failed: %v", err)
		return nil, err
	}
	s.latestMutex.Lock()
	s.latestConfig = getLatestConfigID(cfgs)
	s.latestMutex.Unlock()
	return cfgs, nil
}

func (s *scheduler) addNewConfigs(now time.Time, cfgs map[string]cortexConfig) {
	// TODO: instrument how many configs we have, both valid & invalid.
	log.Debugf("Adding %d configurations", len(cfgs))
	for userID, config := range cfgs {
		rules, err := config.GetRules()
		if err != nil {
			// XXX: This means that if a user has a working configuration and
			// they submit a broken one, we'll keep processing the last known
			// working configuration.
			// TODO: Provide a way of deleting / cancelling recording rules.
			log.Warnf("Invalid Cortex configuration for %v", userID)
			continue
		}

		// XXX: New configs go to the back of the queue. Changed configs are
		// ignored because priority queue ignores repeated queueing.

		// TODO: Change config server to include updated time, so we can use
		// that rather than scheduler's understanding of now, so that we can
		// prioritise by last updated.
		s.addWorkItem(workItem{userID, rules, now})
		s.cfgs[userID] = config
	}
	totalConfigs.Set(float64(len(s.cfgs)))
}

func (s *scheduler) addWorkItem(i workItem) {
	s.q.Enqueue(i)
	queueLength.Set(float64(s.q.Length()))
}

// Get the next scheduled work item, blocking if none
func (s *scheduler) nextWorkItem(now time.Time) *workItem {
	op := s.q.Dequeue()
	if op == nil {
		log.Infof("Queue closed. No more work items.")
		return nil
	}
	queueLength.Set(float64(s.q.Length()))
	item := op.(workItem)
	// XXX: If the item takes longer than `evaluationInterval` to be
	// processed, it will get processed concurrently.
	s.addWorkItem(item.Defer(s.evaluationInterval))
	// XXX: If another older item appears while we are sleeping, we won't
	// notice it. Can't figure out how to do this without having a buffered
	// channel & thus introducing arbitrary blocking points.
	if item.scheduled.After(now) {
		log.Debugf("Work item not yet ready for processing. Blocking...")
		blockedWorkers.Inc()
		defer blockedWorkers.Dec()
		time.Sleep(item.scheduled.Sub(now))
		log.Debugf("Ready now. Unblocked.")
	}
	return &item
}
