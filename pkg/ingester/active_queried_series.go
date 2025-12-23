package ingester

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/util/services"
)

// ActiveQueriedSeries tracks unique queried series using time-windowed HyperLogLog.
// It maintains multiple HyperLogLog sketches in a circular buffer, one per time window.
// It can track up to the maximum configured window duration and query for specific window durations.
type ActiveQueriedSeries struct {
	windowDuration time.Duration
	numWindows     int
	windows        []*hllWindow
	currentWindow  int
	sampleRate     float64
	rng            *rand.Rand
	logger         log.Logger
	mu             sync.RWMutex

	// Cache for merged HLL results per query window to avoid re-merging unchanged windows
	cache map[time.Duration]*mergedCacheEntry
}

// mergedCacheEntry caches a merged HLL result for a specific query window duration.
type mergedCacheEntry struct {
	mergedHLL         *hyperloglog.Sketch
	lastMergedEndTime time.Time // endTime of the latest window included in the merge
}

// hllWindow represents a single time window with its HyperLogLog sketch.
type hllWindow struct {
	hll       *hyperloglog.Sketch
	startTime time.Time
	endTime   time.Time
}

// NewActiveQueriedSeries creates a new ActiveQueriedSeries tracker.
func NewActiveQueriedSeries(windowsToQuery []time.Duration, windowDuration time.Duration, sampleRate float64, logger log.Logger) *ActiveQueriedSeries {
	// Determine the maximum window duration to track
	// windowDurations are assumed to be validated (> 0 and non-empty) by Config.Validate
	maxWindow := time.Duration(0)
	for _, d := range windowsToQuery {
		if d > maxWindow {
			maxWindow = d
		}
	}

	// Calculate number of windows needed for the maximum duration
	numWindows := int(maxWindow / windowDuration)
	if numWindows <= 0 {
		numWindows = 1 // At least 1 window
	}

	windows := make([]*hllWindow, numWindows)
	now := time.Now()
	for i := range numWindows {
		windows[i] = &hllWindow{
			hll:       hyperloglog.New14(),
			startTime: now.Add(time.Duration(i-numWindows) * windowDuration),
			endTime:   now.Add(time.Duration(i-numWindows+1) * windowDuration),
		}
	}

	return &ActiveQueriedSeries{
		windowDuration: windowDuration,
		numWindows:     numWindows,
		windows:        windows,
		currentWindow:  numWindows - 1, // Start with the most recent window
		sampleRate:     sampleRate,
		rng:            rand.New(rand.NewSource(time.Now().UnixNano())),
		logger:         logger,
		cache:          make(map[time.Duration]*mergedCacheEntry),
	}
}

// SampleRequest returns whether this request should be sampled based on sampling.
// This should be called before collecting hashes to avoid unnecessary work.
func (a *ActiveQueriedSeries) SampleRequest() bool {
	if a.sampleRate >= 1.0 {
		return true // 100% sampling, always track
	}
	return a.rng.Float64() <= a.sampleRate
}

// UpdateSeriesBatch adds multiple series hashes to the current active window in a single batch.
// This is more efficient than calling UpdateSeries multiple times as it:
// - Only acquires the lock once
// - Only rotates windows once
// Note: This method should be called from the centralized worker goroutines.
// Sampling should be checked before calling this method.
func (a *ActiveQueriedSeries) UpdateSeriesBatch(hashes []uint64, now time.Time) {
	if len(hashes) == 0 {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Rotate windows if needed
	a.rotateWindowsLocked(now)

	// Add all hashes to current window
	window := a.windows[a.currentWindow]
	if now.After(window.startTime) && now.Before(window.endTime) || now.Equal(window.startTime) {
		for _, hash := range hashes {
			window.hll.InsertHash(hash)
		}
	}
}

// GetSeriesQueried returns the estimated cardinality of active queried series
// by merging all non-expired windows within the specified time range.
// If queryWindow is 0, it uses the full tracking period.
// This method uses caching to efficiently merge only new windows when possible.
func (a *ActiveQueriedSeries) GetSeriesQueried(now time.Time, queryWindow time.Duration) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Determine the cutoff time based on query window or full tracking period
	var cutoffTime time.Time
	if queryWindow > 0 {
		cutoffTime = now.Add(-queryWindow)
	} else {
		cutoffTime = now.Add(-time.Duration(a.numWindows) * a.windowDuration)
	}

	// Get the current window (which is actively being updated and should not be cached)
	currentWindow := a.windows[a.currentWindow]
	currentWindowEndTime := currentWindow.endTime

	// The latest completed window is the one before the current window in the circular buffer.
	// This is guaranteed to be in range because query windows are validated to be larger than windowDuration.
	prevWindowIndex := (a.currentWindow - 1 + a.numWindows) % a.numWindows
	prevWindow := a.windows[prevWindowIndex]

	// Determine the latest completed window endTime
	// The previous window should be completed and within the query range
	// Note: We use >= for cutoffTime to handle the case where queryWindow equals windowDuration
	var latestCompletedEndTime time.Time
	if (prevWindow.endTime.After(cutoffTime) || prevWindow.endTime.Equal(cutoffTime)) && prevWindow.endTime.Before(now) {
		latestCompletedEndTime = prevWindow.endTime
	}
	// If previous window is not in range (shouldn't happen with validation, but handle gracefully),
	// latestCompletedEndTime will be zero, which will trigger a full merge

	// Check if we have a cached result for this query window
	cached, hasCache := a.cache[queryWindow]

	// Check if cache is still valid (no new completed windows since last merge)
	if hasCache && !latestCompletedEndTime.After(cached.lastMergedEndTime) {
		// Cache is valid - merge cached result with current window
		merged := cached.mergedHLL.Clone()

		// Always merge the current window (which is actively being updated)
		if currentWindow.endTime.After(cutoffTime) &&
			(currentWindow.startTime.Before(now) || currentWindow.startTime.Equal(now)) {
			if err := merged.Merge(currentWindow.hll); err != nil {
				if a.logger != nil {
					level.Error(a.logger).Log("msg", "failed to merge HyperLogLog sketches", "err", err)
				}
				return 0, fmt.Errorf("failed to merge HyperLogLog sketches: %w", err)
			}
		}

		return merged.Estimate(), nil
	}

	// Cache needs update - incrementally merge only new completed windows if cache exists
	if hasCache && latestCompletedEndTime.After(cached.lastMergedEndTime) {
		// Clone the cached merged HLL
		merged := cached.mergedHLL.Clone()

		// Find and merge only new completed windows (excluding current window)
		for _, window := range a.windows {
			// Include windows that:
			// 1. End after the cutoff time
			// 2. End before 'now' (completed windows only)
			// 3. End after the last merged endTime (new completed windows)
			// 4. Not the current window
			if window.endTime.After(cutoffTime) &&
				window.endTime.Before(now) &&
				window.endTime.After(cached.lastMergedEndTime) &&
				window.endTime != currentWindowEndTime {
				if err := merged.Merge(window.hll); err != nil {
					if a.logger != nil {
						level.Error(a.logger).Log("msg", "failed to merge HyperLogLog sketches", "err", err)
					}
					return 0, fmt.Errorf("failed to merge HyperLogLog sketches: %w", err)
				}
			}
		}

		// Update cache with new merged result (only completed windows)
		a.cache[queryWindow] = &mergedCacheEntry{
			mergedHLL:         merged,
			lastMergedEndTime: latestCompletedEndTime,
		}

		// Always merge the current window before returning
		if currentWindow.endTime.After(cutoffTime) &&
			(currentWindow.startTime.Before(now) || currentWindow.startTime.Equal(now)) {
			if err := merged.Merge(currentWindow.hll); err != nil {
				if a.logger != nil {
					level.Error(a.logger).Log("msg", "failed to merge HyperLogLog sketches", "err", err)
				}
				return 0, fmt.Errorf("failed to merge HyperLogLog sketches: %w", err)
			}
		}

		return merged.Estimate(), nil
	}

	// No cache or cache invalid - do full merge of completed windows only
	var merged *hyperloglog.Sketch
	activeWindows := 0
	var latestCompletedEndTimeForCache time.Time

	for _, window := range a.windows {
		// Include only completed windows (endTime < now) that are in range
		// Exclude the current window which is still being updated
		if window.endTime.After(cutoffTime) &&
			window.endTime.Before(now) &&
			window.endTime != currentWindowEndTime {
			// Window is within the query range and completed, merge it
			if merged == nil {
				// Clone the first window to avoid modifying it
				merged = window.hll.Clone()
			} else {
				// Merge into the existing merged sketch
				if err := merged.Merge(window.hll); err != nil {
					// Log the error and return 0 with the error
					if a.logger != nil {
						level.Error(a.logger).Log("msg", "failed to merge HyperLogLog sketches", "err", err)
					}
					return 0, fmt.Errorf("failed to merge HyperLogLog sketches: %w", err)
				}
			}
			activeWindows++
			// Track the latest completed endTime
			if window.endTime.After(latestCompletedEndTimeForCache) {
				latestCompletedEndTimeForCache = window.endTime
			}
		}
	}

	// Cache the merged result (only completed windows)
	if merged != nil {
		a.cache[queryWindow] = &mergedCacheEntry{
			mergedHLL:         merged,
			lastMergedEndTime: latestCompletedEndTimeForCache,
		}
	}

	// Always merge the current window before returning
	if currentWindow.endTime.After(cutoffTime) &&
		(currentWindow.startTime.Before(now) || currentWindow.startTime.Equal(now)) {
		if merged == nil {
			// No completed windows, start with current window
			merged = currentWindow.hll.Clone()
		} else {
			// Merge current window into completed windows
			if err := merged.Merge(currentWindow.hll); err != nil {
				if a.logger != nil {
					level.Error(a.logger).Log("msg", "failed to merge HyperLogLog sketches", "err", err)
				}
				return 0, fmt.Errorf("failed to merge HyperLogLog sketches: %w", err)
			}
		}
	}

	if merged == nil {
		return 0, nil
	}

	return merged.Estimate(), nil
}

// Purge rotates expired windows and clears them.
func (a *ActiveQueriedSeries) Purge(now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.rotateWindowsLocked(now)
}

// rotateWindowsLocked ensures we're using the correct window for the current time.
// Must be called with write lock held.
func (a *ActiveQueriedSeries) rotateWindowsLocked(now time.Time) {
	currentWindow := a.windows[a.currentWindow]

	// Check if current window has expired
	if now.After(currentWindow.endTime) || now.Equal(currentWindow.endTime) {
		// Calculate how many windows we need to advance
		timeSinceWindowStart := now.Sub(currentWindow.startTime)
		windowsToAdvance := int(timeSinceWindowStart / a.windowDuration)

		if windowsToAdvance >= a.numWindows {
			// All windows are expired, reset all
			for i := range a.numWindows {
				windowStart := now.Add(time.Duration(i-a.numWindows+1) * a.windowDuration)
				a.windows[i] = &hllWindow{
					hll:       hyperloglog.New14(),
					startTime: windowStart,
					endTime:   windowStart.Add(a.windowDuration),
				}
			}
			a.currentWindow = a.numWindows - 1
			// Invalidate cache since all windows were reset
			a.cache = make(map[time.Duration]*mergedCacheEntry)
		} else {
			// Advance by the required number of windows
			for i := range windowsToAdvance {
				// Move to next window
				a.currentWindow = (a.currentWindow + 1) % a.numWindows

				// Reset the window we're about to use
				windowStart := now.Add(time.Duration(i-windowsToAdvance+1) * a.windowDuration)
				a.windows[a.currentWindow] = &hllWindow{
					hll:       hyperloglog.New14(),
					startTime: windowStart,
					endTime:   windowStart.Add(a.windowDuration),
				}
			}
			// Invalidate cache since windows were rotated
			a.cache = make(map[time.Duration]*mergedCacheEntry)
		}
	}
}

// clear resets all windows (used for testing and cleanup).
func (a *ActiveQueriedSeries) clear() {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now()
	for i := range a.numWindows {
		windowStart := now.Add(time.Duration(i-a.numWindows+1) * a.windowDuration)
		a.windows[i] = &hllWindow{
			hll:       hyperloglog.New14(),
			startTime: windowStart,
			endTime:   windowStart.Add(a.windowDuration),
		}
	}
	a.currentWindow = a.numWindows - 1
	// Clear cache
	a.cache = make(map[time.Duration]*mergedCacheEntry)
}

// activeQueriedSeriesUpdate represents an update batch for a specific ActiveQueriedSeries instance.
type activeQueriedSeriesUpdate struct {
	activeQueriedSeries *ActiveQueriedSeries
	hashes              []uint64
	now                 time.Time
}

// ActiveQueriedSeriesService manages centralized worker goroutines for processing active queried series updates.
// It implements the services.Service interface to handle lifecycle management.
type ActiveQueriedSeriesService struct {
	*services.BasicService

	updateChan          chan activeQueriedSeriesUpdate
	workers             sync.WaitGroup
	logger              log.Logger
	numWorkers          int
	droppedUpdatesTotal prometheus.Counter
}

// NewActiveQueriedSeriesService creates a new ActiveQueriedSeriesService service.
func NewActiveQueriedSeriesService(logger log.Logger, registerer prometheus.Registerer) *ActiveQueriedSeriesService {
	// Cap at 8 workers to avoid excessive goroutines
	numWorkers := min(runtime.NumCPU()/2, 8)

	m := &ActiveQueriedSeriesService{
		updateChan: make(chan activeQueriedSeriesUpdate, 10000), // Buffered channel to avoid blocking
		logger:     logger,
		numWorkers: numWorkers,
		droppedUpdatesTotal: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_active_queried_series_updates_dropped_total",
			Help: "The total number of active queried series updates that were dropped due to full channel.",
		}),
	}

	m.BasicService = services.NewBasicService(m.starting, m.running, m.stopping)
	return m
}

// starting initializes the worker goroutines.
func (m *ActiveQueriedSeriesService) starting(ctx context.Context) error {
	// Start worker goroutines
	for w := 0; w < m.numWorkers; w++ {
		m.workers.Add(1)
		go m.processUpdates(ctx)
	}
	level.Info(m.logger).Log("msg", "started active queried series worker goroutines", "workers", m.numWorkers)
	return nil
}

// running keeps the service running until context is canceled.
func (m *ActiveQueriedSeriesService) running(ctx context.Context) error {
	// Wait for context to be canceled (service is stopping)
	<-ctx.Done()
	return nil
}

// stopping waits for all worker goroutines to finish.
func (m *ActiveQueriedSeriesService) stopping(_ error) error {
	// Close the channel to signal workers to stop
	close(m.updateChan)
	// Wait for all workers to finish
	m.workers.Wait()
	return nil
}

// processUpdates is a worker goroutine that processes updates from the update channel.
func (m *ActiveQueriedSeriesService) processUpdates(ctx context.Context) {
	defer m.workers.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case update, ok := <-m.updateChan:
			if !ok {
				// Channel closed, exit
				return
			}
			// Process the update synchronously
			update.activeQueriedSeries.UpdateSeriesBatch(update.hashes, update.now)
		}
	}
}

// UpdateSeriesBatch sends an update to the update channel for processing.
// This method is non-blocking and will drop updates if the channel is full.
func (m *ActiveQueriedSeriesService) UpdateSeriesBatch(activeQueriedSeries *ActiveQueriedSeries, hashes []uint64, now time.Time, userID string) {
	if len(hashes) == 0 {
		return
	}

	// Non-blocking send to centralized update channel
	select {
	case m.updateChan <- activeQueriedSeriesUpdate{
		activeQueriedSeries: activeQueriedSeries,
		hashes:              hashes,
		now:                 now,
	}:
	// Successfully queued
	default:
		// Channel is full, drop the update to avoid blocking
		// This is acceptable as we're using probabilistic data structures (HLL)
		m.droppedUpdatesTotal.Inc()
		level.Warn(m.logger).Log("msg", "active queried series update channel full, dropping batch", "batch_size", len(hashes), "user", userID)
	}
}
