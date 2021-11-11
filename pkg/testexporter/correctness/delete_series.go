package correctness

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/user"

	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

const deleteRequestPath = "/api/v1/admin/tsdb/delete_series"

var (
	deleteRequestCreationAttemptsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "delete_requests_creation_attempts_total",
		Help:      "Total number of delete requests creation attempts with status",
	}, []string{"status"})
	deleteRequestVerificationsSkippedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "delete_request_verification_skipped_total",
		Help:      "Total number of queries verifying delete series that were skipped",
	}, []string{"test_name"})
)

type DeleteSeriesTestConfig struct {
	deleteRequestCreationInterval time.Duration
	deleteDataForRange            time.Duration
	timeQueryStart                TimeValue
	durationQuerySince            time.Duration
	purgerAddr                    string

	PrometheusAddr string
	ExtraSelectors string
	UserID         string
}

func (cfg *DeleteSeriesTestConfig) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.deleteRequestCreationInterval, "delete-request-creation-interval", 5*time.Minute, "The interval at which delete request should be sent.")
	f.DurationVar(&cfg.deleteDataForRange, "delete-data-for-range", 2*time.Minute, "Time range for which data is deleted.")
	f.StringVar(&cfg.purgerAddr, "purger-addr", "", "Purger address to send delete requests. Keep empty to use same address as prometheus-address.")

	// By default, we only query for values from when this process started
	cfg.timeQueryStart = NewTimeValue(time.Now())
	f.Var(&cfg.timeQueryStart, "delete-series-test.test-query-start", "Minimum start date for queries")
	f.DurationVar(&cfg.durationQuerySince, "delete-series-test.test-query-since", 0, "Duration in the past to test.  Overrides -test-query-start")
}

// DeleteSeriesTest would keep deleting data for configured duration at configured interval.
// Test method would check whether we are getting expected data by eliminating deleted samples while non deleted ones stays untouched.
// For simplification it would not test samples from the start time of last sent delete request and just treat it as passed.
type DeleteSeriesTest struct {
	Case
	cfg                            DeleteSeriesTestConfig
	commonTestConfig               CommonTestConfig
	lastDeleteRequestInterval      interval
	lastDeleteRequestIntervalMutex sync.RWMutex
	quit                           chan struct{}
	wg                             sync.WaitGroup
}

func NewDeleteSeriesTest(name string, f func(time.Time) float64, cfg DeleteSeriesTestConfig, commonTestConfig CommonTestConfig) Case {
	commonTestConfig.timeQueryStart = cfg.timeQueryStart
	commonTestConfig.durationQuerySince = cfg.durationQuerySince
	test := DeleteSeriesTest{
		Case:             NewSimpleTestCase(name, f, commonTestConfig),
		cfg:              cfg,
		commonTestConfig: commonTestConfig,
		quit:             make(chan struct{}),
	}

	if cfg.purgerAddr == "" {
		test.cfg.purgerAddr = test.cfg.PrometheusAddr
	}

	test.wg.Add(1)
	go test.sendDeleteRequestLoop()
	return &test
}

func (d *DeleteSeriesTest) Stop() {
	close(d.quit)
	d.wg.Wait()
}

func (d *DeleteSeriesTest) sendDeleteRequestLoop() {
	defer d.wg.Done()
	// send a delete request as soon as we start to avoid missing creation of delete request across restarts.
	err := d.sendDeleteRequest()
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "error sending delete request", "error", err)
	}

	t := time.NewTicker(d.cfg.deleteRequestCreationInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			err := d.sendDeleteRequest()
			if err != nil {
				level.Error(util_log.Logger).Log("msg", "error sending delete request", "error", err)
			}
		case <-d.quit:
			return
		}
	}
}

func (d *DeleteSeriesTest) Test(ctx context.Context, client v1.API, selectors string, start time.Time, duration time.Duration) (bool, error) {
	log := spanlogger.FromContext(ctx)
	queryInterval := interval{start: start.Add(-duration), end: start}

	d.lastDeleteRequestIntervalMutex.RLock()
	defer d.lastDeleteRequestIntervalMutex.RUnlock()

	// we do not want to query data after the start time of last delete request sent to simplify things.
	lastDeleteRequestInterval := d.lastDeleteRequestInterval
	if !queryInterval.end.Before(lastDeleteRequestInterval.start) {
		deleteRequestVerificationsSkippedTotal.WithLabelValues(d.Name()).Inc()
		level.Info(log).Log("msg", fmt.Sprintf("skipping test for %d to %d requesting samples after last sent delete request's start time %d",
			start.Add(-duration).Unix(), start.Unix(), lastDeleteRequestInterval.end.Unix()))
		return true, nil
	}

	pairs, err := d.Query(ctx, client, selectors, start, duration)
	if err != nil {
		level.Error(log).Log("err", err)
		return false, err
	}

	nonDeletedIntervals := d.getNonDeletedIntervals(queryInterval)
	if len(nonDeletedIntervals) == 0 {
		// we are querying data covered completed by deleted interval so there should not be any sample pairs returned by the query.
		if len(pairs) != 0 {
			return false, errors.New("samples should be 0")
		}
		return true, nil
	}

	level.Debug(log).Log("start", start.Unix(), "query-start", queryInterval.start.Unix(),
		"query-end", queryInterval.end.Unix(), "non-deleted-intervals")

	verifyPairsFrom, verifyPairsTo := 0, 0
	for _, nonDeletedInterval := range nonDeletedIntervals {
		for ; verifyPairsTo < len(pairs); verifyPairsTo++ {
			pair := pairs[verifyPairsTo]
			// do not fail the test if difference is just by couple of ms or ns.
			if pair.Timestamp.Time().Before(nonDeletedInterval.start) && pair.Timestamp.Unix() != nonDeletedInterval.start.Unix() {
				level.Error(log).Log("msg", "unexpected sample", "timestamp", pair.Timestamp.Unix(), "non-deleted-interval.start", nonDeletedInterval.start.Unix(),
					"non-deleted-interval.end", nonDeletedInterval.end.Unix())
				return false, nil
			} else if pair.Timestamp.Time().After(nonDeletedInterval.end) {
				break
			}
		}

		passed := verifySamples(spanlogger.FromContext(ctx), d, pairs[verifyPairsFrom:verifyPairsTo], nonDeletedInterval.end.Sub(nonDeletedInterval.start), d.commonTestConfig)
		if !passed {
			verifyingPairs := pairs[verifyPairsFrom:verifyPairsTo]
			if len(verifyingPairs) == 0 {
				level.Error(log).Log("msg", fmt.Sprintf("expected samples from %d to %d but got 0 samples", nonDeletedInterval.start.Unix(),
					nonDeletedInterval.end.Unix()), "query start", start.Unix(), "query duration", duration)
			} else {
				level.Error(log).Log("msg", "failed to verify samples batch", "query start", start.Unix(), "query duration", duration,
					"batch length", len(verifyingPairs),
					"batch duration", nonDeletedInterval.end.Sub(nonDeletedInterval.start), "batch-start", verifyingPairs[0].Timestamp.Unix(),
					"batch-end", verifyingPairs[len(verifyingPairs)-1].Timestamp.Unix())
			}
			return false, nil
		}

		verifyPairsFrom = verifyPairsTo
	}

	return true, nil
}

func (d *DeleteSeriesTest) sendDeleteRequest() (err error) {
	// data is deleted by slicing the time by deleteRequestCreationInterval from 0 time i.e beginning of epoch
	// and doing deletion for last deleteDataForRange duration at the end of that slice.
	endTime := time.Now().Truncate(d.cfg.deleteRequestCreationInterval)
	startTime := endTime.Add(-d.cfg.deleteDataForRange)
	metricName := prometheus.BuildFQName(namespace, subsystem, d.Name())
	selectors := fmt.Sprintf("%s{%s}", metricName, d.cfg.ExtraSelectors)

	defer func() {
		status := success
		if err != nil {
			status = fail
		}
		deleteRequestCreationAttemptsTotal.WithLabelValues(status).Inc()
	}()

	baseURL, err := url.Parse(d.cfg.purgerAddr)
	if err != nil {
		return
	}

	baseURL.Path = path.Join(baseURL.Path, deleteRequestPath)

	query := baseURL.Query()
	query.Add("match[]", selectors)
	query.Add("start", fmt.Sprint(startTime.Unix()))
	query.Add("end", fmt.Sprint(endTime.Unix()))
	baseURL.RawQuery = query.Encode()

	r, err := http.NewRequest("POST", baseURL.String(), nil)
	if err != nil {
		return err
	}

	if d.cfg.UserID != "" {
		r = r.WithContext(user.InjectOrgID(r.Context(), d.cfg.UserID))
		err = user.InjectOrgIDIntoHTTPRequest(r.Context(), r)
		if err != nil {
			return err
		}
	}

	level.Error(util_log.Logger).Log("msg", "sending delete request", "selector", selectors, "starttime", startTime, "endtime", endTime)
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return
	}

	if resp.StatusCode != 204 {
		return fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}

	d.lastDeleteRequestIntervalMutex.Lock()
	defer d.lastDeleteRequestIntervalMutex.Unlock()

	d.lastDeleteRequestInterval = interval{startTime, endTime}

	return
}

func (d *DeleteSeriesTest) getNonDeletedIntervals(queryInterval interval) []interval {
	intervalToProcess := queryInterval
	var nonDeletedIntervals []interval

	// build first deleted interval
	deletedIntervalEnd := queryInterval.start.Truncate(d.cfg.deleteRequestCreationInterval)
	deletedIntervalStart := deletedIntervalEnd.Add(-d.cfg.deleteDataForRange)

	// first deleted interval could be out of range so try next intervals to find first relevant interval.
	for !deletedIntervalStart.After(intervalToProcess.start) {
		deletedIntervalStart = deletedIntervalStart.Add(d.cfg.deleteRequestCreationInterval)
		if deletedIntervalEnd.Add(1).After(intervalToProcess.start) {
			intervalToProcess.start = deletedIntervalEnd.Add(1)
		}
		deletedIntervalEnd = deletedIntervalEnd.Add(d.cfg.deleteRequestCreationInterval)
	}

	// keep building non-deleted intervals with each being from intervalToProcess.start to min(deletedIntervalStart.Start-1, intervalToProcess.end)
	for !deletedIntervalStart.After(queryInterval.end) {
		nonDeletedInterval := interval{intervalToProcess.start, deletedIntervalStart.Add(-1)}
		if nonDeletedInterval.end.After(intervalToProcess.end) {
			nonDeletedInterval.end = intervalToProcess.end
		}
		nonDeletedIntervals = append(nonDeletedIntervals, nonDeletedInterval)
		intervalToProcess.start = deletedIntervalEnd.Add(1)

		// build next deleted interval
		deletedIntervalStart = deletedIntervalStart.Add(d.cfg.deleteRequestCreationInterval)
		deletedIntervalEnd = deletedIntervalEnd.Add(d.cfg.deleteRequestCreationInterval)
	}

	// see if we have some interval left in intervalToProcess, add it if so.
	if intervalToProcess.start.Before(intervalToProcess.end) {
		nonDeletedIntervals = append(nonDeletedIntervals, intervalToProcess)
	}

	return nonDeletedIntervals
}

func (d *DeleteSeriesTest) MinQueryTime() time.Time {
	return calculateMinQueryTime(d.cfg.durationQuerySince, d.cfg.timeQueryStart)
}

type interval struct {
	start, end time.Time
}
