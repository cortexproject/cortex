package purger

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/purger/purgeplan"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
)

const millisecondPerDay = int64(24 * time.Hour / time.Millisecond)

var separatorString = string([]byte{model.SeparatorByte})

type deleteRequestWithLogger struct {
	chunk.DeleteRequest
	logger log.Logger
}

// DataPurgerConfig holds config for DataPurger
type DataPurgerConfig struct {
	EnablePurger    bool   `yaml:"enable_purger"`
	NumWorkers      int    `yaml:"num_workers"`
	ObjectStoreType string `yaml:"object_store_type"`
}

// RegisterFlags registers CLI flags for DataPurgerConfig
func (cfg *DataPurgerConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.EnablePurger, "purger.enable-purger", false, "Enable purger to allow deletion of series. Be aware that Delete series feature is still experimental")
	f.IntVar(&cfg.NumWorkers, "purger.num-workers", 2, "Number of workers executing delete plans in parallel")
	f.StringVar(&cfg.ObjectStoreType, "purger.object-store-type", "", "Name of the object store to use for storing delete plans")
}

type workerJob struct {
	planNo          int
	userID          string
	deleteRequestID string
	logger          log.Logger
}

type workerJobExecutionStatus struct {
	workerJob
	err error
}

// DataPurger does the purging of data which is requested to be deleted
type DataPurger struct {
	cfg           DataPurgerConfig
	deleteStore   *chunk.DeleteStore
	chunkStore    chunk.Store
	storageClient chunk.ObjectClient

	executePlansChan             chan deleteRequestWithLogger
	workerJobChan                chan workerJob
	workerJobExecutionStatusChan chan workerJobExecutionStatus

	// we would only allow processing of singe delete request at a time since delete requests touching same chunks could change the chunk IDs of partially deleted chunks
	// and break the purge plan for other requests
	inProcessRequestIDs    map[string]string
	inProcessRequestIDsMtx sync.RWMutex

	quit chan struct{}
	wg   sync.WaitGroup
}

// NewDataPurger creates a new DataPurger
func NewDataPurger(cfg DataPurgerConfig, deleteStore *chunk.DeleteStore, chunkStore chunk.Store, storageClient chunk.ObjectClient) (*DataPurger, error) {
	dataPurger := DataPurger{
		cfg:                          cfg,
		deleteStore:                  deleteStore,
		chunkStore:                   chunkStore,
		storageClient:                storageClient,
		inProcessRequestIDs:          map[string]string{},
		executePlansChan:             make(chan deleteRequestWithLogger, 50),
		workerJobChan:                make(chan workerJob, 50),
		workerJobExecutionStatusChan: make(chan workerJobExecutionStatus, 50),
		quit:                         make(chan struct{}),
	}

	dataPurger.runWorkers()
	go dataPurger.jobScheduler()

	err := dataPurger.loadInprocessDeleteRequests()
	if err != nil {
		return nil, err
	}

	return &dataPurger, nil
}

// Run starts workers, job scheduler and a loop for feeding delete requests to them for processing
func (dp *DataPurger) Run() {
	dp.loop()
}

// Stop stops all background workers/loops
func (dp *DataPurger) Stop() {
	close(dp.quit)
	dp.wg.Wait()
}

func (dp *DataPurger) loop() {
	dp.wg.Add(1)
	defer dp.wg.Done()

	pullDeleteRequestsToPlanDeletesTicker := time.NewTicker(time.Hour)
	defer pullDeleteRequestsToPlanDeletesTicker.Stop()

	for {
		select {
		case <-pullDeleteRequestsToPlanDeletesTicker.C:
			err := dp.pullDeleteRequestsToPlanDeletes()
			if err != nil {
				level.Error(util.Logger).Log("msg", "error pulling delete requests for building plans", "err", err)
			}
		case <-dp.quit:
			return
		}
	}
}

// we send all the delete plans to workerJobChan and then start checking for status on workerJobExecutionStatusChan
func (dp *DataPurger) jobScheduler() {

	pendingPlansCount := make(map[string]int)
	pendingPlansCountMtx := sync.Mutex{}

	dp.wg.Add(1)

	go func() {

		for {
			select {
			case jobExecutionStatus := <-dp.workerJobExecutionStatusChan:
				if jobExecutionStatus.err != nil {
					level.Error(jobExecutionStatus.logger).Log("msg", "error executing delete plan",
						"plan_no", jobExecutionStatus.planNo, "err", jobExecutionStatus.err)
				} else {
					err := dp.removeDeletePlan(context.Background(), jobExecutionStatus.userID, jobExecutionStatus.deleteRequestID, jobExecutionStatus.planNo)
					if err != nil {
						level.Error(jobExecutionStatus.logger).Log("msg", "error removing delete plan",
							"plan_no", jobExecutionStatus.planNo, "err", err)
					} else {
						pendingPlansCountMtx.Lock()
						pendingPlansCount[jobExecutionStatus.deleteRequestID]--

						if pendingPlansCount[jobExecutionStatus.deleteRequestID] == 0 {
							level.Info(jobExecutionStatus.logger).Log("msg", "finished execution of all plans, cleaning up and updating status of request")

							err := dp.deleteStore.UpdateStatus(context.Background(), jobExecutionStatus.userID, jobExecutionStatus.deleteRequestID, chunk.Processed)
							if err != nil {
								level.Error(jobExecutionStatus.logger).Log("msg", "error updating delete request status to process", "err", err)
							}

							delete(pendingPlansCount, jobExecutionStatus.deleteRequestID)
							pendingPlansCountMtx.Unlock()

							dp.inProcessRequestIDsMtx.Lock()
							delete(dp.inProcessRequestIDs, jobExecutionStatus.userID)
							dp.inProcessRequestIDsMtx.Unlock()
						} else {
							pendingPlansCountMtx.Unlock()

						}

					}
				}
			case <-dp.quit:
				dp.wg.Done()
				return
			}
		}
	}()

	dp.wg.Add(1)
	defer dp.wg.Done()

	for {
		select {
		case requestWithLogger := <-dp.executePlansChan:
			numPlans := numOfPlans(requestWithLogger.StartTime, requestWithLogger.EndTime)
			level.Info(requestWithLogger.logger).Log("msg", "sending jobs to workers for purging data", "num_jobs", numPlans)

			pendingPlansCountMtx.Lock()
			pendingPlansCount[requestWithLogger.RequestID] = numPlans
			pendingPlansCountMtx.Unlock()

			for i := 0; i < numPlans; i++ {
				dp.workerJobChan <- workerJob{planNo: i, userID: requestWithLogger.UserID,
					deleteRequestID: requestWithLogger.RequestID, logger: requestWithLogger.logger}
			}
		case <-dp.quit:
			close(dp.workerJobChan)
			return
		}
	}
}

func (dp *DataPurger) runWorkers() {
	for i := 0; i < dp.cfg.NumWorkers; i++ {
		dp.wg.Add(1)
		go dp.worker()
	}
}

func (dp *DataPurger) worker() {
	defer dp.wg.Done()

	for job := range dp.workerJobChan {
		err := dp.executePlan(job.userID, job.deleteRequestID, job.planNo, job.logger)
		dp.workerJobExecutionStatusChan <- workerJobExecutionStatus{job, err}
	}
}

func (dp *DataPurger) executePlan(userID, requestID string, planNo int, logger log.Logger) error {
	logger = log.With(logger, "plan_no", planNo)

	plan, err := dp.getDeletePlan(context.Background(), userID, requestID, planNo)
	if err != nil {
		if err == chunk.ErrStorageObjectNotFound {
			level.Info(logger).Log("msg", "plan not found, must have been executed already")
			// this means plan was already executed and got removed. Do nothing.
			return nil
		}
		return err
	}

	level.Info(logger).Log("msg", "executing plan")

	ctx := user.InjectOrgID(context.Background(), userID)

	for i := range plan.ChunksGroup {
		level.Debug(logger).Log("msg", "deleting chunks", "labels", plan.ChunksGroup[i].Labels)

		for _, chunkDetails := range plan.ChunksGroup[i].Chunks {
			chunkRef, err := chunk.ParseExternalKey(userID, chunkDetails.ID)
			if err != nil {
				return err
			}

			var partiallyDeletedInterval *model.Interval = nil
			if chunkDetails.PartiallyDeletedInterval != nil {
				partiallyDeletedInterval = &model.Interval{
					Start: model.Time(chunkDetails.PartiallyDeletedInterval.StartTimestampMs),
					End:   model.Time(chunkDetails.PartiallyDeletedInterval.EndTimestampMs),
				}
			}

			err = dp.chunkStore.DeleteChunk(ctx, chunkRef.From, chunkRef.Through, chunkRef.UserID,
				chunkDetails.ID, client.FromLabelAdaptersToLabels(plan.ChunksGroup[i].Labels), partiallyDeletedInterval)
			if err != nil {
				if isMissingChunkErr(err) {
					level.Error(logger).Log("msg", "chunk not found for deletion. We may have already deleted it",
						"chunk_id", chunkDetails.ID)
					continue
				}
				return err
			}
		}

		level.Debug(logger).Log("msg", "deleting series", "labels", plan.ChunksGroup[i].Labels)

		// this is mostly required to clean up series ids from series store
		err := dp.chunkStore.DeleteSeriesIDs(ctx, model.Time(plan.StartTimestampMs), model.Time(plan.EndTimestampMs),
			userID, client.FromLabelAdaptersToLabels(plan.ChunksGroup[i].Labels))
		if err != nil {
			return err
		}
	}

	level.Info(logger).Log("msg", "finished execution of plan")

	return nil
}

// we need to load all in process delete requests on startup to finish them first
func (dp *DataPurger) loadInprocessDeleteRequests() error {
	requestsWithBuildingPlanStatus, err := dp.deleteStore.GetDeleteRequestsByStatus(context.Background(), chunk.BuildingPlan)
	if err != nil {
		return err
	}

	for _, deleteRequest := range requestsWithBuildingPlanStatus {
		requestWithLogger := makeDeleteRequestWithLogger(deleteRequest, util.Logger)

		level.Info(requestWithLogger.logger).Log("msg", "loaded in process delete requests with status building plan")

		dp.inProcessRequestIDs[deleteRequest.UserID] = deleteRequest.RequestID
		err := dp.buildDeletePlan(requestWithLogger)
		if err != nil {
			level.Error(requestWithLogger.logger).Log("msg", "error building delete plan", "err", err)
		}

		level.Info(requestWithLogger.logger).Log("msg", "sending delete request for execution")
		dp.executePlansChan <- requestWithLogger
	}

	requestsWithDeletingStatus, err := dp.deleteStore.GetDeleteRequestsByStatus(context.Background(), chunk.Deleting)
	if err != nil {
		return err
	}

	for _, deleteRequest := range requestsWithDeletingStatus {
		requestAndLogger := makeDeleteRequestWithLogger(deleteRequest, util.Logger)
		level.Info(requestAndLogger.logger).Log("msg", "loaded in process delete requests with status deleting")

		dp.inProcessRequestIDs[deleteRequest.UserID] = deleteRequest.RequestID
		dp.executePlansChan <- requestAndLogger
	}

	return nil
}

// pullDeleteRequestsToPlanDeletes pulls delete requests which do not have their delete plans built yet and sends them for building delete plans
// after pulling delete requests for building plans, it updates its status to BuildingPlan status to avoid picking this up again next time
func (dp *DataPurger) pullDeleteRequestsToPlanDeletes() error {
	deleteRequests, err := dp.deleteStore.GetDeleteRequestsByStatus(context.Background(), chunk.Received)
	if err != nil {
		return err
	}

	for _, deleteRequest := range deleteRequests {
		dp.inProcessRequestIDsMtx.RLock()
		inprocessDeleteRequstID := dp.inProcessRequestIDs[deleteRequest.UserID]
		dp.inProcessRequestIDsMtx.RUnlock()

		if inprocessDeleteRequstID != "" {
			level.Debug(util.Logger).Log("msg", "skipping delete request processing for now since another request from same user is already in process",
				"inprocess_request_id", inprocessDeleteRequstID,
				"skipped_request_id", deleteRequest.RequestID, "user_id", deleteRequest.UserID)
			continue
		}

		if deleteRequest.CreatedAt.Add(24 * time.Hour).After(model.Now()) {
			continue
		}

		err = dp.deleteStore.UpdateStatus(context.Background(), deleteRequest.UserID, deleteRequest.RequestID, chunk.BuildingPlan)
		if err != nil {
			return err
		}

		dp.inProcessRequestIDsMtx.Lock()
		dp.inProcessRequestIDs[deleteRequest.UserID] = deleteRequest.RequestID
		dp.inProcessRequestIDsMtx.Unlock()

		requestWithLogger := makeDeleteRequestWithLogger(deleteRequest, util.Logger)

		level.Info(requestWithLogger.logger).Log("msg", "building plan for a new delete request")

		err := dp.buildDeletePlan(requestWithLogger)
		if err != nil {
			level.Error(requestWithLogger.logger).Log("msg", "error building delete plan", "err", err)
			return err
		}

		level.Info(requestWithLogger.logger).Log("msg", "sending delete request for execution")
		dp.executePlansChan <- requestWithLogger
	}

	return nil
}

// buildDeletePlan builds per day delete plan for given delete requests.
// A days plan will include chunk ids and labels of all the chunks which are supposed to be deleted.
// Chunks are grouped together by labels to avoid storing labels repetitively.
// After building delete plans it updates status of delete request to Deleting and sends it for execution
func (dp *DataPurger) buildDeletePlan(requestWithLogger deleteRequestWithLogger) error {
	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, requestWithLogger.UserID)

	perDayTimeRange := splitByDay(requestWithLogger.StartTime, requestWithLogger.EndTime)
	level.Info(requestWithLogger.logger).Log("msg", "building delete plan", "num_plans", len(perDayTimeRange))

	plans := make([][]byte, len(perDayTimeRange))
	for i, planRange := range perDayTimeRange {
		chunksGroups := []purgeplan.ChunksGroup{}

		for _, selector := range requestWithLogger.Selectors {
			matchers, err := promql.ParseMetricSelector(selector)
			if err != nil {
				return err
			}

			// ToDo: remove duplicate chunks
			chunks, err := dp.chunkStore.Get(ctx, requestWithLogger.UserID, planRange.Start, planRange.End, matchers...)
			if err != nil {
				return err
			}

			chunksGroups = append(chunksGroups, groupChunks(chunks, requestWithLogger.StartTime, requestWithLogger.EndTime)...)
		}

		plan := purgeplan.Plan{
			StartTimestampMs: int64(planRange.Start),
			EndTimestampMs:   int64(planRange.End),
			ChunksGroup:      chunksGroups,
		}

		pb, err := proto.Marshal(&plan)
		if err != nil {
			return err
		}

		plans[i] = pb
	}

	err := dp.putDeletePlans(ctx, requestWithLogger.UserID, requestWithLogger.RequestID, plans)
	if err != nil {
		return err
	}

	err = dp.deleteStore.UpdateStatus(ctx, requestWithLogger.UserID, requestWithLogger.RequestID, chunk.Deleting)
	if err != nil {
		return err
	}

	level.Info(requestWithLogger.logger).Log("msg", "built delete plans", "num_plans", len(perDayTimeRange))

	return nil
}

func (dp *DataPurger) putDeletePlans(ctx context.Context, userID, requestID string, plans [][]byte) error {
	for i, plan := range plans {
		objectKey := buildObjectKeyForPlan(userID, requestID, i)

		err := dp.storageClient.PutObject(ctx, objectKey, bytes.NewReader(plan))
		if err != nil {
			return err
		}
	}

	return nil
}

func (dp *DataPurger) getDeletePlan(ctx context.Context, userID, requestID string, planNo int) (*purgeplan.Plan, error) {
	objectKey := buildObjectKeyForPlan(userID, requestID, planNo)

	readCloser, err := dp.storageClient.GetObject(ctx, objectKey)
	if err != nil {
		return nil, err
	}

	defer readCloser.Close()

	buf, err := ioutil.ReadAll(readCloser)
	if err != nil {
		return nil, err
	}

	var plan purgeplan.Plan
	err = proto.Unmarshal(buf, &plan)
	if err != nil {
		return nil, err
	}

	return &plan, nil
}

func (dp *DataPurger) removeDeletePlan(ctx context.Context, userID, requestID string, planNo int) error {
	objectKey := buildObjectKeyForPlan(userID, requestID, planNo)
	return dp.storageClient.DeleteObject(ctx, objectKey)
}

// returns interval per plan
func splitByDay(start, end model.Time) []model.Interval {
	numOfDays := numOfPlans(start, end)

	perDayTimeRange := make([]model.Interval, numOfDays)
	startOfNextDay := model.Time(((int64(start) / millisecondPerDay) + 1) * millisecondPerDay)
	perDayTimeRange[0] = model.Interval{Start: start, End: startOfNextDay - 1}

	for i := 1; i < numOfDays; i++ {
		interval := model.Interval{Start: startOfNextDay}
		startOfNextDay += model.Time(millisecondPerDay)
		interval.End = startOfNextDay - 1
		perDayTimeRange[i] = interval
	}

	perDayTimeRange[numOfDays-1].End = end

	return perDayTimeRange
}

func numOfPlans(start, end model.Time) int {
	// rounding down start to start of the day
	if start%model.Time(millisecondPerDay) != 0 {
		start = model.Time((int64(start) / millisecondPerDay) * millisecondPerDay)
	}

	// rounding up end to end of the day
	if end%model.Time(millisecondPerDay) != 0 {
		end = model.Time((int64(end)/millisecondPerDay)*millisecondPerDay + millisecondPerDay)
	}

	return int(int64(end-start) / millisecondPerDay)
}

// groups chunks together by unique label sets i.e all the chunks with same labels would be stored in a group
func groupChunks(chunks []chunk.Chunk, deleteFrom, deleteThrough model.Time) []purgeplan.ChunksGroup {
	chunksGroups := []purgeplan.ChunksGroup{}
	usToChunksGroupsIndexMap := make(map[string]int)

	for i := range chunks {
		us := metricToUniqueString(chunks[i].Metric)
		idx, isOK := usToChunksGroupsIndexMap[us]
		if !isOK {
			chunksGroups = append(chunksGroups, purgeplan.ChunksGroup{Labels: client.FromLabelsToLabelAdapters(chunks[i].Metric)})
			idx = len(chunksGroups) - 1
			usToChunksGroupsIndexMap[us] = idx
		}

		chunkDetails := purgeplan.ChunkDetails{ID: chunks[i].ExternalKey()}

		if deleteFrom > chunks[i].From || deleteThrough < chunks[i].Through {
			partiallyDeletedInterval := purgeplan.Interval{StartTimestampMs: int64(chunks[i].From), EndTimestampMs: int64(chunks[i].Through)}

			if deleteFrom > chunks[i].From {
				partiallyDeletedInterval.StartTimestampMs = int64(deleteFrom)
			}

			if deleteThrough < chunks[i].Through {
				partiallyDeletedInterval.EndTimestampMs = int64(deleteThrough)
			}
			chunkDetails.PartiallyDeletedInterval = &partiallyDeletedInterval
		}

		chunksGroups[idx].Chunks = append(chunksGroups[idx].Chunks, chunkDetails)
	}

	return chunksGroups

}

// ToDo: see if we can have an efficient way to uniquely identify label sets.
func metricToUniqueString(m labels.Labels) string {
	parts := make([]string, 0, len(m))
	for _, pair := range m {
		parts = append(parts, string(pair.Name)+separatorString+string(pair.Value))
	}

	return strings.Join(parts, separatorString)
}

func isMissingChunkErr(err error) bool {
	if err == chunk.ErrStorageObjectNotFound {
		return true
	}
	if promqlStorageErr, ok := err.(promql.ErrStorage); ok && promqlStorageErr.Err == chunk.ErrStorageObjectNotFound {
		return true
	}

	return false
}

func buildObjectKeyForPlan(userID, requestID string, planNo int) string {
	return fmt.Sprintf("%s:%s/%d", userID, requestID, planNo)
}

func makeDeleteRequestWithLogger(deleteRequest chunk.DeleteRequest, l log.Logger) deleteRequestWithLogger {
	logger := log.With(l, "user_id", deleteRequest.UserID, "request_id", deleteRequest.RequestID)
	return deleteRequestWithLogger{deleteRequest, logger}
}
