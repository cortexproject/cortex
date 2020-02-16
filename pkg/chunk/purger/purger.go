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

// DataPurgerConfig holds config for DataPurger
type DataPurgerConfig struct {
	EnablePurger    bool   `yaml:"enable_purger"`
	NumWorkers      int    `yaml:"num_workers"`
	ObjectStoreType string `yaml:"object_store_type"`
}

// RegisterFlags registers CLI flags for DataPurgerConfig
func (cfg *DataPurgerConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.EnablePurger, "enable-purger", false, "Enable purger to allow deletion of series. Be aware that Delete series feature is still experimental")
	f.IntVar(&cfg.NumWorkers, "num-workers", 2, "Number of workers executing delete plans in parallel")
	f.StringVar(&cfg.ObjectStoreType, "object-store-type", "", "Name of the object store to use for storing delete plans")
}

type workerJob struct {
	planNo          int
	userID          string
	deleteRequestID string
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
	storageClient chunk.StorageClient

	executePlansChan             chan chunk.DeleteRequest
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
func NewDataPurger(cfg DataPurgerConfig, deleteStore *chunk.DeleteStore, chunkStore chunk.Store, storageClient chunk.StorageClient) (*DataPurger, error) {
	dataPurger := DataPurger{
		cfg:                          cfg,
		deleteStore:                  deleteStore,
		chunkStore:                   chunkStore,
		storageClient:                storageClient,
		inProcessRequestIDs:          map[string]string{},
		executePlansChan:             make(chan chunk.DeleteRequest, 50),
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
					level.Error(util.Logger).Log("msg", "error executing delete plan", "request-id", jobExecutionStatus.deleteRequestID,
						"plan-no", jobExecutionStatus.planNo, "err", jobExecutionStatus.err)
				} else {
					err := dp.removeDeletePlan(context.Background(), jobExecutionStatus.userID, jobExecutionStatus.deleteRequestID, jobExecutionStatus.planNo)
					if err != nil {
						level.Error(util.Logger).Log("msg", "error removing delete plan", "request-id", jobExecutionStatus.deleteRequestID,
							"plan-no", jobExecutionStatus.planNo, "err", err)
					} else {
						pendingPlansCountMtx.Lock()
						pendingPlansCount[jobExecutionStatus.deleteRequestID]--

						if pendingPlansCount[jobExecutionStatus.deleteRequestID] == 0 {
							level.Info(util.Logger).Log("msg", "finished execution of all plans, cleaning up and updating status of request",
								"request-id", jobExecutionStatus.deleteRequestID, "user-id", jobExecutionStatus.userID)

							err := dp.deleteStore.UpdateStatus(context.Background(), jobExecutionStatus.userID, jobExecutionStatus.deleteRequestID, chunk.Processed)
							if err != nil {
								level.Error(util.Logger).Log("msg", "error updating delete request status to process",
									"request-id", jobExecutionStatus.deleteRequestID, "err", err)
							}

							delete(pendingPlansCount, jobExecutionStatus.deleteRequestID)
							pendingPlansCountMtx.Unlock()

							dp.inProcessRequestIDsMtx.Lock()
							delete(dp.inProcessRequestIDs, jobExecutionStatus.deleteRequestID)
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
		case deleteRequest := <-dp.executePlansChan:
			numPlans := numOfPlans(deleteRequest.StartTime, deleteRequest.EndTime)
			level.Info(util.Logger).Log("msg", "sending jobs to workers for purging data",
				"request-id", deleteRequest.RequestID, "user-id", deleteRequest.UserID, "num-jobs", numPlans)

			pendingPlansCountMtx.Lock()
			pendingPlansCount[deleteRequest.RequestID] = numPlans
			pendingPlansCountMtx.Unlock()

			for i := 0; i < numPlans; i++ {
				dp.workerJobChan <- workerJob{planNo: i, userID: deleteRequest.UserID, deleteRequestID: deleteRequest.RequestID}
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
		err := dp.executePlan(job.userID, job.deleteRequestID, job.planNo)
		dp.workerJobExecutionStatusChan <- workerJobExecutionStatus{job, err}
	}
}

func (dp *DataPurger) executePlan(userID, requestID string, planNo int) error {
	plan, err := dp.getDeletePlan(context.Background(), userID, requestID, planNo)
	if err != nil {
		if err == chunk.ErrStorageObjectNotFound {
			level.Info(util.Logger).Log("msg", "plan not found, must have been executed already",
				"request-id", requestID, "user-id", userID, "plan-no", planNo)
			// this means plan was already executed and got removed. Do nothing.
			return nil
		}
		return err
	}

	level.Info(util.Logger).Log("msg", "executing plan",
		"request-id", requestID, "user-id", userID, "plan-no", planNo)

	ctx := user.InjectOrgID(context.Background(), userID)

	for i := range plan.ChunksGroup {
		level.Debug(util.Logger).Log("msg", "deleting chunks",
			"request-id", requestID, "user-id", userID, "plan-no", planNo, "labels", plan.ChunksGroup[i].Labels)

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
					level.Error(util.Logger).Log("msg", "chunk not found for deletion. We may have already deleted it",
						"request-id", requestID, "user-id", userID, "plan-no", planNo, "chunk-id", chunkDetails.ID)
					continue
				}
				return err
			}
		}

		level.Debug(util.Logger).Log("msg", "deleting series",
			"request-id", requestID, "user-id", userID, "plan-no", planNo, "labels", plan.ChunksGroup[i].Labels)

		// this is mostly required to clean up series ids from series store
		err := dp.chunkStore.DeleteSeriesIDs(ctx, model.Time(plan.StartTimestampMs), model.Time(plan.EndTimestampMs),
			userID, client.FromLabelAdaptersToLabels(plan.ChunksGroup[i].Labels))
		if err != nil {
			return err
		}
	}

	level.Info(util.Logger).Log("msg", "finished execution of plan",
		"request-id", requestID, "user-id", userID, "plan-no", planNo)

	return nil
}

// we need to load all in process delete requests on startup to finish them first
func (dp *DataPurger) loadInprocessDeleteRequests() error {
	requestsWithBuildingPlanStatus, err := dp.deleteStore.GetDeleteRequestsByStatus(context.Background(), chunk.BuildingPlan)
	if err != nil {
		return err
	}

	for _, deleteRequest := range requestsWithBuildingPlanStatus {
		level.Info(util.Logger).Log("msg", "loaded in process delete requests with status building plan",
			"request-id", deleteRequest.RequestID, "user-id", deleteRequest.UserID)

		dp.inProcessRequestIDs[deleteRequest.UserID] = dp.inProcessRequestIDs[deleteRequest.RequestID]
		err := dp.buildDeletePlan(deleteRequest)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error building delete plan", "request-id", deleteRequest.RequestID, "err", err)
		}

		level.Info(util.Logger).Log("msg", "sending delete request for execution",
			"request-id", deleteRequest.RequestID, "user-id", deleteRequest.UserID)
		dp.executePlansChan <- deleteRequest
	}

	requestsWithDeletingStatus, err := dp.deleteStore.GetDeleteRequestsByStatus(context.Background(), chunk.Deleting)
	if err != nil {
		return err
	}

	for _, deleteRequest := range requestsWithDeletingStatus {
		level.Info(util.Logger).Log("msg", "loaded in process delete requests with status deleting",
			"request-id", deleteRequest.RequestID, "user-id", deleteRequest.UserID)

		dp.inProcessRequestIDs[deleteRequest.UserID] = dp.inProcessRequestIDs[deleteRequest.RequestID]
		dp.executePlansChan <- deleteRequest
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
				"inprocess-request-id", inprocessDeleteRequstID,
				"skipped-request-id", deleteRequest.RequestID, "user-id", deleteRequest.UserID)
			continue
		}

		if deleteRequest.CreatedAt.Add(24 * time.Second).After(model.Now()) {
			continue
		}

		err = dp.deleteStore.UpdateStatus(context.Background(), deleteRequest.UserID, deleteRequest.RequestID, chunk.BuildingPlan)
		if err != nil {
			return err
		}

		dp.inProcessRequestIDsMtx.Lock()
		dp.inProcessRequestIDs[deleteRequest.UserID] = deleteRequest.RequestID
		dp.inProcessRequestIDsMtx.Unlock()

		level.Info(util.Logger).Log("msg", "building plan for a new delete request",
			"request-id", deleteRequest.RequestID, "user-id", deleteRequest.UserID)

		err := dp.buildDeletePlan(deleteRequest)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error building delete plan", "request-id", deleteRequest.RequestID, "err", err)
			return err
		}

		level.Info(util.Logger).Log("msg", "sending delete request for execution",
			"request-id", deleteRequest.RequestID, "user-id", deleteRequest.UserID)
		dp.executePlansChan <- deleteRequest
	}

	return nil
}

// buildDeletePlan builds per day delete plan for given delete requests.
// A days plan will include chunk ids and labels of all the chunks which are supposed to be deleted.
// Chunks are grouped together by labels to avoid storing labels repetitively.
// After building delete plans it updates status of delete request to Deleting and sends it for execution
func (dp *DataPurger) buildDeletePlan(deleteRequest chunk.DeleteRequest) error {
	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, deleteRequest.UserID)

	perDayTimeRange := splitByDay(deleteRequest.StartTime, deleteRequest.EndTime)
	level.Info(util.Logger).Log("msg", "building delete plan",
		"request-id", deleteRequest.RequestID, "user-id", deleteRequest.UserID, "num-plans", len(perDayTimeRange))

	plans := make([][]byte, len(perDayTimeRange))
	for i, planRange := range perDayTimeRange {
		chunksGroups := []purgeplan.ChunksGroup{}

		for _, selector := range deleteRequest.Selectors {
			matchers, err := promql.ParseMetricSelector(selector)
			if err != nil {
				return err
			}

			// ToDo: remove duplicate chunks
			chunks, err := dp.chunkStore.Get(ctx, deleteRequest.UserID, planRange.Start, planRange.End, matchers...)
			if err != nil {
				return err
			}

			chunksGroups = append(chunksGroups, groupChunks(chunks, deleteRequest.StartTime, deleteRequest.EndTime)...)
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

	err := dp.putDeletePlans(ctx, deleteRequest.UserID, deleteRequest.RequestID, plans)
	if err != nil {
		return err
	}

	err = dp.deleteStore.UpdateStatus(ctx, deleteRequest.UserID, deleteRequest.RequestID, chunk.Deleting)
	if err != nil {
		return err
	}

	level.Info(util.Logger).Log("msg", "built delete plans",
		"request-id", deleteRequest.RequestID, "user-id", deleteRequest.UserID, "num-plans", len(perDayTimeRange))

	return nil
}

func (dp *DataPurger) putDeletePlans(ctx context.Context, userID, requestID string, plans [][]byte) error {
	userIDAndRequestID := fmt.Sprintf("%s:%s", userID, requestID)

	for i, plan := range plans {
		objectKey := fmt.Sprintf("%s/%d", userIDAndRequestID, i)

		err := dp.storageClient.PutObject(ctx, objectKey, bytes.NewReader(plan))
		if err != nil {
			return err
		}
	}

	return nil
}

func (dp *DataPurger) getDeletePlan(ctx context.Context, userID, requestID string, planNo int) (*purgeplan.Plan, error) {
	objectKey := fmt.Sprintf("%s:%s/%d", userID, requestID, planNo)

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
	objectKey := fmt.Sprintf("%s:%s/%d", userID, requestID, planNo)
	return dp.storageClient.DeleteObject(ctx, objectKey)
}

// returns start, [start of each following days before last day...], end
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
	if start%model.Time(millisecondPerDay) != 0 {
		start = model.Time((int64(start) / millisecondPerDay) * millisecondPerDay)
	}
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
