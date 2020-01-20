package purger

import (
	"context"
	"flag"
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

const millisecondPerDay = int64(time.Second)

var separatorString = string([]byte{model.SeparatorByte})

// DataPurgerConfig holds config for DataPurger
type DataPurgerConfig struct {
	NumWorkers int `yaml:"num_workers"`
}

// RegisterFlags registers CLI flags for DataPurgerConfig
func (cfg *DataPurgerConfig) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.NumWorkers, "num-workers", 2, "Number of workers executing delete plans in parallel")
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
	cfg         DataPurgerConfig
	deleteStore chunk.DeleteStore
	chunkStore  chunk.Store

	buildPlanChan                chan chunk.DeleteRequest
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
func NewDataPurger(cfg DataPurgerConfig, deleteStore chunk.DeleteStore, chunkStore chunk.Store) (*DataPurger, error) {
	dataPurger := DataPurger{
		cfg:                          cfg,
		deleteStore:                  deleteStore,
		chunkStore:                   chunkStore,
		inProcessRequestIDs:          map[string]string{},
		buildPlanChan:                make(chan chunk.DeleteRequest, 50),
		executePlansChan:             make(chan chunk.DeleteRequest, 50),
		workerJobChan:                make(chan workerJob, 50),
		workerJobExecutionStatusChan: make(chan workerJobExecutionStatus, 50),
	}

	err := dataPurger.loadInprocessDeleteRequests()
	if err != nil {
		return nil, err
	}

	return &dataPurger, nil
}

// Run starts workers, job scheduler and a loop for feeding delete requests to them for processing
func (dp *DataPurger) Run() {
	dp.runWorkers()
	go dp.jobScheduler()
	dp.loop()
}

// Stop stops all background workers/loops
func (dp *DataPurger) Stop() {
	close(dp.quit)
	dp.wg.Wait()
}

func (dp *DataPurger) loop() {
	dp.wg.Add(1)

	pullDeleteRequestsToPlanDeletesTicker := time.NewTicker(time.Second)
	defer pullDeleteRequestsToPlanDeletesTicker.Stop()

	for {
		select {
		case <-pullDeleteRequestsToPlanDeletesTicker.C:
			err := dp.pullDeleteRequestsToPlanDeletes()
			if err != nil {
				level.Error(util.Logger).Log("msg", "error pulling delete requests for building plans", "err", err)
			}
		case deleteRequest := <-dp.buildPlanChan:
			err := dp.buildDeletePlan(deleteRequest)
			if err != nil {
				level.Error(util.Logger).Log("msg", "error building delete plan", "request-id", deleteRequest.RequestID, "err", err)
			}
		case <-dp.quit:
			dp.wg.Done()
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
					level.Error(util.Logger).Log("msg", "error executing delete plan", "request-id", jobExecutionStatus.deleteRequestID, "plan-no", jobExecutionStatus.planNo, "err", jobExecutionStatus.err)
				} else {
					err := dp.deleteStore.RemoveDeletePlan(context.Background(), jobExecutionStatus.userID, jobExecutionStatus.deleteRequestID, jobExecutionStatus.planNo)
					if err != nil {
						level.Error(util.Logger).Log("msg", "error removing delete plan", "request-id", jobExecutionStatus.deleteRequestID, "plan-no", jobExecutionStatus.planNo, "err", err)
					} else {
						pendingPlansCountMtx.Lock()
						pendingPlansCount[jobExecutionStatus.deleteRequestID]--

						if pendingPlansCount[jobExecutionStatus.deleteRequestID] == 0 {
							level.Debug(util.Logger).Log("msg", "finished execution of all plans so cleaning up and updating status of request",
								"request-id", jobExecutionStatus.deleteRequestID, "user-id", jobExecutionStatus.userID)

							err := dp.deleteStore.UpdateStatus(context.Background(), jobExecutionStatus.userID, jobExecutionStatus.deleteRequestID, chunk.DeleteRequestStatusProcessed)
							if err != nil {
								level.Error(util.Logger).Log("msg", "error updating delete request status to process", "request-id", jobExecutionStatus.deleteRequestID, "err", err)
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
				break
			}
		}
	}()

	dp.wg.Add(1)

	for {
		select {
		case deleteRequest := <-dp.executePlansChan:
			numPlans := numOfPlans(deleteRequest.StartTime, deleteRequest.EndTime)
			level.Debug(util.Logger).Log("msg", "sending jobs to workers for purging data",
				"request-id", deleteRequest.RequestID, "user-id", deleteRequest.UserID, "num-jobs", numPlans)

			pendingPlansCountMtx.Lock()
			pendingPlansCount[deleteRequest.RequestID] = numPlans
			pendingPlansCountMtx.Unlock()

			for i := 0; i < numPlans; i++ {
				dp.workerJobChan <- workerJob{planNo: i, userID: deleteRequest.UserID, deleteRequestID: deleteRequest.RequestID}
			}
		case <-dp.quit:
			close(dp.workerJobChan)
			dp.wg.Done()
			break
		}
	}
}

func (dp *DataPurger) runWorkers() {
	for i := 0; i < dp.cfg.NumWorkers; i++ {
		go dp.worker()
	}
}

func (dp *DataPurger) worker() {
	for job := range dp.workerJobChan {
		err := dp.executePlan(job.userID, job.deleteRequestID, job.planNo)
		dp.workerJobExecutionStatusChan <- workerJobExecutionStatus{job, err}
	}
}

func (dp *DataPurger) executePlan(userID, requestID string, planNo int) error {
	pb, err := dp.deleteStore.GetDeletePlan(context.Background(), userID, requestID, planNo)
	if err != nil {
		return err
	}

	if pb == nil {
		level.Debug(util.Logger).Log("msg", "plan not found, must be executed already",
			"request-id", requestID, "user-id", userID, "plan-no", planNo)
		// this means plan was already executed and got removed. Do nothing.
		return nil
	}

	level.Debug(util.Logger).Log("msg", "executing plan",
		"request-id", requestID, "user-id", userID, "plan-no", planNo)

	var plan purgeplan.Plan
	err = proto.Unmarshal(pb, &plan)
	if err != nil {
		return err
	}

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
				if isMissingChunkErr(err) && partiallyDeletedInterval != nil {
					level.Error(util.Logger).Log("msg", "partially deleted chunk missing. We may have already processed it",
						"request-id", requestID, "user-id", userID, "plan-no", planNo, "chunk-id", chunkDetails.ID)
					continue
				}
				return err
			}
		}

		level.Debug(util.Logger).Log("msg", "deleting series",
			"request-id", requestID, "user-id", userID, "plan-no", planNo, "labels", plan.ChunksGroup[i].Labels)

		err := dp.chunkStore.DeleteLabels(ctx, model.Time(plan.StartTimestampMs), model.Time(plan.EndTimestampMs),
			userID, client.FromLabelAdaptersToLabels(plan.ChunksGroup[i].Labels))
		if err != nil {
			return err
		}
	}

	level.Debug(util.Logger).Log("msg", "finished execution of plan",
		"request-id", requestID, "user-id", userID, "plan-no", planNo)

	return nil
}

// we need to load all in process delete requests on startup to finish them first
func (dp *DataPurger) loadInprocessDeleteRequests() error {
	requestsWithBuildingPlanStatus, err := dp.deleteStore.GetDeleteRequestsByStatus(context.Background(), chunk.DeleteRequestStatusBuildingPlan)
	if err != nil {
		return err
	}

	for _, deleteRequest := range requestsWithBuildingPlanStatus {
		level.Debug(util.Logger).Log("msg", "loaded in process delete requests with status building plan", "request-id", deleteRequest.RequestID, "user-id", deleteRequest.UserID)

		dp.inProcessRequestIDs[deleteRequest.UserID] = dp.inProcessRequestIDs[deleteRequest.RequestID]
		dp.buildPlanChan <- deleteRequest
	}

	requestsWithDeletingStatus, err := dp.deleteStore.GetDeleteRequestsByStatus(context.Background(), chunk.DeleteRequestStatusDeleting)
	if err != nil {
		return err
	}

	for _, deleteRequest := range requestsWithDeletingStatus {
		level.Debug(util.Logger).Log("msg", "loaded in process delete requests with status deleting", "request-id", deleteRequest.RequestID, "user-id", deleteRequest.UserID)

		dp.inProcessRequestIDs[deleteRequest.UserID] = dp.inProcessRequestIDs[deleteRequest.RequestID]
		dp.executePlansChan <- deleteRequest
	}

	return nil
}

// pullDeleteRequestsToPlanDeletes pulls delete requests which do not have their delete plans built yet and sends them for building delete plans
// after pulling delete requests for building plans, it updates its status to DeleteRequestStatusBuildingPlan status to avoid picking this up again next time
func (dp *DataPurger) pullDeleteRequestsToPlanDeletes() error {
	deleteRequests, err := dp.deleteStore.GetDeleteRequestsByStatus(context.Background(), chunk.DeleteRequestStatusReceived)
	if err != nil {
		return err
	}

	for _, deleteRequest := range deleteRequests {
		dp.inProcessRequestIDsMtx.RLock()
		inprocessDeleteRequstID := dp.inProcessRequestIDs[deleteRequest.UserID]
		dp.inProcessRequestIDsMtx.RUnlock()

		if inprocessDeleteRequstID != "" {
			level.Debug(util.Logger).Log("msg", "skipping delete request processing for now since another request from same users is already in process",
				"inprocess-request-id", inprocessDeleteRequstID,
				"skipped-request-id", deleteRequest.RequestID, "user-id", deleteRequest.UserID)
			continue
		}

		if deleteRequest.CreatedAt.Add(24 * time.Second).After(model.Now()) {
			continue
		}

		err = dp.deleteStore.UpdateStatus(context.Background(), deleteRequest.UserID, deleteRequest.RequestID, chunk.DeleteRequestStatusBuildingPlan)
		if err != nil {
			return err
		}

		dp.inProcessRequestIDsMtx.Lock()
		dp.inProcessRequestIDs[deleteRequest.UserID] = deleteRequest.RequestID
		dp.inProcessRequestIDsMtx.Unlock()

		level.Debug(util.Logger).Log("msg", "new delete request sent for building plan",
			"request-id", deleteRequest.RequestID, "user-id", deleteRequest.UserID)
		dp.buildPlanChan <- deleteRequest

	}

	return nil
}

// buildDeletePlan builds per day delete plan for given delete requests.
// A days plan will include chunk ids and labels of all the chunks which are supposed to be deleted.
// Chunks are grouped together by labels to avoid storing labels repetitively.
// After building delete plans it updates status of delete request to DeleteRequestStatusDeleting and sends it for execution
func (dp *DataPurger) buildDeletePlan(deleteRequest chunk.DeleteRequest) error {
	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, deleteRequest.UserID)

	perDayTimeRange := splitByDay(deleteRequest.StartTime, deleteRequest.EndTime)
	level.Debug(util.Logger).Log("msg", "building delete plan",
		"request-id", deleteRequest.RequestID, "user-id", deleteRequest.UserID, "num-plans", len(perDayTimeRange)-1)

	plans := make([][]byte, len(perDayTimeRange)-1)
	for i := 0; i < len(perDayTimeRange)-1; i++ {
		chunksGroups := []purgeplan.ChunksGroup{}

		for _, selector := range deleteRequest.Selectors {
			matchers, err := promql.ParseMetricSelector(selector)
			if err != nil {
				return err
			}
			chunks, err := dp.chunkStore.Get(ctx, deleteRequest.UserID, perDayTimeRange[i], perDayTimeRange[i+1], matchers...)
			if err != nil {
				return err
			}

			chunksGroups = append(chunksGroups, groupChunks(chunks, deleteRequest.StartTime, deleteRequest.EndTime)...)
		}

		plan := purgeplan.Plan{
			StartTimestampMs: int64(perDayTimeRange[i]),
			EndTimestampMs:   int64(perDayTimeRange[i+1]),
			ChunksGroup:      chunksGroups,
		}

		pb, err := proto.Marshal(&plan)
		if err != nil {
			return err
		}

		plans[i] = pb
	}

	err := dp.deleteStore.PutDeletePlans(ctx, deleteRequest.UserID, deleteRequest.RequestID, plans)
	if err != nil {
		return err
	}

	err = dp.deleteStore.UpdateStatus(ctx, deleteRequest.UserID, deleteRequest.RequestID, chunk.DeleteRequestStatusDeleting)
	if err != nil {
		return err
	}

	level.Debug(util.Logger).Log("msg", "built delete plans and sending delete request for execution",
		"request-id", deleteRequest.RequestID, "user-id", deleteRequest.UserID, "num-plans", len(perDayTimeRange)-1)
	dp.executePlansChan <- deleteRequest

	return nil
}

// returns start, [start of each following days before last day...], end
func splitByDay(start, end model.Time) []model.Time {
	numOfDays := numOfPlans(start, end)

	perDayTimeRange := make([]model.Time, numOfDays+1)

	for i := 0; i < numOfDays; i++ {
		perDayTimeRange[i] = start
		start = model.Time(((int64(start) / millisecondPerDay) + 1) * millisecondPerDay)
	}

	perDayTimeRange[numOfDays] = end

	return perDayTimeRange
}

func numOfPlans(start, end model.Time) int {
	numOfDays := int(int64(end-start) / millisecondPerDay)
	if int64(start)%millisecondPerDay != 0 {
		numOfDays++
	}

	return numOfDays
}

// groups chunks together by unique label sets
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
	if promqlStorageErr, ok := err.(promql.ErrStorage); ok && promqlStorageErr.Err == chunk.ErrChunkNotFound {
		return true
	}

	return false
}
