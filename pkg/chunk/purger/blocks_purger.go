package purger

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	fmt "fmt"
	"net/http"
	"sort"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

type BlocksPurgerAPI struct {
	bucketClient              objstore.Bucket
	logger                    log.Logger
	cfgProvider               bucket.TenantConfigProvider
	deleteRequestCancelPeriod time.Duration
}

func NewBlocksPurgerAPI(storageCfg cortex_tsdb.BlocksStorageConfig, cfgProvider bucket.TenantConfigProvider, logger log.Logger, reg prometheus.Registerer, cancellationPeriod time.Duration) (*BlocksPurgerAPI, error) {
	bucketClient, err := createBucketClient(storageCfg, logger, "blocks-purger", reg)
	if err != nil {
		return nil, err
	}

	return newBlocksPurgerAPI(bucketClient, cfgProvider, logger, cancellationPeriod), nil
}

func newBlocksPurgerAPI(bkt objstore.Bucket, cfgProvider bucket.TenantConfigProvider, logger log.Logger, cancellationPeriod time.Duration) *BlocksPurgerAPI {
	return &BlocksPurgerAPI{
		bucketClient:              bkt,
		cfgProvider:               cfgProvider,
		logger:                    logger,
		deleteRequestCancelPeriod: cancellationPeriod,
	}
}

func (api *BlocksPurgerAPI) AddDeleteRequestHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	params := r.URL.Query()
	match := params["match[]"]
	if len(match) == 0 {
		http.Error(w, "selectors not set", http.StatusBadRequest)
		return
	}

	matchers, err := cortex_tsdb.ParseMatchers(match)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	startParam := params.Get("start")
	startTime := int64(0)
	if startParam != "" {
		startTime, err = util.ParseTime(startParam)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	endParam := params.Get("end")
	endTime := int64(model.Now())

	if endParam != "" {
		endTime, err = util.ParseTime(endParam)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if endTime > int64(model.Now()) {
			http.Error(w, "deletes in future not allowed", http.StatusBadRequest)
			return
		}
	}

	if startTime > endTime {
		http.Error(w, "start time can't be greater than end time", http.StatusBadRequest)
		return
	}

	tManager := cortex_tsdb.NewTombstoneManager(api.bucketClient, userID, api.cfgProvider, api.logger)

	requestID := getTombstoneHash(startTime, endTime, matchers)
	// Since the request id is based on a hash of the parameters, there is a possibility that a tombstone could already exist for it
	// if the request was previously cancelled, we need to remove the cancelled tombstone before adding the pending one
	if err := tManager.RemoveCancelledStateIfExists(ctx, requestID); err != nil {
		level.Error(util_log.Logger).Log("msg", "removing cancelled tombstone state if it exists", "err", err)
		http.Error(w, "Error checking previous delete requests and removing the past cancelled version of this request if it exists ", http.StatusInternalServerError)
		return
	}

	prevT, err := tManager.GetTombstoneByIDForUser(ctx, requestID)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "error getting delete request by id", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if prevT != nil {
		http.Error(w, "delete request tombstone with same information already exists", http.StatusBadRequest)
		return
	}

	curTime := time.Now().Unix() * 1000
	t := cortex_tsdb.NewTombstone(userID, curTime, curTime, startTime, endTime, match, requestID, cortex_tsdb.StatePending)

	if err = tManager.WriteTombstone(ctx, t); err != nil {
		level.Error(util_log.Logger).Log("msg", "error adding delete request to the object store", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (api *BlocksPurgerAPI) GetAllDeleteRequestsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	tManager := cortex_tsdb.NewTombstoneManager(api.bucketClient, userID, api.cfgProvider, api.logger)
	deleteRequests, err := tManager.GetAllTombstonesForUser(ctx)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "error getting delete requests from the block store", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(deleteRequests); err != nil {
		level.Error(util_log.Logger).Log("msg", "error marshalling response", "err", err)
		http.Error(w, fmt.Sprintf("Error marshalling response: %v", err), http.StatusInternalServerError)
		return
	}
}

func (api *BlocksPurgerAPI) CancelDeleteRequestHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	params := r.URL.Query()
	requestID := params.Get("request_id")
	if len(requestID) == 0 {
		http.Error(w, "request_id not set", http.StatusBadRequest)
		return
	}

	tManager := cortex_tsdb.NewTombstoneManager(api.bucketClient, userID, api.cfgProvider, api.logger)
	deleteRequest, err := tManager.GetTombstoneByIDForUser(ctx, requestID)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "error getting delete request from the object store", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if deleteRequest == nil {
		http.Error(w, "could not find delete request with given id", http.StatusBadRequest)
		return
	}

	if deleteRequest.State == cortex_tsdb.StateCancelled {
		http.Error(w, "the series deletion request was cancelled previously", http.StatusAccepted)
		return
	}

	if deleteRequest.State == cortex_tsdb.StateProcessed {
		http.Error(w, "deletion of request which is already processed is not allowed", http.StatusBadRequest)
		return
	}

	if time.Since(deleteRequest.GetCreateTime()) > api.deleteRequestCancelPeriod {
		http.Error(w, fmt.Sprintf("Cancellation of request past the deadline of %s since its creation is not allowed", api.deleteRequestCancelPeriod.String()), http.StatusBadRequest)
		return
	}

	// create file with the cancelled state
	_, err = tManager.UpdateTombstoneState(ctx, deleteRequest, cortex_tsdb.StateCancelled)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "error cancelling the delete request", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func getTombstoneHash(startTime int64, endTime int64, selectors []*labels.Matcher) string {
	// Any delete request with the same start, end time and same selectors should result in the same hash

	hash := md5.New()

	bufStart := make([]byte, 8)
	binary.LittleEndian.PutUint64(bufStart, uint64(startTime))
	bufEnd := make([]byte, 8)
	binary.LittleEndian.PutUint64(bufEnd, uint64(startTime))

	hash.Write(bufStart)
	hash.Write(bufEnd)

	// First we get the strings of the parsed matchers which
	// then are sorted and hashed after. This is done so that logically
	// equivalent deletion requests result in the same hash
	selectorStrings := make([]string, len(selectors))
	for i, s := range selectors {
		selectorStrings[i] = s.String()
	}

	sort.Strings(selectorStrings)
	for _, s := range selectorStrings {
		hash.Write([]byte(s))
	}

	md5Bytes := hash.Sum(nil)
	return hex.EncodeToString(md5Bytes[:])
}
