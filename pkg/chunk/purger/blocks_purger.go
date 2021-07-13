package purger

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	fmt "fmt"
	"net/http"
	"strconv"
	strings "strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"
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
	bucketClient, err := createBucketClient(storageCfg, logger, reg)
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

func createBucketClient(cfg cortex_tsdb.BlocksStorageConfig, logger log.Logger, reg prometheus.Registerer) (objstore.Bucket, error) {
	bucketClient, err := bucket.NewClient(context.Background(), cfg.Bucket, "purger", logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "create bucket client")
	}

	return bucketClient, nil
}

func (api *BlocksPurgerAPI) V2AddDeleteRequestHandler(w http.ResponseWriter, r *http.Request) {

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

	for i := range match {
		_, err := parser.ParseMetricSelector(match[i])
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
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

	requestId := getTombstoneRequestID(startTime, endTime, match)
	curTime := time.Now().Unix()
	t := cortex_tsdb.NewTombstone(userID, curTime, curTime, startTime, endTime, match, requestId, cortex_tsdb.StatePending)

	if err = cortex_tsdb.WriteTombstoneFile(ctx, api.bucketClient, userID, api.cfgProvider, t); err != nil {

		if err == cortex_tsdb.ErrTombstoneAlreadyExists {
			level.Error(util_log.Logger).Log("msg", "delete request tombstone with same information already exists", "err", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		level.Error(util_log.Logger).Log("msg", "error adding delete request to the store", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (api *BlocksPurgerAPI) V2GetAllDeleteRequestsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	deleteRequests, err := cortex_tsdb.GetAllDeleteRequestsForUser(ctx, api.bucketClient, api.cfgProvider, userID)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "error getting delete requests from the block store", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(w).Encode(deleteRequests); err != nil {
		level.Error(util_log.Logger).Log("msg", "error marshalling response", "err", err)
		http.Error(w, fmt.Sprintf("Error marshalling response: %v", err), http.StatusInternalServerError)
	}

	w.WriteHeader(http.StatusOK)

}

func (api *BlocksPurgerAPI) V2CancelDeleteRequestHandler(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	params := r.URL.Query()
	requestID := params.Get("request_id")

	deleteRequest, err := cortex_tsdb.GetDeleteRequestsForUser(ctx, api.bucketClient, api.cfgProvider, userID, requestID)
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
		http.Error(w, "the request has already been previously deleted", http.StatusBadRequest)
		return
	}

	if deleteRequest.State == cortex_tsdb.StateProcessed {
		http.Error(w, "deletion of request which is already processed is not allowed", http.StatusBadRequest)
		return
	}

	currentTime := int64(time.Now().Unix() * 1000)
	timeElapsed := float64(currentTime - deleteRequest.RequestCreatedAt)

	if timeElapsed > float64(api.deleteRequestCancelPeriod.Milliseconds()) {
		http.Error(w, fmt.Sprintf("deletion of request past the deadline of %s since its creation is not allowed", api.deleteRequestCancelPeriod.String()), http.StatusBadRequest)
		return
	}

	// create file with the cancelled state
	_, err = cortex_tsdb.UpgradeTombstoneState(ctx, api.bucketClient, api.cfgProvider, deleteRequest, cortex_tsdb.StateCancelled)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "error cancelling the delete request", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// Calculates the tombstone file name based on a hash of the start time, end time and selectors
func getTombstoneRequestID(startTime int64, endTime int64, selectors []string) string {
	// First make a string of the tombstone info

	var b strings.Builder

	b.WriteString(strconv.FormatInt(startTime, 10))
	b.WriteString(",")
	b.WriteString(strconv.FormatInt(endTime, 10))

	for _, s := range selectors {
		b.WriteString(",")
		b.WriteString(s)
	}

	return getTombstoneHash(b.String())
}

func getTombstoneHash(s string) string {
	data := []byte(s)
	md5Bytes := md5.Sum(data)
	return hex.EncodeToString(md5Bytes[:])
}
