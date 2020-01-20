package chunk

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
)

const (
	// DeleteRequestStatusReceived means no actions done on request yet, just doing query time filtering
	DeleteRequestStatusReceived = "0"
	// DeleteRequestStatusBuildingPlan means request is picked up for processing and building plans for it, still doing query time filtering
	DeleteRequestStatusBuildingPlan = "1"
	// DeleteRequestStatusDeleting means plans built already, running delete operations and still doing query time filtering
	DeleteRequestStatusDeleting = "2"
	// DeleteRequestStatusProcessed means all requested data deleted, not considering this for query time filtering
	DeleteRequestStatusProcessed = "3"
)

var pendingDeleteRequestStatuses = []string{DeleteRequestStatusReceived, DeleteRequestStatusBuildingPlan, DeleteRequestStatusDeleting}

// DeleteRequest holds all the details about a delete request
type DeleteRequest struct {
	RequestID string              `json:"request_id"`
	UserID    string              `json:"-"`
	StartTime model.Time          `json:"start_time"`
	EndTime   model.Time          `json:"end_time"`
	Selectors []string            `json:"selectors"`
	Status    string              `json:"status"`
	Matchers  [][]*labels.Matcher `json:"-"`
	CreatedAt model.Time          `json:"created_at"`
}

// DeleteStore provides all the methods required to manage lifecycle of delete request and things related to it
type DeleteStore interface {
	Add(ctx context.Context, userID string, startTime, endTime model.Time, selectors []string) error
	GetDeleteRequestsByStatus(ctx context.Context, status string) ([]DeleteRequest, error)
	GetDeleteRequestsForUserByStatus(ctx context.Context, userID string, status string) ([]DeleteRequest, error)
	GetPendingDeleteRequestsForUser(ctx context.Context, userID string) ([]DeleteRequest, error)
	UpdateStatus(ctx context.Context, userID, requestID string, newStatus string) error
	GetDeleteRequest(ctx context.Context, userID, requestID string) (*DeleteRequest, error)
	GetAllDeleteRequestsForUser(ctx context.Context, userID string) ([]DeleteRequest, error)

	PutDeletePlans(ctx context.Context, userID, requestID string, plans [][]byte) error
	GetDeletePlan(ctx context.Context, userID, requestID string, planNumber int) ([]byte, error)
	RemoveDeletePlan(ctx context.Context, userID, requestID string, planNumber int) error
}

type deleteStore struct {
	cfg         DeleteStoreConfig
	indexClient IndexClient
}

// DeleteStoreConfig holds configuration for delete store
type DeleteStoreConfig struct {
	Store             string `yaml:"store"`
	RequestsTableName string `yaml:"requests_table_name"`
	PlansTableName    string `yaml:"plans_table_name"`
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *DeleteStoreConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Store, "deletes.store", "", "Store for keeping delete request")
	f.StringVar(&cfg.RequestsTableName, "deletes.requests-table-name", "delete_requests", "Name of the table which stores delete requests")
	f.StringVar(&cfg.PlansTableName, "deletes.plans-table-name", "delete_plans", "Name of the table which stores delete plans")
}

// NewDeleteStore creates a store for managing delete requests
func NewDeleteStore(cfg DeleteStoreConfig, indexClient IndexClient) (DeleteStore, error) {
	ds := deleteStore{
		cfg:         cfg,
		indexClient: indexClient,
	}

	return &ds, nil
}

// Add creates entries for a new delete request
func (ds *deleteStore) Add(ctx context.Context, userID string, startTime, endTime model.Time, selectors []string) error {
	requestID := generateUniqueID(userID, selectors)

	// userID, requestID
	rangeValue := encodeRangeKey([]byte(userID), requestID)

	batch := ds.indexClient.NewWriteBatch()
	batch.Add(ds.cfg.RequestsTableName, "", rangeValue, []byte(DeleteRequestStatusReceived))
	batch.Add(ds.cfg.RequestsTableName, fmt.Sprintf("%s:%s", userID, requestID),
		encodeRangeKey(encodeUint64Time(uint64(model.Now())), encodeUint64Time(uint64(startTime)), encodeUint64Time(uint64(endTime))),
		[]byte(strings.Join(selectors, "&")))

	return ds.indexClient.BatchWrite(ctx, batch)
}

// GetDeleteRequestsByStatus returns all delete requests for given status
func (ds *deleteStore) GetDeleteRequestsByStatus(ctx context.Context, status string) ([]DeleteRequest, error) {
	return ds.queryDeleteRequests(ctx, []IndexQuery{{TableName: ds.cfg.RequestsTableName, ValueEqual: []byte(status)}})
}

// GetDeleteRequestsForUserByStatus returns all delete requests for a user with given status
func (ds *deleteStore) GetDeleteRequestsForUserByStatus(ctx context.Context, userID string, status string) ([]DeleteRequest, error) {
	return ds.queryDeleteRequests(ctx, []IndexQuery{
		{TableName: ds.cfg.RequestsTableName, RangeValuePrefix: encodeRangeKey([]byte(userID)), ValueEqual: []byte(status)},
	})
}

// GetAllDeleteRequestsForUser returns all delete requests for a user
func (ds *deleteStore) GetAllDeleteRequestsForUser(ctx context.Context, userID string) ([]DeleteRequest, error) {
	return ds.queryDeleteRequests(ctx, []IndexQuery{
		{TableName: ds.cfg.RequestsTableName, RangeValuePrefix: encodeRangeKey([]byte(userID))},
	})
}

// UpdateStatus updates status of a delete request
func (ds *deleteStore) UpdateStatus(ctx context.Context, userID, requestID string, newStatus string) error {
	batch := ds.indexClient.NewWriteBatch()

	rangeValue := encodeRangeKey([]byte(userID), []byte(requestID))
	batch.Update(ds.cfg.RequestsTableName, "", rangeValue, []byte(newStatus))

	return ds.indexClient.BatchWrite(ctx, batch)
}

// GetDeleteRequest returns delete request with given requestID
func (ds *deleteStore) GetDeleteRequest(ctx context.Context, userID, requestID string) (*DeleteRequest, error) {
	deleteRequests, err := ds.queryDeleteRequests(ctx, []IndexQuery{
		{TableName: ds.cfg.RequestsTableName, RangeValuePrefix: encodeRangeKey([]byte(userID), []byte(requestID))},
	})

	if err != nil {
		return nil, err
	}

	if len(deleteRequests) == 0 {
		return nil, nil
	}

	return &deleteRequests[0], nil
}

// GetPendingDeleteRequestsForUser returns all delete requests for a user which are not processed
func (ds *deleteStore) GetPendingDeleteRequestsForUser(ctx context.Context, userID string) ([]DeleteRequest, error) {
	pendingDeleteRequests := []DeleteRequest{}
	for _, status := range pendingDeleteRequestStatuses {
		deleteRequests, err := ds.GetDeleteRequestsForUserByStatus(ctx, userID, status)
		if err != nil {
			return nil, err
		}

		pendingDeleteRequests = append(pendingDeleteRequests, deleteRequests...)
	}

	return pendingDeleteRequests, nil
}

// PutDeletePlans adds delete plans built from a delete request
func (ds *deleteStore) PutDeletePlans(ctx context.Context, userID, requestID string, plans [][]byte) error {
	planIDPrefix := fmt.Sprintf("%s:%s", userID, requestID)

	batch := ds.indexClient.NewWriteBatch()
	for i := range plans {
		batch.Add(ds.cfg.PlansTableName, fmt.Sprintf("%s:%d", planIDPrefix, i), nil, plans[i])
	}

	return ds.indexClient.BatchWrite(ctx, batch)
}

// GetDeletePlan returns delete plan with given plan number, if available
func (ds *deleteStore) GetDeletePlan(ctx context.Context, userID, requestID string, planNumber int) ([]byte, error) {
	var plan []byte
	planID := fmt.Sprintf("%s:%s:%d", userID, requestID, planNumber)

	query := []IndexQuery{
		{TableName: ds.cfg.PlansTableName, HashValue: planID},
	}

	err := ds.indexClient.QueryPages(ctx, query, func(query IndexQuery, batch ReadBatch) (shouldContinue bool) {
		itr := batch.Iterator()
		for itr.Next() {
			plan = itr.Value()
			break
		}
		return false
	})

	if err != nil {
		return nil, err
	}

	return plan, nil
}

// RemoveDeletePlan removes delete plan with given plan number
func (ds *deleteStore) RemoveDeletePlan(ctx context.Context, userID, requestID string, planNumber int) error {
	planID := fmt.Sprintf("%s:%s:%d", userID, requestID, planNumber)

	batch := ds.indexClient.NewWriteBatch()
	batch.Delete(ds.cfg.PlansTableName, planID, nil)

	return ds.indexClient.BatchWrite(ctx, batch)
}

func (ds *deleteStore) queryDeleteRequests(ctx context.Context, deleteQuery []IndexQuery) ([]DeleteRequest, error) {
	deleteRequests := []DeleteRequest{}
	err := ds.indexClient.QueryPages(ctx, deleteQuery, func(query IndexQuery, batch ReadBatch) (shouldContinue bool) {
		itr := batch.Iterator()
		for itr.Next() {
			rangeKey := decodeRangeKey(itr.RangeValue())
			deleteRequest := DeleteRequest{
				UserID:    string(rangeKey[0]),
				RequestID: string(rangeKey[1]),
				Status:    string(itr.Value()),
			}
			deleteRequests = append(deleteRequests, deleteRequest)
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	for i, deleteRequest := range deleteRequests {
		deleteRequestQuery := []IndexQuery{{TableName: ds.cfg.RequestsTableName, HashValue: fmt.Sprintf("%s:%s", deleteRequest.UserID, deleteRequest.RequestID)}}
		err := ds.indexClient.QueryPages(ctx, deleteRequestQuery, func(query IndexQuery, batch ReadBatch) (shouldContinue bool) {
			itr := batch.Iterator()
			itr.Next()
			rangeKey := decodeRangeKey(itr.RangeValue())

			deleteRequests[i].CreatedAt = model.Time(decodeUint64Time(rangeKey[0]))
			deleteRequests[i].StartTime = model.Time(decodeUint64Time(rangeKey[1]))
			deleteRequests[i].EndTime = model.Time(decodeUint64Time(rangeKey[2]))
			deleteRequests[i].Selectors = strings.Split(string(itr.Value()), "&")

			return true
		})

		if err != nil {
			return nil, err
		}
	}

	return deleteRequests, nil
}

func encodeUint64Time(t uint64) []byte {
	// timestamps are hex encoded such that it doesn't contain null byte,
	// but is still lexicographically sortable.
	throughBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(throughBytes, t)
	encodedThroughBytes := make([]byte, 16)
	hex.Encode(encodedThroughBytes, throughBytes)
	return encodedThroughBytes
}

func decodeUint64Time(bs []byte) uint64 {
	buf := make([]byte, 8)
	_, _ = hex.Decode(buf, bs)
	return binary.BigEndian.Uint64(buf)
}

// An id is useful in managing delete requests
func generateUniqueID(orgID string, selectors []string) []byte {
	uniqueID := fnv.New32()
	_, _ = uniqueID.Write([]byte(orgID))

	timeNow := make([]byte, 8)
	binary.LittleEndian.PutUint64(timeNow, uint64(time.Now().UnixNano()))
	_, _ = uniqueID.Write(timeNow)

	for _, selector := range selectors {
		_, _ = uniqueID.Write([]byte(selector))
	}

	return encodeUniqueID(uniqueID.Sum32())
}

func encodeUniqueID(t uint32) []byte {
	throughBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(throughBytes, t)
	encodedThroughBytes := make([]byte, 8)
	hex.Encode(encodedThroughBytes, throughBytes)
	return encodedThroughBytes
}
