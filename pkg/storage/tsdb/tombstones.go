package tsdb

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"path/filepath"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

type BlockDeleteRequestState string

const (
	StatePending   BlockDeleteRequestState = "pending"
	StateProcessed BlockDeleteRequestState = "processed"
	StateCancelled BlockDeleteRequestState = "deleted"
)

// Relative to user-specific prefix.
const TombstonePath = "tombstones/"

var (
	ErrTombstoneAlreadyExists      = errors.New("the deletion tombstone with the same request information already exists")
	ErrInvalidDeletionRequestState = errors.New("Deletion request filename extension indicating the state is invalid")

	PossibleDeletionState = []BlockDeleteRequestState{StatePending, StateProcessed, StateCancelled}
)

type Tombstone struct {
	RequestCreatedAt int64                   `json:"request_created_at"`
	StateCreatedAt   int64                   `json:"state_created_at"`
	RequestID        string                  `json:"request_id"`
	StartTime        int64                   `json:"start_time"`
	EndTime          int64                   `json:"end_time"`
	Selectors        []string                `json:"selectors"`
	Matchers         []*labels.Matcher       `json:"-"`
	UserID           string                  `json:"user_id"`
	State            BlockDeleteRequestState `json:"-"`
}

func NewTombstone(userID string, requestTime int64, stateTime int64, startTime int64, endTime int64, selectors []string, requestId string, state BlockDeleteRequestState) *Tombstone {
	return &Tombstone{
		RequestCreatedAt: requestTime,
		StateCreatedAt:   stateTime,
		StartTime:        startTime,
		EndTime:          endTime,
		Selectors:        selectors,
		UserID:           userID,
		RequestID:        requestId,
		State:            state,
	}
}

// Uploads a tombstone file to object sotre
func WriteTombstoneFile(ctx context.Context, bkt objstore.Bucket, userID string, cfgProvider bucket.TenantConfigProvider, tombstone *Tombstone) error {
	userBkt := bucket.NewUserBucketClient(userID, bkt, cfgProvider)

	data, err := json.Marshal(tombstone)
	if err != nil {
		return errors.Wrap(err, "serialize tombstone")
	}

	// Add the state of the request using the filename extension
	filename := tombstone.RequestID + ".json." + string(tombstone.State)

	fullTombstonePath := path.Join(TombstonePath, filename)

	// Check if the tombstone already exists. Could be the case the same request was made
	// and is already in the middle of deleting series. Creating a new tombstone would restart
	// the progress
	tombstoneExists, err := TombstoneExists(ctx, userBkt, userID, tombstone.RequestID, tombstone.State)
	if err != nil {
		level.Warn(util_log.Logger).Log("msg", "unable to check if the same tombstone already exists", "user", userID, "requestID", tombstone.RequestID, "err", err)
	} else if tombstoneExists {
		return ErrTombstoneAlreadyExists
	}

	return errors.Wrap(userBkt.Upload(ctx, fullTombstonePath, bytes.NewReader(data)), "upload tombstone file")
}

func TombstoneExists(ctx context.Context, bkt objstore.BucketReader, userID string, requestID string, state BlockDeleteRequestState) (bool, error) {
	// Add the state of the request using the filename extension
	filename := requestID + ".json." + string(state)
	fullTombstonePath := path.Join(TombstonePath, filename)

	exists, err := bkt.Exists(ctx, fullTombstonePath)

	if exists || err != nil {
		return exists, err
	}

	return false, nil
}

func GetDeleteRequestsForUser(ctx context.Context, bkt objstore.Bucket, cfgProvider bucket.TenantConfigProvider, userID string, requestId string) (*Tombstone, error) {
	userBucket := bucket.NewUserBucketClient(userID, bkt, cfgProvider)

	var deleteRequest *Tombstone = nil

	for _, state := range PossibleDeletionState {
		filename := requestId + ".json." + string(state)

		exists, err := TombstoneExists(ctx, userBucket, userID, requestId, state)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}

		t, err := readTombstoneFile(ctx, userBucket, userID, path.Join(TombstonePath, filename))
		if err != nil {
			return nil, err
		}

		//checking if there exists more than one file for the same tombstone
		// If it is the case, then should remove the one with the older state
		if deleteRequest == nil {
			deleteRequest = t
		} else {
			deleteRequest, err = removeDuplicateTombstone(ctx, bkt, cfgProvider, t.UserID, t, deleteRequest)
			if err != nil {
				return nil, err
			}
		}
	}

	return deleteRequest, nil
}

func GetAllDeleteRequestsForUser(ctx context.Context, bkt objstore.Bucket, cfgProvider bucket.TenantConfigProvider, userID string) ([]*Tombstone, error) {
	userBucket := bucket.NewUserBucketClient(userID, bkt, cfgProvider)

	// add all the tombstones to a map and check for duplicates,
	// if a key exists with the same request ID (but two different states)
	tombstoneMap := make(map[string]*Tombstone)
	err := userBucket.Iter(ctx, "tombstones/", func(s string) error {
		t, err := readTombstoneFile(ctx, userBucket, userID, s)
		if err != nil {
			return err
		}

		if _, exists := tombstoneMap[t.RequestID]; !exists {
			tombstoneMap[t.RequestID] = t
		} else {
			newT, err := removeDuplicateTombstone(ctx, bkt, cfgProvider, t.UserID, t, tombstoneMap[t.RequestID])
			if err != nil {
				return err
			}
			tombstoneMap[t.RequestID] = newT
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	deletionRequests := []*Tombstone{}
	for _, t := range tombstoneMap {
		deletionRequests = append(deletionRequests, t)
	}

	return deletionRequests, nil
}

func removeDuplicateTombstone(ctx context.Context, bkt objstore.Bucket, cfgProvider bucket.TenantConfigProvider, userID string, tombstoneA *Tombstone, tombstoneB *Tombstone) (*Tombstone, error) {
	userLogger := util_log.WithUserID(userID, util_log.Logger)

	if tombstoneA.RequestID != tombstoneB.RequestID {
		return nil, errors.New("Cannot run the remove duplicate function for 2 tombstones with different request ID's")
	} else if tombstoneA.State == tombstoneB.State {
		return tombstoneA, nil
	}

	level.Info(userLogger).Log("msg", "Found multiple tombstones for the same deletion request ID but with different state'", "request ID", tombstoneA.RequestID)

	//find which tombstone should be removed
	var toDelete, toKeep *Tombstone

	orderA, err := tombstoneA.GetStateOrder()
	if err != nil {
		return nil, err
	}

	orderB, err := tombstoneB.GetStateOrder()
	if err != nil {
		return nil, err
	}

	if orderA > orderB {
		toKeep = tombstoneA
		toDelete = tombstoneB
	} else {
		toKeep = tombstoneB
		toDelete = tombstoneA
	}

	level.Info(userLogger).Log("msg", "For deletion request %s", toDelete.RequestID, ", removing %s", toDelete.State, "state file, while keeping .", string(toKeep.State), "state file")
	err = DeleteTombstoneFile(ctx, bkt, cfgProvider, toDelete)

	if err != nil {
		return nil, errors.Wrap(err, "failed to delete duplicate tombstone")
	}

	return toKeep, nil
}

func readTombstoneFile(ctx context.Context, bkt objstore.BucketReader, userID string, tombstonePath string) (*Tombstone, error) {
	filenameExtesion := filepath.Ext(tombstonePath)

	// Ensure that the state exists as the filename extension
	if len(filenameExtesion) == 0 {
		return nil, ErrInvalidDeletionRequestState
	}

	state := BlockDeleteRequestState(filenameExtesion[1:])

	if !isValidDeleteRequestState(state) {
		return nil, errors.Wrapf(ErrInvalidDeletionRequestState, "Filename extension is invalid for tombstone: %s", tombstonePath)

	}

	r, err := bkt.Get(ctx, tombstonePath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read tombstone object: %s", tombstonePath)
	}

	tombstone := &Tombstone{}
	err = json.NewDecoder(r).Decode(tombstone)

	// Close reader before dealing with decode error.
	if closeErr := r.Close(); closeErr != nil {
		level.Warn(util_log.Logger).Log("msg", "failed to close bucket reader", "err", closeErr)
	}

	if err != nil {
		return nil, errors.Wrapf(err, "failed to decode tombstone object: %s", tombstonePath)
	}

	tombstone.State = BlockDeleteRequestState(state)

	// Convert the string selectors to label matchers
	var parsedMatcher []*labels.Matcher

	for _, selector := range tombstone.Selectors {
		parsedMatcher, err = parser.ParseMetricSelector(selector)
		if err != nil {
			return nil, errors.Wrapf(err, "error parsing metric selector")
		}
		tombstone.Matchers = append(tombstone.Matchers, parsedMatcher...)
	}

	return tombstone, nil
}

func UpgradeTombstoneState(ctx context.Context, bkt objstore.Bucket, cfgProvider bucket.TenantConfigProvider, t *Tombstone, newState BlockDeleteRequestState) (*Tombstone, error) {
	// Create the new tombstone, and will delete the previous tombstone
	newT := NewTombstone(t.UserID, t.RequestCreatedAt, time.Now().Unix(), t.StartTime, t.EndTime, t.Selectors, t.RequestID, newState)

	err := WriteTombstoneFile(ctx, bkt, newT.UserID, cfgProvider, newT)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "error creating file tombstone file with the updated state", "err", err)
		return nil, err
	}

	err = DeleteTombstoneFile(ctx, bkt, cfgProvider, t)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "Created file with updated state but unable to delete previous state. Will retry next time tombstones are loaded", "err", err)
	}

	return newT, nil
}

func DeleteTombstoneFile(ctx context.Context, bkt objstore.Bucket, cfgProvider bucket.TenantConfigProvider, t *Tombstone) error {
	userLogger := util_log.WithUserID(t.UserID, util_log.Logger)
	userBucket := bucket.NewUserBucketClient(t.UserID, bkt, cfgProvider)

	// Create the full path of the tombstone by appending the state as the extension
	filename := t.RequestID + ".json." + string(t.State)
	fullTombstonePath := path.Join(TombstonePath, filename)

	level.Info(userLogger).Log("msg", "Deleting tombstone file", "file", fullTombstonePath)

	return errors.Wrap(userBucket.Delete(ctx, fullTombstonePath), "delete tombstone file")

}

func isValidDeleteRequestState(state BlockDeleteRequestState) bool {
	switch state {
	case
		StatePending,
		StateProcessed,
		StateCancelled:
		return true
	}
	return false
}

func (t *Tombstone) GetStateOrder() (int, error) {
	switch t.State {
	case StatePending:
		return 0, nil
	case StateProcessed:
		return 2, nil
	case StateCancelled:
		return 3, nil
	}

	return -1, ErrInvalidDeletionRequestState
}
