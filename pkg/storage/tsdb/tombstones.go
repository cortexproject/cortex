package tsdb

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"path/filepath"
	"time"

	"github.com/go-kit/kit/log"
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
	ErrTombstoneAlreadyExists      = errors.New("The deletion tombstone with the same request information already exists")
	ErrInvalidDeletionRequestState = errors.New("Deletion request filename extension indicating the state is invalid")
	ErrTombstoneNotFound           = errors.New("Tombstone file not found in the object store")
	ErrTombstoneDecode             = errors.New("Unable to read tombstone contents from file")
	AllDeletionStates              = []BlockDeleteRequestState{StatePending, StateProcessed, StateCancelled}
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
	State            BlockDeleteRequestState `json:"state"`
}

func NewTombstone(userID string, requestTime int64, stateTime int64, startTime int64, endTime int64, selectors []string, requestID string, state BlockDeleteRequestState) *Tombstone {
	return &Tombstone{
		RequestCreatedAt: requestTime,
		StateCreatedAt:   stateTime,
		StartTime:        startTime,
		EndTime:          endTime,
		Selectors:        selectors,
		UserID:           userID,
		RequestID:        requestID,
		State:            state,
	}
}

// TombstoneManager is responsible for reading and writing tombstone files to the bucket.
type TombstoneManager struct {
	bkt    objstore.InstrumentedBucket
	logger log.Logger
}

func NewTombstoneManager(
	bkt objstore.Bucket,
	userID string,
	cfgProvider bucket.TenantConfigProvider,
	logger log.Logger) *TombstoneManager {

	return &TombstoneManager{
		bkt:    bucket.NewUserBucketClient(userID, bkt, cfgProvider),
		logger: util_log.WithUserID(userID, logger),
	}
}

// Uploads a tombstone file to object sotre
func (m *TombstoneManager) WriteTombstoneFile(ctx context.Context, tombstone *Tombstone) error {
	data, err := json.Marshal(tombstone)
	if err != nil {
		return errors.Wrap(err, "serialize tombstone")
	}

	fullTombstonePath := path.Join(TombstonePath, getTombstoneFileName(tombstone.RequestID, tombstone.State))

	// Check if the tombstone already exists for the same state. Could be the case the same request was made
	// and is already in the middle of deleting series. Creating a new tombstone would restart
	// the progress
	tombstoneExists, err := m.TombstoneExists(ctx, tombstone.RequestID, tombstone.State)
	if err != nil {
		level.Error(m.logger).Log("msg", "unable to check if the same tombstone already exists", "requestID", tombstone.RequestID, "err", err)
	} else if tombstoneExists {
		return ErrTombstoneAlreadyExists
	}

	return errors.Wrap(m.bkt.Upload(ctx, fullTombstonePath, bytes.NewReader(data)), "upload tombstone file")
}

func (m *TombstoneManager) TombstoneExists(ctx context.Context, requestID string, state BlockDeleteRequestState) (bool, error) {
	fullTombstonePath := path.Join(TombstonePath, getTombstoneFileName(requestID, state))
	exists, err := m.bkt.Exists(ctx, fullTombstonePath)

	if exists || err != nil {
		return exists, err
	}

	return false, nil
}

func (m *TombstoneManager) GetDeleteRequestByIDForUser(ctx context.Context, requestID string) (*Tombstone, error) {
	found := []*Tombstone{}

	for _, state := range AllDeletionStates {
		filename := getTombstoneFileName(requestID, state)
		exists, err := m.TombstoneExists(ctx, requestID, state)
		if err != nil {
			return nil, err
		}

		if exists {
			t, err := m.ReadTombstoneFile(ctx, path.Join(TombstonePath, filename))
			if err != nil {
				return nil, err
			}
			found = append(found, t)
		}
	}

	if len(found) == 0 {
		return nil, nil
	}

	// If there are multiple tombstones with the same request id but different state, want to return only the latest one
	// The older states will be cleaned up by the compactor
	return found[len(found)-1], nil

}

func (m *TombstoneManager) GetAllDeleteRequestsForUser(ctx context.Context) ([]*Tombstone, error) {
	// add all the tombstones to a map and check for duplicates,
	// if a key exists with the same request ID (but two different states)
	tombstoneMap := make(map[string]*Tombstone)
	err := m.bkt.Iter(ctx, TombstonePath, func(s string) error {
		t, err := m.ReadTombstoneFile(ctx, s)
		if err != nil {
			return err
		}

		if _, exists := tombstoneMap[t.RequestID]; !exists {
			tombstoneMap[t.RequestID] = t
		} else {
			// if there is more than one tombstone for a given request,
			// we only want to return the latest state. The older file
			// will be cleaned by the compactor
			newT, err := m.getLatestTombstateByState(t, tombstoneMap[t.RequestID])
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

func (m *TombstoneManager) getLatestTombstateByState(a *Tombstone, b *Tombstone) (*Tombstone, error) {
	orderA, err := a.State.GetStateOrder()
	if err != nil {
		return nil, err
	}

	orderB, err := b.State.GetStateOrder()
	if err != nil {
		return nil, err
	}

	if orderB > orderA {
		return b, nil
	}

	return a, nil
}

func (m *TombstoneManager) ReadTombstoneFile(ctx context.Context, tombstonePath string) (*Tombstone, error) {
	_, _, err := GetTombstoneStateAndRequestIDFromPath(tombstonePath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get the requestID and state from filename: %s", tombstonePath)
	}

	r, err := m.bkt.Get(ctx, tombstonePath)
	if m.bkt.IsObjNotFoundErr(err) {
		return nil, errors.Wrapf(ErrTombstoneNotFound, "tombstone file not found %s", tombstonePath)
	}
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
		return nil, errors.Wrapf(ErrTombstoneDecode, "failed to decode tombstone object: %s, err: %v", tombstonePath, err.Error())
	}

	tombstone.Matchers, err = ParseMatchers(tombstone.Selectors)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse tombstone selectors for: %s", tombstonePath)
	}

	return tombstone, nil
}

func (m *TombstoneManager) UpdateTombstoneState(ctx context.Context, t *Tombstone, newState BlockDeleteRequestState) (*Tombstone, error) {
	userLogger := util_log.WithUserID(t.UserID, util_log.Logger)
	// Create the new tombstone, and will delete the previous tombstone
	newT := NewTombstone(t.UserID, t.RequestCreatedAt, time.Now().Unix()*1000, t.StartTime, t.EndTime, t.Selectors, t.RequestID, newState)
	newT.Matchers = t.Matchers

	err := m.WriteTombstoneFile(ctx, newT)
	if err != nil {
		level.Error(userLogger).Log("msg", "error creating file tombstone file with the updated state", "err", err)
		return nil, err
	}

	if err = m.DeleteTombstoneFile(ctx, t.RequestID, t.State); err != nil {
		level.Error(userLogger).Log("msg", "Created file with updated state but unable to delete previous state. Will retry next time tombstones are loaded", "err", err)
	}

	return newT, nil
}

func (m *TombstoneManager) DeleteTombstoneFile(ctx context.Context, requestID string, state BlockDeleteRequestState) error {
	filename := getTombstoneFileName(requestID, state)
	fullTombstonePath := path.Join(TombstonePath, filename)

	level.Info(m.logger).Log("msg", "Deleting tombstone file", "file", fullTombstonePath)

	return errors.Wrap(m.bkt.Delete(ctx, fullTombstonePath), "delete tombstone file")

}

func (m *TombstoneManager) RemoveCancelledStateIfExists(ctx context.Context, requestID string) error {
	exists, err := m.TombstoneExists(ctx, requestID, StateCancelled)
	if err != nil {
		level.Error(m.logger).Log("msg", "unable to check if the request has previously been cancelled", "requestID", requestID, "err", err)
		return err
	}

	if exists {
		if err = m.DeleteTombstoneFile(ctx, requestID, StateCancelled); err != nil {
			level.Error(m.logger).Log("msg", "unable to delete tombstone with previously cancelled state", "requestID", requestID, "err", err)
			return err
		}
		level.Info(m.logger).Log("msg", "Removing tombstone file with previously cancelled state", "requestID", requestID, "err", err)

	}
	return nil
}

func GetTombstoneStateAndRequestIDFromPath(tombstonePath string) (string, BlockDeleteRequestState, error) {
	// This should get the first extension which is .json
	filenameExtesion := filepath.Ext(tombstonePath)
	filenameWithoutJSON := tombstonePath[0 : len(tombstonePath)-len(filenameExtesion)]

	stateExtension := filepath.Ext(filenameWithoutJSON)
	requestID := filenameWithoutJSON[0 : len(filenameWithoutJSON)-len(stateExtension)]

	// Ensure that the state exists as the filename extension
	if len(stateExtension) == 0 {
		return "", "", ErrInvalidDeletionRequestState
	}

	state := BlockDeleteRequestState(stateExtension[1:])
	if !isValidDeleteRequestState(state) {
		return "", "", errors.Wrapf(ErrInvalidDeletionRequestState, "Filename extension is invalid for tombstone: %s", tombstonePath)

	}

	return requestID, state, nil
}

func ParseMatchers(selectors []string) ([]*labels.Matcher, error) {
	// Convert the string selectors to label matchers
	var m []*labels.Matcher

	for _, selector := range selectors {
		parsed, err := parser.ParseMetricSelector(selector)
		if err != nil {
			return nil, errors.Wrapf(err, "error parsing metric selector")
		}
		//keep the matchers in a 1D array because the deletions are applied based
		// on the "and" between all matchers. There is no need to
		m = append(m, parsed...)
	}

	return m, nil
}

func (t *Tombstone) GetFilename() string {
	return t.RequestID + "." + string(t.State) + ".json"
}

func (t *Tombstone) IsOverlappingInterval(minT int64, maxT int64) bool {
	return t.StartTime <= maxT && minT < t.EndTime
}

func (t *Tombstone) GetCreateTime() time.Time {
	return time.Unix(t.RequestCreatedAt/1000, 0)
}

func getTombstoneFileName(requestID string, state BlockDeleteRequestState) string {
	return requestID + "." + string(state) + ".json"
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

func (s BlockDeleteRequestState) GetStateOrder() (int, error) {
	switch s {
	case StatePending:
		return 0, nil
	case StateProcessed:
		return 2, nil
	case StateCancelled:
		return 3, nil
	}

	return -1, ErrInvalidDeletionRequestState
}
