package compactor

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/compact"

	cortex_testutil "github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
)

func TestMarkPending(t *testing.T) {
	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
	logger := log.NewNopLogger()

	ownerIdentifier := "test-owner"
	testVisitMarker := NewTestVisitMarker(ownerIdentifier)

	visitMarkerManager := NewVisitMarkerManager(objstore.WithNoopInstr(bkt), logger, ownerIdentifier, testVisitMarker)
	visitMarkerManager.MarkWithStatus(ctx, Pending)

	require.Equal(t, Pending, testVisitMarker.Status)

	visitMarkerFromFile := &TestVisitMarker{}
	err := visitMarkerManager.ReadVisitMarker(ctx, visitMarkerFromFile)
	require.NoError(t, err)
	require.Equal(t, Pending, visitMarkerFromFile.Status)
}

func TestMarkInProgress(t *testing.T) {
	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
	logger := log.NewNopLogger()

	ownerIdentifier := "test-owner"
	testVisitMarker := NewTestVisitMarker(ownerIdentifier)

	visitMarkerManager := NewVisitMarkerManager(objstore.WithNoopInstr(bkt), logger, ownerIdentifier, testVisitMarker)
	visitMarkerManager.MarkWithStatus(ctx, InProgress)

	require.Equal(t, InProgress, testVisitMarker.Status)

	visitMarkerFromFile := &TestVisitMarker{}
	err := visitMarkerManager.ReadVisitMarker(ctx, visitMarkerFromFile)
	require.NoError(t, err)
	require.Equal(t, InProgress, visitMarkerFromFile.Status)
}

func TestMarkCompleted(t *testing.T) {
	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
	logger := log.NewNopLogger()

	ownerIdentifier := "test-owner"
	testVisitMarker := NewTestVisitMarker(ownerIdentifier)

	visitMarkerManager := NewVisitMarkerManager(objstore.WithNoopInstr(bkt), logger, ownerIdentifier, testVisitMarker)
	visitMarkerManager.MarkWithStatus(ctx, Completed)

	require.Equal(t, Completed, testVisitMarker.Status)

	visitMarkerFromFile := &TestVisitMarker{}
	err := visitMarkerManager.ReadVisitMarker(ctx, visitMarkerFromFile)
	require.NoError(t, err)
	require.Equal(t, Completed, visitMarkerFromFile.Status)
}

func TestUpdateExistingVisitMarker(t *testing.T) {
	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
	logger := log.NewNopLogger()

	ownerIdentifier1 := "test-owner-1"
	testVisitMarker1 := NewTestVisitMarker(ownerIdentifier1)
	visitMarkerManager1 := NewVisitMarkerManager(objstore.WithNoopInstr(bkt), logger, ownerIdentifier1, testVisitMarker1)
	visitMarkerManager1.MarkWithStatus(ctx, InProgress)

	ownerIdentifier2 := "test-owner-2"
	testVisitMarker2 := &TestVisitMarker{
		OwnerIdentifier: ownerIdentifier2,
		markerID:        testVisitMarker1.markerID,
		StoredValue:     testVisitMarker1.StoredValue,
	}
	visitMarkerManager2 := NewVisitMarkerManager(objstore.WithNoopInstr(bkt), logger, ownerIdentifier2, testVisitMarker2)
	visitMarkerManager2.MarkWithStatus(ctx, Completed)

	visitMarkerFromFile := &TestVisitMarker{}
	err := visitMarkerManager2.ReadVisitMarker(ctx, visitMarkerFromFile)
	require.NoError(t, err)
	require.Equal(t, ownerIdentifier2, visitMarkerFromFile.OwnerIdentifier)
	require.Equal(t, Completed, visitMarkerFromFile.Status)
}

func TestHeartBeat(t *testing.T) {
	for _, tcase := range []struct {
		name           string
		isCancelled    bool
		callerErr      error
		expectedStatus VisitStatus
		deleteOnExit   bool
	}{
		{
			name:           "heart beat got cancelled",
			isCancelled:    true,
			callerErr:      nil,
			expectedStatus: Pending,
			deleteOnExit:   false,
		},
		{
			name:           "heart beat complete without error",
			isCancelled:    false,
			callerErr:      nil,
			expectedStatus: Completed,
			deleteOnExit:   false,
		},
		{
			name:           "heart beat stopped due to halt error",
			isCancelled:    false,
			callerErr:      compact.HaltError{},
			expectedStatus: Failed,
			deleteOnExit:   false,
		},
		{
			name:           "heart beat stopped due to non halt error",
			isCancelled:    false,
			callerErr:      fmt.Errorf("some error"),
			expectedStatus: Pending,
			deleteOnExit:   false,
		},
		{
			name:           "heart beat got cancelled and delete visit marker on exit",
			isCancelled:    true,
			callerErr:      nil,
			expectedStatus: Pending,
			deleteOnExit:   true,
		},
		{
			name:           "heart beat complete without error and delete visit marker on exit",
			isCancelled:    false,
			callerErr:      nil,
			expectedStatus: Completed,
			deleteOnExit:   true,
		},
		{
			name:           "heart beat stopped due to caller error and delete visit marker on exit",
			isCancelled:    false,
			callerErr:      fmt.Errorf("some error"),
			expectedStatus: Failed,
			deleteOnExit:   true,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
			logger := log.NewNopLogger()
			errChan := make(chan error, 1)

			ownerIdentifier := "test-owner"
			testVisitMarker := NewTestVisitMarker(ownerIdentifier)
			resultTestVisitMarker := CopyTestVisitMarker(testVisitMarker)
			visitMarkerManager := NewVisitMarkerManager(objstore.WithNoopInstr(bkt), logger, ownerIdentifier, testVisitMarker)
			go visitMarkerManager.HeartBeat(ctx, errChan, time.Second, tcase.deleteOnExit)

			time.Sleep(2 * time.Second)
			if tcase.isCancelled {
				cancel()
			} else {
				errChan <- tcase.callerErr
				defer cancel()
			}
			time.Sleep(2 * time.Second)

			if tcase.deleteOnExit {
				exists, err := bkt.Exists(context.Background(), testVisitMarker.GetVisitMarkerFilePath())
				require.NoError(t, err)
				require.False(t, exists)
			} else {
				resultVisitMarkerManager := NewVisitMarkerManager(objstore.WithNoopInstr(bkt), logger, ownerIdentifier, resultTestVisitMarker)
				err := resultVisitMarkerManager.ReadVisitMarker(context.Background(), resultTestVisitMarker)
				require.NoError(t, err)
				require.Equal(t, tcase.expectedStatus, resultTestVisitMarker.Status)
			}
		})
	}
}

type TestVisitMarker struct {
	OwnerIdentifier string      `json:"ownerIdentifier"`
	Status          VisitStatus `json:"status"`
	StoredValue     string      `json:"storedValue"`

	markerID ulid.ULID
}

func (t *TestVisitMarker) IsExpired(visitMarkerTimeout time.Duration) bool {
	return true
}

func (t *TestVisitMarker) GetStatus() VisitStatus {
	return t.Status
}

func NewTestVisitMarker(ownerIdentifier string) *TestVisitMarker {
	return &TestVisitMarker{
		OwnerIdentifier: ownerIdentifier,
		markerID:        ulid.MustNew(uint64(time.Now().UnixMilli()), rand.Reader),
		StoredValue:     "initial value",
	}
}

func CopyTestVisitMarker(sourceVisitMarker *TestVisitMarker) *TestVisitMarker {
	return &TestVisitMarker{
		OwnerIdentifier: sourceVisitMarker.OwnerIdentifier,
		markerID:        sourceVisitMarker.markerID,
		StoredValue:     sourceVisitMarker.StoredValue,
	}
}

func (t *TestVisitMarker) GetVisitMarkerFilePath() string {
	return fmt.Sprintf("test-visit-marker-%s.json", t.markerID.String())
}

func (t *TestVisitMarker) UpdateStatus(ownerIdentifier string, status VisitStatus) {
	t.OwnerIdentifier = ownerIdentifier
	t.Status = status
}

func (t *TestVisitMarker) String() string {
	return fmt.Sprintf("id=%s ownerIdentifier=%s status=%s storedValue=%s", t.markerID.String(), t.OwnerIdentifier, t.Status, t.StoredValue)
}
