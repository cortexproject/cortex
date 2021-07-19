package purger

import (
	"bytes"
	"context"
	math "math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func TestBlocksDeleteSeries_AddingDeletionRequests(t *testing.T) {
	for name, tc := range map[string]struct {
		parameters         url.Values
		expectedHTTPStatus int
	}{
		"empty": {
			parameters:         nil,
			expectedHTTPStatus: http.StatusBadRequest,
		},

		"valid request": {
			parameters: url.Values{
				"start":   []string{"1"},
				"end":     []string{"2"},
				"match[]": []string{"selector"},
			},
			expectedHTTPStatus: http.StatusNoContent,
		},

		"end time in the future": {
			parameters: url.Values{
				"start":   []string{"1"},
				"end":     []string{strconv.Itoa(math.MaxInt64)},
				"match[]": []string{"selector"},
			},
			expectedHTTPStatus: http.StatusBadRequest,
		},
		"the start time is after the end time": {
			parameters: url.Values{
				"start":   []string{"2"},
				"end":     []string{"1"},
				"match[]": []string{"selector"},
			},
			expectedHTTPStatus: http.StatusBadRequest,
		},
	} {
		t.Run(name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			api := newBlocksPurgerAPI(bkt, nil, log.NewNopLogger(), 0)

			ctx := context.Background()
			ctx = user.InjectOrgID(ctx, userID)

			u := &url.URL{
				RawQuery: tc.parameters.Encode(),
			}

			req := &http.Request{
				Method:     "GET",
				RequestURI: u.String(),
				URL:        u,
				Body:       http.NoBody,
				Header:     http.Header{},
			}

			resp := httptest.NewRecorder()
			api.AddDeleteRequestHandler(resp, req.WithContext(ctx))
			require.Equal(t, tc.expectedHTTPStatus, resp.Code)

		})
	}
}

func TestBlocksDeleteSeries_AddingSameRequestTwiceShouldFail(t *testing.T) {

	bkt := objstore.NewInMemBucket()
	api := newBlocksPurgerAPI(bkt, nil, log.NewNopLogger(), 0)

	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, userID)

	params := url.Values{
		"start":   []string{"1"},
		"end":     []string{"2"},
		"match[]": []string{"node_exporter"},
	}

	u := &url.URL{
		RawQuery: params.Encode(),
	}

	req := &http.Request{
		Method:     "GET",
		RequestURI: u.String(),
		URL:        u,
		Body:       http.NoBody,
		Header:     http.Header{},
	}

	resp := httptest.NewRecorder()
	api.AddDeleteRequestHandler(resp, req.WithContext(ctx))

	// First request made should be okay
	require.Equal(t, http.StatusNoContent, resp.Code)

	//second should not be accepted because the same exact request already exists
	resp = httptest.NewRecorder()
	api.AddDeleteRequestHandler(resp, req.WithContext(ctx))

	require.Equal(t, http.StatusBadRequest, resp.Code)

}

func TestBlocksDeleteSeries_AddingNewRequestShouldDeleteCancelledState(t *testing.T) {

	// If a tombstone has previously been cancelled, and a new request
	// being made results in the same request id, the cancelled tombstone
	// should be deleted from the bucket

	bkt := objstore.NewInMemBucket()
	api := newBlocksPurgerAPI(bkt, nil, log.NewNopLogger(), 0)

	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, userID)

	//first create a new tombstone
	paramsCreate := url.Values{
		"start":   []string{"1"},
		"end":     []string{"2"},
		"match[]": []string{"node_exporter"},
	}

	uCreate := &url.URL{
		RawQuery: paramsCreate.Encode(),
	}

	reqCreate := &http.Request{
		Method:     "GET",
		RequestURI: uCreate.String(),
		URL:        uCreate,
		Body:       http.NoBody,
		Header:     http.Header{},
	}

	resp := httptest.NewRecorder()
	api.AddDeleteRequestHandler(resp, reqCreate.WithContext(ctx))
	require.Equal(t, http.StatusNoContent, resp.Code)

	//cancel the previous request
	requestID := getTombstoneRequestID(1000, 2000, []string{"node_exporter"})
	paramsDelete := url.Values{
		"request_id": []string{requestID},
	}
	uCancel := &url.URL{
		RawQuery: paramsDelete.Encode(),
	}

	reqCancel := &http.Request{
		Method:     "POST",
		RequestURI: uCancel.String(),
		URL:        uCancel,
		Body:       http.NoBody,
		Header:     http.Header{},
	}

	resp = httptest.NewRecorder()
	api.CancelDeleteRequestHandler(resp, reqCancel.WithContext(ctx))
	require.Equal(t, http.StatusNoContent, resp.Code)

	// check that the cancelled file exists
	tCancelledPath := userID + "/tombstones/" + requestID + "." + string(cortex_tsdb.StateCancelled) + ".json"
	exists, _ := bkt.Exists(ctx, tCancelledPath)
	require.True(t, exists)

	// create a new request and make sure the cancelled file no longer exists
	resp = httptest.NewRecorder()
	api.AddDeleteRequestHandler(resp, reqCreate.WithContext(ctx))
	require.Equal(t, http.StatusNoContent, resp.Code)

	exists, _ = bkt.Exists(ctx, tCancelledPath)
	require.False(t, exists)

}

func TestBlocksDeleteSeries_CancellingRequestl(t *testing.T) {

	for name, tc := range map[string]struct {
		createdAt           int64
		requestState        cortex_tsdb.BlockDeleteRequestState
		cancellationPeriod  time.Duration
		cancelledFileExists bool
		expectedHTTPStatus  int
	}{
		"not allowed, grace period has passed": {
			createdAt:           0,
			requestState:        cortex_tsdb.StatePending,
			cancellationPeriod:  time.Second,
			cancelledFileExists: false,
			expectedHTTPStatus:  http.StatusBadRequest,
		},

		"allowed, grace period not over yet": {
			createdAt:           time.Now().Unix() * 1000,
			requestState:        cortex_tsdb.StatePending,
			cancellationPeriod:  time.Hour,
			cancelledFileExists: true,
			expectedHTTPStatus:  http.StatusNoContent,
		},
		"not allowed, deletion already occurred": {
			createdAt:           0,
			requestState:        cortex_tsdb.StateProcessed,
			cancellationPeriod:  time.Second,
			cancelledFileExists: false,
			expectedHTTPStatus:  http.StatusBadRequest,
		},
		"not allowed,request already cancelled": {
			createdAt:           0,
			requestState:        cortex_tsdb.StateCancelled,
			cancellationPeriod:  time.Second,
			cancelledFileExists: true,
			expectedHTTPStatus:  http.StatusBadRequest,
		},
	} {
		t.Run(name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			api := newBlocksPurgerAPI(bkt, nil, log.NewNopLogger(), 0)

			ctx := context.Background()
			ctx = user.InjectOrgID(ctx, userID)

			//create the tombstone
			tombstone := cortex_tsdb.NewTombstone(userID, tc.createdAt, tc.createdAt, 0, 1, []string{"match"}, "request_id", tc.requestState)
			err := cortex_tsdb.WriteTombstoneFile(ctx, api.bucketClient, userID, api.cfgProvider, tombstone)
			require.NoError(t, err)

			params := url.Values{
				"request_id": []string{"request_id"},
			}

			u := &url.URL{
				RawQuery: params.Encode(),
			}

			req := &http.Request{
				Method:     "POST",
				RequestURI: u.String(),
				URL:        u,
				Body:       http.NoBody,
				Header:     http.Header{},
			}

			resp := httptest.NewRecorder()
			api.CancelDeleteRequestHandler(resp, req.WithContext(ctx))
			require.Equal(t, tc.expectedHTTPStatus, resp.Code)

			// check if the cancelled tombstone file exists
			userBkt := bucket.NewUserBucketClient(userID, bkt, api.cfgProvider)
			exists, _ := cortex_tsdb.TombstoneExists(ctx, userBkt, userID, "request_id", cortex_tsdb.StateCancelled)
			require.Equal(t, tc.cancelledFileExists, exists)

		})
	}
}

func TestDeleteTenant(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	api := newBlocksPurgerAPI(bkt, nil, log.NewNopLogger(), 0)

	{
		resp := httptest.NewRecorder()
		api.DeleteTenant(resp, &http.Request{})
		require.Equal(t, http.StatusUnauthorized, resp.Code)
	}

	{
		ctx := context.Background()
		ctx = user.InjectOrgID(ctx, userID)

		req := &http.Request{}
		resp := httptest.NewRecorder()
		api.DeleteTenant(resp, req.WithContext(ctx))

		require.Equal(t, http.StatusOK, resp.Code)
		objs := bkt.Objects()
		require.NotNil(t, objs[path.Join(userID, tsdb.TenantDeletionMarkPath)])
	}
}

func TestDeleteTenantStatus(t *testing.T) {
	const username = "user"

	for name, tc := range map[string]struct {
		objects               map[string][]byte
		expectedBlocksDeleted bool
	}{
		"empty": {
			objects:               nil,
			expectedBlocksDeleted: true,
		},

		"no user objects": {
			objects: map[string][]byte{
				"different-user/01EQK4QKFHVSZYVJ908Y7HH9E0/meta.json": []byte("data"),
			},
			expectedBlocksDeleted: true,
		},

		"non-block files": {
			objects: map[string][]byte{
				"user/deletion-mark.json": []byte("data"),
			},
			expectedBlocksDeleted: true,
		},

		"block files": {
			objects: map[string][]byte{
				"user/01EQK4QKFHVSZYVJ908Y7HH9E0/meta.json": []byte("data"),
			},
			expectedBlocksDeleted: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			// "upload" objects
			for objName, data := range tc.objects {
				require.NoError(t, bkt.Upload(context.Background(), objName, bytes.NewReader(data)))
			}

			api := newBlocksPurgerAPI(bkt, nil, log.NewNopLogger(), 0)

			res, err := api.isBlocksForUserDeleted(context.Background(), username)
			require.NoError(t, err)
			require.Equal(t, tc.expectedBlocksDeleted, res)
		})
	}
}
