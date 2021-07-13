package purger

import (
	"context"
	math "math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/weaveworks/common/user"
)

func TestBlocksDeleteSeries_AddingDeletionRequests(t *testing.T) {
	for name, tc := range map[string]struct {
		parameters         url.Values
		expectedHttpStatus int
	}{
		"empty": {
			parameters:         nil,
			expectedHttpStatus: http.StatusBadRequest,
		},

		"valid request": {
			parameters: url.Values{
				"start":   []string{"1"},
				"end":     []string{"2"},
				"match[]": []string{"selector"},
			},
			expectedHttpStatus: http.StatusNoContent,
		},

		"end time in the future": {
			parameters: url.Values{
				"start":   []string{"1"},
				"end":     []string{strconv.Itoa(math.MaxInt64)},
				"match[]": []string{"selector"},
			},
			expectedHttpStatus: http.StatusBadRequest,
		},
		"the start time is after the end time": {
			parameters: url.Values{
				"start":   []string{"2"},
				"end":     []string{"1"},
				"match[]": []string{"selector"},
			},
			expectedHttpStatus: http.StatusBadRequest,
		},
	} {
		t.Run(name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			api := newBlocksPurgerAPI(bkt, nil, log.NewNopLogger(), 0)

			ctx := context.Background()
			ctx = user.InjectOrgID(ctx, "fake")

			u := &url.URL{
				RawQuery: tc.parameters.Encode(),
			}

			req := &http.Request{
				Method:     "GET",
				RequestURI: u.String(), // This is what the httpgrpc code looks at.
				URL:        u,
				Body:       http.NoBody,
				Header:     http.Header{},
			}

			resp := httptest.NewRecorder()
			api.V2AddDeleteRequestHandler(resp, req.WithContext(ctx))
			require.Equal(t, tc.expectedHttpStatus, resp.Code)

		})
	}
}

func TestBlocksDeleteSeries_AddingSameRequestTwiceShouldFail(t *testing.T) {

	bkt := objstore.NewInMemBucket()
	api := newBlocksPurgerAPI(bkt, nil, log.NewNopLogger(), 0)

	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, "fake")

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
	api.V2AddDeleteRequestHandler(resp, req.WithContext(ctx))

	// First request made should be okay
	require.Equal(t, http.StatusNoContent, resp.Code)

	//second should not be accepted because the same exact request already exists
	resp = httptest.NewRecorder()
	api.V2AddDeleteRequestHandler(resp, req.WithContext(ctx))

	require.Equal(t, http.StatusBadRequest, resp.Code)

}

func TestBlocksDeleteSeries_CancellingRequestl(t *testing.T) {

	for name, tc := range map[string]struct {
		createdAt           int64
		requestState        cortex_tsdb.BlockDeleteRequestState
		cancellationPeriod  time.Duration
		cancelledFileExists bool
		expectedHttpStatus  int
	}{
		"not allowed, grace period has passed": {
			createdAt:           0,
			requestState:        cortex_tsdb.StatePending,
			cancellationPeriod:  time.Second,
			cancelledFileExists: false,
			expectedHttpStatus:  http.StatusBadRequest,
		},

		"allowed, grace period not over yet": {
			createdAt:           time.Now().Unix() * 1000,
			requestState:        cortex_tsdb.StatePending,
			cancellationPeriod:  time.Hour,
			cancelledFileExists: true,
			expectedHttpStatus:  http.StatusNoContent,
		},
		"not allowed, deletion already occurred": {
			createdAt:           0,
			requestState:        cortex_tsdb.StateProcessed,
			cancellationPeriod:  time.Second,
			cancelledFileExists: false,
			expectedHttpStatus:  http.StatusBadRequest,
		},
		"not allowed,request already cancelled": {
			createdAt:           0,
			requestState:        cortex_tsdb.StateCancelled,
			cancellationPeriod:  time.Second,
			cancelledFileExists: true,
			expectedHttpStatus:  http.StatusBadRequest,
		},
	} {
		t.Run(name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			api := newBlocksPurgerAPI(bkt, nil, log.NewNopLogger(), 0)

			ctx := context.Background()
			ctx = user.InjectOrgID(ctx, "fake")

			//create the tombstone
			tombstone := cortex_tsdb.NewTombstone("fake", tc.createdAt, tc.createdAt, 0, 1, []string{"match"}, "request_id", tc.requestState)
			err := cortex_tsdb.WriteTombstoneFile(ctx, api.bucketClient, "fake", api.cfgProvider, tombstone)
			require.NoError(t, err)

			params := url.Values{
				"request_id": []string{"request_id"},
			}

			u := &url.URL{
				RawQuery: params.Encode(),
			}

			req := &http.Request{
				Method:     "POST",
				RequestURI: u.String(), // This is what the httpgrpc code looks at.
				URL:        u,
				Body:       http.NoBody,
				Header:     http.Header{},
			}

			resp := httptest.NewRecorder()
			api.V2CancelDeleteRequestHandler(resp, req.WithContext(ctx))
			require.Equal(t, tc.expectedHttpStatus, resp.Code)

			// check if the cancelled tombstone file exists
			userBkt := bucket.NewUserBucketClient("fake", bkt, api.cfgProvider)
			exists, _ := cortex_tsdb.TombstoneExists(ctx, userBkt, "fake", "request_id", cortex_tsdb.StateCancelled)
			require.Equal(t, tc.cancelledFileExists, exists)

		})
	}
}
