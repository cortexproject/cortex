//go:build requires_docker
// +build requires_docker

package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestFormatQueryAPI(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-auth.enabled": "true",
	})

	// Start the query-frontend.
	queryFrontend := e2ecortex.NewQueryFrontend("query-frontend", flags, "")
	require.NoError(t, s.Start(queryFrontend))

	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-querier.frontend-address": queryFrontend.NetworkGRPCEndpoint(),
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier))

	// Start querier without frontend.
	querierDirect := e2ecortex.NewQuerier("querier-direct", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(querierDirect))

	require.NoError(t, s.WaitReady(queryFrontend))

	testCases := []struct {
		name           string
		query          string
		expectedResp   string
		expectError    bool
		method         string
		useOnlyQuerier bool
	}{
		{
			name:         "Valid query for GET method",
			query:        "foo/bar",
			expectedResp: "foo / bar",
			expectError:  false,
			method:       "GET",
		},
		{
			name:         "Valid query for POST method",
			query:        "foo/bar",
			expectedResp: "foo / bar",
			expectError:  false,
			method:       "POST",
		},
		{
			name:        "Invalid query for GET method",
			query:       "invalid_expression/",
			expectError: true,
			method:      "GET",
		},
		{
			name:        "Invalid query for POST method",
			query:       "invalid_expression/",
			expectError: true,
			method:      "POST",
		},
		{
			name:           "Valid query using only querier (GET)",
			query:          "foo/bar",
			expectedResp:   "foo / bar",
			expectError:    false,
			method:         "GET",
			useOnlyQuerier: true,
		},
	}
	var parsed struct {
		Data string `json:"data"`
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var endpoint string
			if tc.useOnlyQuerier {
				endpoint = fmt.Sprintf("http://%s/api/prom/api/v1/format_query?query=%s", querierDirect.HTTPEndpoint(), tc.query)
			} else {
				endpoint = fmt.Sprintf("http://%s/api/prom/api/v1/format_query?query=%s", queryFrontend.HTTPEndpoint(), tc.query)
			}

			req, err := http.NewRequest(tc.method, endpoint, nil)
			req.Header.Set("X-Scope-OrgID", "user-1")
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			if tc.expectError {
				require.NotEqual(t, 200, resp.StatusCode)
				return
			} else {
				require.Equal(t, 200, resp.StatusCode)
			}

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			require.NoError(t, json.Unmarshal(body, &parsed))
			require.Equal(t, tc.expectedResp, parsed.Data)
		})
	}
}
