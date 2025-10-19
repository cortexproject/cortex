package querier

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/prometheus/prometheus/scrape"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestMetadataHandler_Success(t *testing.T) {
	t.Parallel()

	d := &MockDistributor{}
	d.On("MetricsMetadata", mock.Anything, mock.Anything).Return(
		[]scrape.MetricMetadata{
			{MetricFamily: "alertmanager_dispatcher_aggregation_groups", Help: "Number of active aggregation groups", Type: "gauge", Unit: ""},
			{MetricFamily: "go_threads", Help: "Number of OS threads created", Type: "gauge", Unit: ""},
			{MetricFamily: "go_threads", Help: "Number of OS threads that were created", Type: "gauge", Unit: ""},
		},
		nil)

	fullResponseJson := `
		{
			"status": "success",
			"data": {
				"alertmanager_dispatcher_aggregation_groups": [
					{
						"help": "Number of active aggregation groups",
						"type": "gauge",
						"unit": ""
					}
				],
				"go_threads": [
					{
						"help": "Number of OS threads created",
						"type": "gauge",
						"unit": ""
					},
					{
						"help": "Number of OS threads that were created",
						"type": "gauge",
						"unit": ""
					}
				]
			}
		}
	`

	emptyDataResponseJson := `
		{
			"status": "success",
			"data": {}
		}
	`

	tests := []struct {
		description  string
		queryParams  url.Values
		expectedCode int
		expectedJson string
	}{
		{
			description:  "no params",
			queryParams:  url.Values{},
			expectedCode: http.StatusOK,
			expectedJson: fullResponseJson,
		},
		{
			description: "limit: -1",
			queryParams: url.Values{
				"limit": []string{"-1"},
			},
			expectedCode: http.StatusOK,
			expectedJson: fullResponseJson,
		},
		{
			description: "limit: 0",
			queryParams: url.Values{
				"limit": []string{"0"},
			},
			expectedCode: http.StatusOK,
			expectedJson: emptyDataResponseJson,
		},
		{
			description: "limit: 1",
			queryParams: url.Values{
				"limit": []string{"1"},
			},
			expectedCode: http.StatusOK,
			expectedJson: `
				{
					"status": "success",
					"data": {
						"alertmanager_dispatcher_aggregation_groups": [
							{
								"help": "Number of active aggregation groups",
								"type": "gauge",
								"unit": ""
							}
						]
					}
				}
			`,
		},
		{
			description: "limit: invalid",
			queryParams: url.Values{
				"limit": []string{"aaa"},
			},
			expectedCode: http.StatusBadRequest,
			expectedJson: `
				{
					"status": "error",
					"error": "limit must be a number"
				}
			`,
		},
		{
			description: "limit_per_metric: -1",
			queryParams: url.Values{
				"limit_per_metric": []string{"-1"},
			},
			expectedCode: http.StatusOK,
			expectedJson: fullResponseJson,
		},
		{
			description: "limit_per_metric: 0, should be ignored",
			queryParams: url.Values{
				"limit_per_metric": []string{"0"},
			},
			expectedCode: http.StatusOK,
			expectedJson: fullResponseJson,
		},
		{
			description: "limit_per_metric: 1",
			queryParams: url.Values{
				"limit_per_metric": []string{"1"},
			},
			expectedCode: http.StatusOK,
			expectedJson: `
				{
					"status": "success",
					"data": {
						"alertmanager_dispatcher_aggregation_groups": [
							{
								"help": "Number of active aggregation groups",
								"type": "gauge",
								"unit": ""
							}
						],
						"go_threads": [
							{
								"help": "Number of OS threads created",
								"type": "gauge",
								"unit": ""
							}
						]
					}
				}
			`,
		},
		{
			description: "limit_per_metric: invalid",
			queryParams: url.Values{
				"limit_per_metric": []string{"aaa"},
			},
			expectedCode: http.StatusBadRequest,
			expectedJson: `
				{
					"status": "error",
					"error": "limit_per_metric must be a number"
				}
			`,
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			handler := MetadataHandler(d)

			request, err := http.NewRequest("GET", "/metadata", nil)
			request.URL.RawQuery = test.queryParams.Encode()
			require.NoError(t, err)

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			require.Equal(t, test.expectedCode, recorder.Result().StatusCode)
			responseBody, err := io.ReadAll(recorder.Result().Body)
			require.NoError(t, err)
			require.JSONEq(t, test.expectedJson, string(responseBody))
		})
	}
}

func TestMetadataHandler_Error(t *testing.T) {
	t.Parallel()

	d := &MockDistributor{}
	d.On("MetricsMetadata", mock.Anything, mock.Anything).Return([]scrape.MetricMetadata{}, fmt.Errorf("no user id"))

	handler := MetadataHandler(d)

	request, err := http.NewRequest("GET", "/metadata", nil)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Result().StatusCode)
	responseBody, err := io.ReadAll(recorder.Result().Body)
	require.NoError(t, err)

	expectedJSON := `
	{
		"status": "error",
		"error": "no user id"
	}
	`

	require.JSONEq(t, expectedJSON, string(responseBody))
}
