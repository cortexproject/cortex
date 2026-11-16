package cortex

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
)

func TestNewServer_MessageSizeHistograms(t *testing.T) {
	for name, tc := range map[string]struct {
		nativeHistogramFactor float64
		expectNative          bool
	}{
		"native buckets enabled":  {nativeHistogramFactor: 1.1, expectNative: true},
		"native buckets disabled": {nativeHistogramFactor: 0, expectNative: false},
	} {
		t.Run(name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()

			serv, err := newServer(server.Config{
				HTTPListenNetwork:            server.DefaultNetwork,
				GRPCListenNetwork:            server.DefaultNetwork,
				MetricsNamespace:             "cortex",
				MetricsNativeHistogramFactor: tc.nativeHistogramFactor,
				Registerer:                   reg,
			})
			require.NoError(t, err)

			serv.HTTP.Path("/test").Methods(http.MethodPost).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// The middleware counts only the request bytes that the handler reads.
				_, _ = io.Copy(io.Discard, r.Body)
				_, _ = w.Write([]byte("response body"))
			})

			t.Cleanup(serv.Shutdown)

			// Send the request to the instrumented server handler directly. The test
			// then does not depend on the network.
			req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewBufferString("request body"))
			rec := httptest.NewRecorder()
			serv.HTTPServer.Handler.ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			families, err := reg.Gather()
			require.NoError(t, err)

			for _, metricName := range []string{"cortex_request_message_bytes", "cortex_response_message_bytes"} {
				h := findHistogram(t, families, metricName)

				// The classic buckets must stay, so that existing queries keep working.
				assert.Len(t, h.GetBucket(), len(middleware.BodySizeBuckets), metricName)
				assert.Equal(t, uint64(1), h.GetSampleCount(), metricName)

				if tc.expectNative {
					assert.NotNil(t, h.Schema, "%s must expose native buckets", metricName)
					assert.NotEmpty(t, h.GetPositiveSpan(), "%s must have a native bucket for the observed size", metricName)
				} else {
					assert.Nil(t, h.Schema, "%s must not expose native buckets", metricName)
					assert.Empty(t, h.GetPositiveSpan(), metricName)
				}
			}

			// The other server metrics must still be registered.
			findHistogram(t, families, "cortex_request_duration_seconds")
		})
	}
}

func findHistogram(t *testing.T, families []*dto.MetricFamily, name string) *dto.Histogram {
	t.Helper()

	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		require.Len(t, family.GetMetric(), 1, name)
		return family.GetMetric()[0].GetHistogram()
	}

	require.FailNow(t, "metric not found", name)
	return nil
}
