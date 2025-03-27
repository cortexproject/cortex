package tripperware

import (
	"bytes"
	"compress/gzip"
	"math"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/util"
)

// Same as https://github.com/prometheus/client_golang/blob/v1.19.1/api/prometheus/v1/api_test.go#L1577.
func TestSampleHistogramPairJSONSerialization(t *testing.T) {
	tests := []struct {
		name     string
		point    SampleHistogramPair
		expected string
	}{
		{
			name: "empty histogram",
			point: SampleHistogramPair{
				TimestampMs: 0,
				Histogram:   SampleHistogram{},
			},
			expected: `[0,{"count":"0","sum":"0"}]`,
		},
		{
			name: "histogram with NaN/Inf and no buckets",
			point: SampleHistogramPair{
				TimestampMs: 0,
				Histogram: SampleHistogram{
					Count: math.NaN(),
					Sum:   math.Inf(1),
				},
			},
			expected: `[0,{"count":"NaN","sum":"+Inf"}]`,
		},
		{
			name: "six-bucket histogram",
			point: SampleHistogramPair{
				TimestampMs: 1,
				Histogram:   testHistogram1,
			},
			expected: `[0.001,{"count":"13.5","sum":"3897.1","buckets":[[1,"-4870.992343051145","-4466.7196729968955","1"],[1,"-861.0779292198035","-789.6119426088657","2"],[1,"-558.3399591246119","-512","3"],[0,"2048","2233.3598364984477","1.5"],[0,"2896.3093757400984","3158.4477704354626","2.5"],[0,"4466.7196729968955","4870.992343051145","3.5"]]}]`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b, err := json.Marshal(test.point)
			require.NoError(t, err)
			require.Equal(t, test.expected, string(b))

			// To test Unmarshal we will Unmarshal then re-Marshal. This way we
			// can do a string compare, otherwise NaN values don't show equivalence
			// properly.
			var sp SampleHistogramPair
			err = json.Unmarshal(b, &sp)
			require.NoError(t, err)

			b, err = json.Marshal(sp)
			require.NoError(t, err)
			require.Equal(t, test.expected, string(b))
		})
	}
}

// Same as https://github.com/prometheus/client_golang/blob/v1.19.1/api/prometheus/v1/api_test.go#L1682.
func TestSampleStreamJSONSerialization(t *testing.T) {
	floats, histograms := generateData(1, 5)

	tests := []struct {
		name         string
		stream       SampleStream
		expectedJSON string
	}{
		{
			"floats",
			*floats[0],
			`{"metric":{"__name__":"timeseries_0","foo":"bar"},"values":[[1677587259.055,"1"],[1677587244.055,"2"],[1677587229.055,"3"],[1677587214.055,"4"],[1677587199.055,"5"]]}`,
		},
		{
			"histograms",
			*histograms[0],
			`{"metric":{"__name__":"timeseries_0","foo":"bar"},"histograms":[[1677587259.055,{"count":"13.5","sum":"0.1","buckets":[[1,"-4870.992343051145","-4466.7196729968955","1"],[1,"-861.0779292198035","-789.6119426088657","2"],[1,"-558.3399591246119","-512","3"],[0,"2048","2233.3598364984477","1.5"],[0,"2896.3093757400984","3158.4477704354626","2.5"],[0,"4466.7196729968955","4870.992343051145","3.5"]]}],[1677587244.055,{"count":"27","sum":"0.2","buckets":[[1,"-4870.992343051145","-4466.7196729968955","2"],[1,"-861.0779292198035","-789.6119426088657","4"],[1,"-558.3399591246119","-512","6"],[0,"2048","2233.3598364984477","3"],[0,"2896.3093757400984","3158.4477704354626","5"],[0,"4466.7196729968955","4870.992343051145","7"]]}],[1677587229.055,{"count":"40.5","sum":"0.30000000000000004","buckets":[[1,"-4870.992343051145","-4466.7196729968955","3"],[1,"-861.0779292198035","-789.6119426088657","6"],[1,"-558.3399591246119","-512","9"],[0,"2048","2233.3598364984477","4.5"],[0,"2896.3093757400984","3158.4477704354626","7.5"],[0,"4466.7196729968955","4870.992343051145","10.5"]]}],[1677587214.055,{"count":"54","sum":"0.4","buckets":[[1,"-4870.992343051145","-4466.7196729968955","4"],[1,"-861.0779292198035","-789.6119426088657","8"],[1,"-558.3399591246119","-512","12"],[0,"2048","2233.3598364984477","6"],[0,"2896.3093757400984","3158.4477704354626","10"],[0,"4466.7196729968955","4870.992343051145","14"]]}],[1677587199.055,{"count":"67.5","sum":"0.5","buckets":[[1,"-4870.992343051145","-4466.7196729968955","5"],[1,"-861.0779292198035","-789.6119426088657","10"],[1,"-558.3399591246119","-512","15"],[0,"2048","2233.3598364984477","7.5"],[0,"2896.3093757400984","3158.4477704354626","12.5"],[0,"4466.7196729968955","4870.992343051145","17.5"]]}]]}`,
		},
		{
			"both",
			SampleStream{
				Labels:     floats[0].Labels,
				Samples:    floats[0].Samples,
				Histograms: histograms[0].Histograms,
			},
			`{"metric":{"__name__":"timeseries_0","foo":"bar"},"values":[[1677587259.055,"1"],[1677587244.055,"2"],[1677587229.055,"3"],[1677587214.055,"4"],[1677587199.055,"5"]],"histograms":[[1677587259.055,{"count":"13.5","sum":"0.1","buckets":[[1,"-4870.992343051145","-4466.7196729968955","1"],[1,"-861.0779292198035","-789.6119426088657","2"],[1,"-558.3399591246119","-512","3"],[0,"2048","2233.3598364984477","1.5"],[0,"2896.3093757400984","3158.4477704354626","2.5"],[0,"4466.7196729968955","4870.992343051145","3.5"]]}],[1677587244.055,{"count":"27","sum":"0.2","buckets":[[1,"-4870.992343051145","-4466.7196729968955","2"],[1,"-861.0779292198035","-789.6119426088657","4"],[1,"-558.3399591246119","-512","6"],[0,"2048","2233.3598364984477","3"],[0,"2896.3093757400984","3158.4477704354626","5"],[0,"4466.7196729968955","4870.992343051145","7"]]}],[1677587229.055,{"count":"40.5","sum":"0.30000000000000004","buckets":[[1,"-4870.992343051145","-4466.7196729968955","3"],[1,"-861.0779292198035","-789.6119426088657","6"],[1,"-558.3399591246119","-512","9"],[0,"2048","2233.3598364984477","4.5"],[0,"2896.3093757400984","3158.4477704354626","7.5"],[0,"4466.7196729968955","4870.992343051145","10.5"]]}],[1677587214.055,{"count":"54","sum":"0.4","buckets":[[1,"-4870.992343051145","-4466.7196729968955","4"],[1,"-861.0779292198035","-789.6119426088657","8"],[1,"-558.3399591246119","-512","12"],[0,"2048","2233.3598364984477","6"],[0,"2896.3093757400984","3158.4477704354626","10"],[0,"4466.7196729968955","4870.992343051145","14"]]}],[1677587199.055,{"count":"67.5","sum":"0.5","buckets":[[1,"-4870.992343051145","-4466.7196729968955","5"],[1,"-861.0779292198035","-789.6119426088657","10"],[1,"-558.3399591246119","-512","15"],[0,"2048","2233.3598364984477","7.5"],[0,"2896.3093757400984","3158.4477704354626","12.5"],[0,"4466.7196729968955","4870.992343051145","17.5"]]}]]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b, err := json.Marshal(test.stream)
			require.NoError(t, err)
			require.Equal(t, test.expectedJSON, string(b))

			var stream SampleStream
			err = json.Unmarshal(b, &stream)
			require.NoError(t, err)
			require.Equal(t, test.stream, stream)
		})
	}
}

func generateData(timeseries, datapoints int) (floatMatrix, histogramMatrix []*SampleStream) {
	for i := 0; i < timeseries; i++ {
		lset := labels.FromMap(map[string]string{
			model.MetricNameLabel: "timeseries_" + strconv.Itoa(i),
			"foo":                 "bar",
		})
		now := model.Time(1677587274055).Time()
		floats := make([]cortexpb.Sample, datapoints)
		histograms := make([]SampleHistogramPair, datapoints)

		for x := datapoints; x > 0; x-- {
			f := float64(x)
			floats[x-1] = cortexpb.Sample{
				// Set the time back assuming a 15s interval. Since this is used for
				// Marshal/Unmarshal testing the actual interval doesn't matter.
				TimestampMs: util.TimeToMillis(now.Add(time.Second * -15 * time.Duration(x))),
				Value:       f,
			}
			histograms[x-1] = SampleHistogramPair{
				TimestampMs: util.TimeToMillis(now.Add(time.Second * -15 * time.Duration(x))),
				Histogram: SampleHistogram{
					Count: 13.5 * f,
					Sum:   .1 * f,
					Buckets: []*HistogramBucket{
						{
							Boundaries: 1,
							Lower:      -4870.992343051145,
							Upper:      -4466.7196729968955,
							Count:      1 * f,
						},
						{
							Boundaries: 1,
							Lower:      -861.0779292198035,
							Upper:      -789.6119426088657,
							Count:      2 * f,
						},
						{
							Boundaries: 1,
							Lower:      -558.3399591246119,
							Upper:      -512,
							Count:      3 * f,
						},
						{
							Boundaries: 0,
							Lower:      2048,
							Upper:      2233.3598364984477,
							Count:      1.5 * f,
						},
						{
							Boundaries: 0,
							Lower:      2896.3093757400984,
							Upper:      3158.4477704354626,
							Count:      2.5 * f,
						},
						{
							Boundaries: 0,
							Lower:      4466.7196729968955,
							Upper:      4870.992343051145,
							Count:      3.5 * f,
						},
					},
				},
			}
		}

		fss := &SampleStream{
			Labels:  cortexpb.FromLabelsToLabelAdapters(lset),
			Samples: floats,
		}
		hss := &SampleStream{
			Labels:     cortexpb.FromLabelsToLabelAdapters(lset),
			Histograms: histograms,
		}

		floatMatrix = append(floatMatrix, fss)
		histogramMatrix = append(histogramMatrix, hss)
	}
	return
}

func Test_getResponseSize(t *testing.T) {
	tests := []struct {
		body    []byte
		useGzip bool
	}{
		{
			body:    []byte(`foo`),
			useGzip: false,
		},
		{
			body:    []byte(`foo`),
			useGzip: true,
		},
		{
			body:    []byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`),
			useGzip: false,
		},
		{
			body:    []byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`),
			useGzip: true,
		},
	}

	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			expectedBodyLength := len(test.body)
			buf := &bytes.Buffer{}
			response := &http.Response{}

			if test.useGzip {
				response = &http.Response{
					Header: http.Header{"Content-Encoding": []string{"gzip"}},
				}
				w := gzip.NewWriter(buf)
				_, err := w.Write(test.body)
				require.NoError(t, err)
				w.Close()
			} else {
				buf = bytes.NewBuffer(test.body)
			}

			bodyLength := getResponseSize(response, buf)
			require.Equal(t, expectedBodyLength, bodyLength)
		})
	}
}
