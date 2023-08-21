package instantquery

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/weaveworks/common/httpgrpc"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

func TestRequest(t *testing.T) {
	t.Parallel()
	codec := InstantQueryCodec

	for _, tc := range []struct {
		url         string
		expectedURL string
		expected    *PrometheusRequest
		expectedErr error
	}{
		{
			url:         "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&stats=all&time=1536673680",
			expectedURL: "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&stats=all&time=1536673680",
			expected: &PrometheusRequest{
				Path:  "/api/v1/query",
				Time:  1536673680 * 1e3,
				Query: "sum(container_memory_rss) by (namespace)",
				Stats: "all",
				Headers: map[string][]string{
					"Test-Header": {"test"},
				},
			},
		},
		{
			url:         "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&time=1536673680",
			expectedURL: "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&time=1536673680",
			expected: &PrometheusRequest{
				Path:  "/api/v1/query",
				Time:  1536673680 * 1e3,
				Query: "sum(container_memory_rss) by (namespace)",
				Stats: "",
				Headers: map[string][]string{
					"Test-Header": {"test"},
				},
			},
		},
		{
			url:         "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29",
			expectedURL: "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&time=",
			expected: &PrometheusRequest{
				Path:  "/api/v1/query",
				Time:  0,
				Query: "sum(container_memory_rss) by (namespace)",
				Stats: "",
				Headers: map[string][]string{
					"Test-Header": {"test"},
				},
			},
		},
	} {
		tc := tc
		t.Run(tc.url, func(t *testing.T) {
			t.Parallel()
			r, err := http.NewRequest("GET", tc.url, nil)
			require.NoError(t, err)
			r.Header.Add("Test-Header", "test")

			ctx := user.InjectOrgID(context.Background(), "1")

			// Get a deep copy of the request with Context changed to ctx
			r = r.Clone(ctx)

			if tc.expected.Time == 0 {
				now := time.Now()
				tc.expectedURL = fmt.Sprintf("%s%d", tc.expectedURL, now.Unix())
				tc.expected.Time = now.Unix() * 1e3
			}
			req, err := codec.DecodeRequest(ctx, r, []string{"Test-Header"})
			if err != nil {
				require.EqualValues(t, tc.expectedErr, err)
				return
			}
			require.EqualValues(t, tc.expected, req)

			rdash, err := codec.EncodeRequest(context.Background(), req)
			require.NoError(t, err)
			require.EqualValues(t, tc.expectedURL, rdash.RequestURI)
		})
	}
}

func TestCompressedResponse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		compression string
		jsonBody    string
		promBody    *PrometheusInstantQueryResponse
		status      int
		err         error
	}{
		{
			name:        "successful response",
			compression: "gzip",
			promBody: &PrometheusInstantQueryResponse{
				Status: "success",
				Data: PrometheusInstantQueryData{
					ResultType: model.ValString.String(),
					Result:     PrometheusInstantQueryResult{Result: &PrometheusInstantQueryResult_RawBytes{[]byte(`{"resultType":"string","result":[1,"foo"]}`)}},
				},
				Headers: []*tripperware.PrometheusResponseHeader{
					{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					{Name: "Content-Encoding", Values: []string{"gzip"}},
				},
			},
			status: 200,
		},
		{
			name:        "successful response",
			compression: "snappy",
			promBody: &PrometheusInstantQueryResponse{
				Status: "success",
				Data: PrometheusInstantQueryData{
					ResultType: model.ValString.String(),
					Result:     PrometheusInstantQueryResult{Result: &PrometheusInstantQueryResult_RawBytes{[]byte(`{"resultType":"string","result":[1,"foo"]}`)}},
				},
				Headers: []*tripperware.PrometheusResponseHeader{
					{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					{Name: "Content-Encoding", Values: []string{"snappy"}},
				},
			},
			status: 200,
		},
		{
			name:        `400 error`,
			compression: `gzip`,
			jsonBody:    `error generic 400`,
			status:      400,
			err:         httpgrpc.Errorf(400, `error generic 400`),
		},
		{
			name:        `400 error`,
			compression: `snappy`,
			jsonBody:    `error generic 400`,
			status:      400,
			err:         httpgrpc.Errorf(400, `error generic 400`),
		},
		{
			name:        `400 error empty body`,
			compression: `gzip`,
			status:      400,
			err:         httpgrpc.Errorf(400, ""),
		},
		{
			name:        `400 error empty body`,
			compression: `snappy`,
			status:      400,
			err:         httpgrpc.Errorf(400, ""),
		},
	} {
		for _, c := range []bool{true, false} {
			c := c
			t.Run(fmt.Sprintf("%s compressed %t [%s]", tc.compression, c, tc.name), func(t *testing.T) {
				t.Parallel()
				h := http.Header{}
				var b []byte
				if tc.promBody != nil {
					protobuf, err := proto.Marshal(tc.promBody)
					b = protobuf
					require.NoError(t, err)
					h.Set("Content-Type", "application/x-protobuf")
				} else {
					b = []byte(tc.jsonBody)
					h.Set("Content-Type", "application/json")
				}
				responseBody := bytes.NewBuffer(b)

				var buf bytes.Buffer
				if c && tc.compression == "gzip" {
					h.Set("Content-Encoding", "gzip")
					w := gzip.NewWriter(&buf)
					_, err := w.Write(b)
					require.NoError(t, err)
					w.Close()
					responseBody = &buf
				} else if c && tc.compression == "snappy" {
					h.Set("Content-Encoding", "snappy")
					w := snappy.NewBufferedWriter(&buf)
					_, err := w.Write(b)
					require.NoError(t, err)
					w.Close()
					responseBody = &buf
				}

				response := &http.Response{
					StatusCode: tc.status,
					Header:     h,
					Body:       io.NopCloser(responseBody),
				}
				resp, err := InstantQueryCodec.DecodeResponse(context.Background(), response, nil)
				require.Equal(t, tc.err, err)

				if err == nil {
					require.NoError(t, err)
					require.Equal(t, tc.promBody, resp)
				}
			})
		}
	}
}

func TestResponse(t *testing.T) {
	t.Parallel()
	for i, tc := range []struct {
		expectedResp string
		promBody     *PrometheusInstantQueryResponse
	}{
		{
			expectedResp: `{"status":"success","data":{"resultType":"string","result":[1,"foo"]}}`,
			promBody: &PrometheusInstantQueryResponse{
				Status: "success",
				Data: PrometheusInstantQueryData{
					ResultType: model.ValString.String(),
					Result:     PrometheusInstantQueryResult{Result: &PrometheusInstantQueryResult_RawBytes{[]byte(`{"resultType":"string","result":[1,"foo"]}`)}},
				},
				Headers: []*tripperware.PrometheusResponseHeader{
					{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
				},
			},
		},
		{
			expectedResp: `{"status":"success","data":{"resultType":"string","result":[1,"foo"],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1536673680,5],[1536673780,5]]}}}}`,
			promBody: &PrometheusInstantQueryResponse{
				Status: "success",
				Data: PrometheusInstantQueryData{
					ResultType: model.ValString.String(),
					Result:     PrometheusInstantQueryResult{Result: &PrometheusInstantQueryResult_RawBytes{[]byte(`{"resultType":"string","result":[1,"foo"],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1536673680,5],[1536673780,5]]}}}`)}},
				},
				Headers: []*tripperware.PrometheusResponseHeader{
					{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
				},
			},
		},
		{
			expectedResp: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1,"137"],[2,"137"]]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1536673680,5],[1536673780,5]]}}}}`,
			promBody: &PrometheusInstantQueryResponse{
				Status: "success",
				Data: PrometheusInstantQueryData{
					ResultType: model.ValMatrix.String(),
					Result: PrometheusInstantQueryResult{
						Result: &PrometheusInstantQueryResult_Matrix{
							Matrix: &Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{
											{"foo", "bar"},
										},
										Samples: []cortexpb.Sample{
											{Value: 137, TimestampMs: 1000},
											{Value: 137, TimestampMs: 2000},
										},
									},
								},
							},
						},
					},
					Stats: &tripperware.PrometheusResponseStats{
						Samples: &tripperware.PrometheusResponseSamplesStats{
							TotalQueryableSamplesPerStep: []*tripperware.PrometheusResponseQueryableSamplesStatsPerStep{
								{Value: 5, TimestampMs: 1536673680000},
								{Value: 5, TimestampMs: 1536673780000},
							},
							TotalQueryableSamples: 10,
						},
					},
				},
				Headers: []*tripperware.PrometheusResponseHeader{
					{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
				},
			},
		},
		{
			expectedResp: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1,"137"],[2,"137"]]}]}}`,
			promBody: &PrometheusInstantQueryResponse{
				Status: "success",
				Data: PrometheusInstantQueryData{
					ResultType: model.ValMatrix.String(),
					Result: PrometheusInstantQueryResult{
						Result: &PrometheusInstantQueryResult_Matrix{
							Matrix: &Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{
											{"foo", "bar"},
										},
										Samples: []cortexpb.Sample{
											{Value: 137, TimestampMs: 1000},
											{Value: 137, TimestampMs: 2000},
										},
									},
								},
							},
						},
					},
				},
				Headers: []*tripperware.PrometheusResponseHeader{
					{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
				},
			},
		},
		{
			expectedResp: `{"status":"success","data":{"resultType":"scalar","result":[1,"13"]}}`,
			promBody: &PrometheusInstantQueryResponse{
				Status: "success",
				Data: PrometheusInstantQueryData{
					ResultType: model.ValString.String(),
					Result:     PrometheusInstantQueryResult{Result: &PrometheusInstantQueryResult_RawBytes{[]byte(`{"resultType":"scalar","result":[1,"13"]}`)}},
				},
				Headers: []*tripperware.PrometheusResponseHeader{
					{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
				},
			},
		},
		{
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"1266464.0146205237"]}]}}`,
			promBody: &PrometheusInstantQueryResponse{
				Status: "success",
				Data: PrometheusInstantQueryData{
					ResultType: model.ValVector.String(),
					Result: PrometheusInstantQueryResult{
						Result: &PrometheusInstantQueryResult_Vector{
							Vector: &Vector{
								Samples: []*Sample{
									{
										Labels: []cortexpb.LabelAdapter{},
										Sample: cortexpb.Sample{Value: 1266464.0146205237, TimestampMs: 1000},
									},
								},
							},
						},
					},
				},
				Headers: []*tripperware.PrometheusResponseHeader{
					{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
				},
			},
		},
	} {
		tc := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			protobuf, err := proto.Marshal(tc.promBody)
			require.NoError(t, err)

			response := &http.Response{
				StatusCode: 200,
				Header:     http.Header{"Content-Type": []string{"application/x-protobuf"}},
				Body:       io.NopCloser(bytes.NewBuffer(protobuf)),
			}
			resp, err := InstantQueryCodec.DecodeResponse(context.Background(), response, nil)
			require.NoError(t, err)

			// Reset response, as the above call will have consumed the body reader.
			response = &http.Response{
				StatusCode:    200,
				Header:        http.Header{"Content-Type": []string{"application/json"}},
				Body:          io.NopCloser(bytes.NewBuffer([]byte(tc.expectedResp))),
				ContentLength: int64(len(tc.expectedResp)),
			}
			resp2, err := InstantQueryCodec.EncodeResponse(context.Background(), resp)
			require.NoError(t, err)
			assert.Equal(t, response, resp2)
		})
	}
}

func TestMergeResponse(t *testing.T) {
	t.Parallel()
	defaultReq := &PrometheusRequest{
		Query: "sum(up)",
	}
	for _, tc := range []struct {
		name               string
		req                tripperware.Request
		resps              []*PrometheusInstantQueryResponse
		expectedResp       string
		expectedErr        error
		cancelBeforeDecode bool
		expectedDecodeErr  error
		cancelBeforeMerge  bool
	}{
		{
			name: "empty response",
			req:  defaultReq,
			resps: []*PrometheusInstantQueryResponse{
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: make([]*Sample, 0),
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[]}}`,
		},
		{
			name: "empty response with stats",
			req:  defaultReq,
			resps: []*PrometheusInstantQueryResponse{
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{},
								},
							},
						},
						Stats: &tripperware.PrometheusResponseStats{
							Samples: &tripperware.PrometheusResponseSamplesStats{
								TotalQueryableSamplesPerStep: []*tripperware.PrometheusResponseQueryableSamplesStatsPerStep{},
								TotalQueryableSamples:        0,
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[],"stats":{"samples":{"totalQueryableSamples":0,"totalQueryableSamplesPerStep":[]}}}}`,
		},
		{
			name: "single response",
			req:  defaultReq,
			resps: []*PrometheusInstantQueryResponse{
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "up"},
											},
											Sample: cortexpb.Sample{Value: 1, TimestampMs: 1000},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}]}}`,
		},
		{
			name: "single response with stats",
			req:  defaultReq,
			resps: []*PrometheusInstantQueryResponse{
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{Name: "__name__", Value: "up"},
											},
											Sample: cortexpb.Sample{Value: 1, TimestampMs: 1000},
										},
									},
								},
							},
						},
						Stats: &tripperware.PrometheusResponseStats{
							Samples: &tripperware.PrometheusResponseSamplesStats{
								TotalQueryableSamplesPerStep: []*tripperware.PrometheusResponseQueryableSamplesStatsPerStep{
									{Value: 10, TimestampMs: 1000},
								},
								TotalQueryableSamples: 10,
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
		},
		{
			name: "duplicated response",
			req:  defaultReq,
			resps: []*PrometheusInstantQueryResponse{
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "up"},
											},
											Sample: cortexpb.Sample{Value: 1, TimestampMs: 1000},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "up"},
											},
											Sample: cortexpb.Sample{Value: 1, TimestampMs: 1000},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}]}}`,
		},
		{
			name: "duplicated response with stats",
			req:  defaultReq,
			resps: []*PrometheusInstantQueryResponse{
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{Name: "__name__", Value: "up"},
											},
											Sample: cortexpb.Sample{Value: 1, TimestampMs: 1000},
										},
									},
								},
							},
						},
						Stats: &tripperware.PrometheusResponseStats{
							Samples: &tripperware.PrometheusResponseSamplesStats{
								TotalQueryableSamplesPerStep: []*tripperware.PrometheusResponseQueryableSamplesStatsPerStep{
									{Value: 10, TimestampMs: 1000},
								},
								TotalQueryableSamples: 10,
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{Name: "__name__", Value: "up"},
											},
											Sample: cortexpb.Sample{Value: 1, TimestampMs: 1000},
										},
									},
								},
							},
						},
						Stats: &tripperware.PrometheusResponseStats{
							Samples: &tripperware.PrometheusResponseSamplesStats{
								TotalQueryableSamplesPerStep: []*tripperware.PrometheusResponseQueryableSamplesStatsPerStep{
									{Value: 10, TimestampMs: 1000},
								},
								TotalQueryableSamples: 10,
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[1,20]]}}}}`,
		},
		{
			name: "merge two responses",
			req:  defaultReq,
			resps: []*PrometheusInstantQueryResponse{
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "up"},
												{"job", "foo"},
											},
											Sample: cortexpb.Sample{Value: 1, TimestampMs: 1000},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "up"},
												{"job", "bar"},
											},
											Sample: cortexpb.Sample{Value: 2, TimestampMs: 2000},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]},{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}]}}`,
		},
		{
			name: "merge two responses with sort",
			req:  &PrometheusRequest{Query: "sort(sum by (job) (up))"},
			resps: []*PrometheusInstantQueryResponse{
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "up"},
												{"job", "foo"},
											},
											Sample: cortexpb.Sample{Value: 1, TimestampMs: 1000},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "up"},
												{"job", "bar"},
											},
											Sample: cortexpb.Sample{Value: 2, TimestampMs: 1000},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]},{"metric":{"__name__":"up","job":"bar"},"value":[1,"2"]}]}}`,
		},
		{
			name: "merge two responses with sort_desc",
			req:  &PrometheusRequest{Query: "sort_desc(sum by (job) (up))"},
			resps: []*PrometheusInstantQueryResponse{
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "up"},
												{"job", "foo"},
											},
											Sample: cortexpb.Sample{Value: 1, TimestampMs: 1000},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "up"},
												{"job", "bar"},
											},
											Sample: cortexpb.Sample{Value: 2, TimestampMs: 1000},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[1,"2"]},{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}]}}`,
		},
		{
			name: "merge two responses with topk",
			req:  &PrometheusRequest{Query: "topk(10, up) by(job)"},
			resps: []*PrometheusInstantQueryResponse{
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "up"},
												{"job", "foo"},
											},
											Sample: cortexpb.Sample{Value: 1, TimestampMs: 1000},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "up"},
												{"job", "bar"},
											},
											Sample: cortexpb.Sample{Value: 2, TimestampMs: 1000},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]},{"metric":{"__name__":"up","job":"bar"},"value":[1,"2"]}]}}`,
		},
		{
			name: "merge two responses with stats",
			req:  defaultReq,
			resps: []*PrometheusInstantQueryResponse{
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{Name: "__name__", Value: "up"},
												{Name: "job", Value: "foo"},
											},
											Sample: cortexpb.Sample{Value: 1, TimestampMs: 1000},
										},
									},
								},
							},
						},
						Stats: &tripperware.PrometheusResponseStats{
							Samples: &tripperware.PrometheusResponseSamplesStats{
								TotalQueryableSamplesPerStep: []*tripperware.PrometheusResponseQueryableSamplesStatsPerStep{
									{Value: 10, TimestampMs: 1000},
								},
								TotalQueryableSamples: 10,
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{Name: "__name__", Value: "up"},
												{Name: "job", Value: "bar"},
											},
											Sample: cortexpb.Sample{Value: 2, TimestampMs: 2000},
										},
									},
								},
							},
						},
						Stats: &tripperware.PrometheusResponseStats{
							Samples: &tripperware.PrometheusResponseSamplesStats{
								TotalQueryableSamplesPerStep: []*tripperware.PrometheusResponseQueryableSamplesStatsPerStep{
									{Value: 10, TimestampMs: 1000},
								},
								TotalQueryableSamples: 10,
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]},{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[1,20]]}}}}`,
		},
		{
			name: "responses don't contain vector, should return an error",
			req:  defaultReq,
			resps: []*PrometheusInstantQueryResponse{
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValString.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_RawBytes{
								RawBytes: []byte(`{"resultType":"string","result":[1662682521.409,"foo"]}`),
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValString.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_RawBytes{
								RawBytes: []byte(`{"resultType":"string","result":[1662682521.409,"foo"]}`),
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
			},
			expectedErr: fmt.Errorf("unexpected result type on instant query: %s", "string"),
		},
		{
			name: "single matrix response",
			req:  defaultReq,
			resps: []*PrometheusInstantQueryResponse{
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValMatrix.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Matrix{
								Matrix: &Matrix{
									SampleStreams: []tripperware.SampleStream{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "up"},
											},
											Samples: []cortexpb.Sample{
												{Value: 1, TimestampMs: 1000},
												{Value: 2, TimestampMs: 2000},
											},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
			},
			expectedResp: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up"},"values":[[1,"1"],[2,"2"]]}]}}`,
		},
		{
			name: "multiple matrix responses without duplicated series",
			req:  defaultReq,
			resps: []*PrometheusInstantQueryResponse{
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValMatrix.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Matrix{
								Matrix: &Matrix{
									SampleStreams: []tripperware.SampleStream{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "bar"},
											},
											Samples: []cortexpb.Sample{
												{Value: 1, TimestampMs: 1000},
												{Value: 2, TimestampMs: 2000},
											},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValMatrix.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Matrix{
								Matrix: &Matrix{
									SampleStreams: []tripperware.SampleStream{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "foo"},
											},
											Samples: []cortexpb.Sample{
												{Value: 3, TimestampMs: 3000},
												{Value: 4, TimestampMs: 4000},
											},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
			},
			expectedResp: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[1,"1"],[2,"2"]]},{"metric":{"__name__":"foo"},"values":[[3,"3"],[4,"4"]]}]}}`,
		},
		{
			name: "multiple matrix responses with duplicated series, but not same samples",
			req:  defaultReq,
			resps: []*PrometheusInstantQueryResponse{
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValMatrix.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Matrix{
								Matrix: &Matrix{
									SampleStreams: []tripperware.SampleStream{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "bar"},
											},
											Samples: []cortexpb.Sample{
												{Value: 1, TimestampMs: 1000},
												{Value: 2, TimestampMs: 2000},
											},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValMatrix.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Matrix{
								Matrix: &Matrix{
									SampleStreams: []tripperware.SampleStream{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "bar"},
											},
											Samples: []cortexpb.Sample{
												{Value: 3, TimestampMs: 3000},
											},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
			},
			expectedResp: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[1,"1"],[2,"2"],[3,"3"]]}]}}`,
		},
		{
			name: "multiple matrix responses with duplicated series and same samples",
			req:  defaultReq,
			resps: []*PrometheusInstantQueryResponse{
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValMatrix.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Matrix{
								Matrix: &Matrix{
									SampleStreams: []tripperware.SampleStream{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "bar"},
											},
											Samples: []cortexpb.Sample{
												{Value: 1, TimestampMs: 1000},
												{Value: 2, TimestampMs: 2000},
											},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValMatrix.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Matrix{
								Matrix: &Matrix{
									SampleStreams: []tripperware.SampleStream{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "bar"},
											},
											Samples: []cortexpb.Sample{
												{Value: 1, TimestampMs: 1000},
												{Value: 2, TimestampMs: 2000},
												{Value: 3, TimestampMs: 3000},
											},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
			},
			expectedResp: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[1,"1"],[2,"2"],[3,"3"]]}]}}`,
		},
		{
			name: "context cancelled before decoding response",
			req:  defaultReq,
			resps: []*PrometheusInstantQueryResponse{
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "up"},
												{"job", "foo"},
											},
											Sample: cortexpb.Sample{Value: 1, TimestampMs: 1000},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "up"},
												{"job", "bar"},
											},
											Sample: cortexpb.Sample{Value: 2, TimestampMs: 1000},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
			},
			expectedDecodeErr:  context.Canceled,
			cancelBeforeDecode: true,
		},
		{
			name: "context cancelled before merging response",
			req:  defaultReq,
			resps: []*PrometheusInstantQueryResponse{
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "up"},
												{"job", "foo"},
											},
											Sample: cortexpb.Sample{Value: 1, TimestampMs: 1000},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
				{
					Status: "success",
					Data: PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: PrometheusInstantQueryResult{
							Result: &PrometheusInstantQueryResult_Vector{
								Vector: &Vector{
									Samples: []*Sample{
										{
											Labels: []cortexpb.LabelAdapter{
												{"__name__", "up"},
												{"job", "bar"},
											},
											Sample: cortexpb.Sample{Value: 2, TimestampMs: 1000},
										},
									},
								},
							},
						},
					},
					Headers: []*tripperware.PrometheusResponseHeader{
						{Name: "Content-Type", Values: []string{"application/x-protobuf"}},
					},
				},
			},
			expectedErr:       context.Canceled,
			cancelBeforeMerge: true,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancelCtx := context.WithCancel(context.Background())

			var resps []tripperware.Response
			for _, r := range tc.resps {
				protobuf, err := proto.Marshal(r)
				hr := &http.Response{
					StatusCode: 200,
					Header:     http.Header{"Content-Type": []string{"application/x-protobuf"}},
					Body:       io.NopCloser(bytes.NewBuffer(protobuf)),
				}

				if tc.cancelBeforeDecode {
					cancelCtx()
				}
				dr, err := InstantQueryCodec.DecodeResponse(ctx, hr, nil)
				assert.Equal(t, tc.expectedDecodeErr, err)
				if err != nil {
					cancelCtx()
					return
				}
				resps = append(resps, dr)
			}

			if tc.cancelBeforeMerge {
				cancelCtx()
			}
			resp, err := InstantQueryCodec.MergeResponse(ctx, tc.req, resps...)
			assert.Equal(t, tc.expectedErr, err)
			if err != nil {
				cancelCtx()
				return
			}
			dr, err := InstantQueryCodec.EncodeResponse(ctx, resp)
			assert.Equal(t, tc.expectedErr, err)
			contents, err := io.ReadAll(dr.Body)
			assert.Equal(t, tc.expectedErr, err)
			assert.Equal(t, tc.expectedResp, string(contents))
			cancelCtx()
		})
	}
}

func Test_sortPlanForQuery(t *testing.T) {
	tc := []struct {
		query        string
		expectedPlan sortPlan
		err          bool
	}{
		{
			query:        "invalid(10, up)",
			expectedPlan: mergeOnly,
			err:          true,
		},
		{
			query:        "topk(10, up)",
			expectedPlan: mergeOnly,
			err:          false,
		},
		{
			query:        "bottomk(10, up)",
			expectedPlan: mergeOnly,
			err:          false,
		},
		{
			query:        "1 + topk(10, up)",
			expectedPlan: sortByLabels,
			err:          false,
		},
		{
			query:        "1 + sort_desc(sum by (job) (up) )",
			expectedPlan: sortByValuesDesc,
			err:          false,
		},
		{
			query:        "sort(topk by (job) (10, up))",
			expectedPlan: sortByValuesAsc,
			err:          false,
		},
		{
			query:        "topk(5, up) by (job) + sort_desc(up)",
			expectedPlan: sortByValuesDesc,
			err:          false,
		},
		{
			query:        "sort(up) + topk(5, up) by (job)",
			expectedPlan: sortByValuesAsc,
			err:          false,
		},
		{
			query:        "sum(up) by (job)",
			expectedPlan: sortByLabels,
			err:          false,
		},
	}

	for _, tc := range tc {
		t.Run(tc.query, func(t *testing.T) {
			p, err := sortPlanForQuery(tc.query)
			if tc.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedPlan, p)
			}
		})
	}
}

func Benchmark_Decode(b *testing.B) {
	maxSamplesCount := 1000000
	samples := make([]tripperware.SampleStream, maxSamplesCount)

	for i := 0; i < maxSamplesCount; i++ {
		samples[i].Labels = append(samples[i].Labels, cortexpb.LabelAdapter{Name: fmt.Sprintf("Sample%v", i), Value: fmt.Sprintf("Value%v", i)})
		samples[i].Labels = append(samples[i].Labels, cortexpb.LabelAdapter{Name: fmt.Sprintf("Sample2%v", i), Value: fmt.Sprintf("Value2%v", i)})
		samples[i].Labels = append(samples[i].Labels, cortexpb.LabelAdapter{Name: fmt.Sprintf("Sample3%v", i), Value: fmt.Sprintf("Value3%v", i)})
		samples[i].Samples = append(samples[i].Samples, cortexpb.Sample{TimestampMs: int64(i), Value: float64(i)})
	}

	for name, tc := range map[string]struct {
		sampleStream []tripperware.SampleStream
	}{
		"100 samples": {
			sampleStream: samples[:100],
		},
		"1000 samples": {
			sampleStream: samples[:1000],
		},
		"10000 samples": {
			sampleStream: samples[:10000],
		},
		"100000 samples": {
			sampleStream: samples[:100000],
		},
		"1000000 samples": {
			sampleStream: samples[:1000000],
		},
	} {
		b.Run(name, func(b *testing.B) {
			r := PrometheusInstantQueryResponse{
				Data: PrometheusInstantQueryData{
					ResultType: model.ValMatrix.String(),
					Result: PrometheusInstantQueryResult{
						Result: &PrometheusInstantQueryResult_Matrix{
							Matrix: &Matrix{
								SampleStreams: tc.sampleStream,
							},
						},
					},
				},
			}

			body, err := proto.Marshal(&r)
			require.NoError(b, err)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				response := &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(bytes.NewBuffer(body)),
				}
				_, err := InstantQueryCodec.DecodeResponse(context.Background(), response, nil)
				require.NoError(b, err)
			}
		})
	}

}
