// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prism

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/storage/remote/generic"
	"golang.org/x/net/context"

	"github.com/weaveworks/prism/user"
)

// legacy from scope as a service.
const userIDHeaderName = "X-Scope-OrgID"

// SampleAppender is the interface to append samples to both, local and remote
// storage. All methods are goroutine-safe.
type SampleAppender interface {
	Append(context.Context, []*model.Sample) error
}

func parseRequest(w http.ResponseWriter, r *http.Request, req proto.Message) (ctx context.Context, abort bool) {
	userID := r.Header.Get(userIDHeaderName)
	if userID == "" {
		http.Error(w, "", http.StatusUnauthorized)
		return nil, true
	}

	ctx = user.WithID(context.Background(), userID)
	buf := bytes.Buffer{}
	_, err := buf.ReadFrom(r.Body)
	if err != nil {
		log.Errorf(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil, true
	}
	err = proto.Unmarshal(buf.Bytes(), req)
	if err != nil {
		log.Errorf(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil, true
	}

	return ctx, false
}

func writeResponse(w http.ResponseWriter, resp proto.Message) {
	data, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err = w.Write(data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// TODO: set Content-type.
}

type grpcSampleAppender struct {
	SampleAppender
}

func (g grpcSampleAppender) Write(ctx context.Context, req *remote.WriteRequest) (*remote.WriteResponse, error) {
	var samples []*model.Sample
	for _, ts := range req.Timeseries {
		metric := model.Metric{}
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		for _, s := range ts.Samples {
			samples = append(samples, &model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.TimestampMs),
			})
		}
	}

	if err := g.Append(ctx, samples); err != nil {
		return nil, err
	}

	return &remote.WriteResponse{}, nil
}

// AppenderHandler returns a http.Handler that accepts proto encoded samples.
func AppenderHandler(appender SampleAppender) http.Handler {
	grpcHandler := grpcSampleAppender{appender}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID := r.Header.Get(userIDHeaderName)
		if userID == "" {
			http.Error(w, "", http.StatusUnauthorized)
			return
		}
		ctx := user.WithID(context.Background(), userID)

		reqBuf, err := ioutil.ReadAll(snappy.NewReader(r.Body))
		if err != nil {
			log.Errorf("read err: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req remote.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			log.Errorf("unmarshall err: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := grpcHandler.Write(ctx, &req)
		if err != nil {
			log.Errorf("grpc write err: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		respBuf, err := proto.Marshal(resp)
		if err != nil {
			log.Errorf("marshall err: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if _, err := snappy.NewWriter(w).Write(respBuf); err != nil {
			log.Errorf("write err: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Add("Content-Encoding", "snappy")
		w.WriteHeader(http.StatusOK)
	})
}

// QueryHandler returns a http.Handler that accepts protobuf formatted
// query requests and serves them.
func QueryHandler(querier Querier) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := &generic.GenericReadRequest{}
		ctx, abort := parseRequest(w, r, req)
		if abort {
			return
		}

		matchers := make(metric.LabelMatchers, 0, len(req.Matchers))
		for _, matcher := range req.Matchers {
			var mtype metric.MatchType
			switch matcher.GetType() {
			case generic.MatchType_EQUAL:
				mtype = metric.Equal
			case generic.MatchType_NOT_EQUAL:
				mtype = metric.NotEqual
			case generic.MatchType_REGEX_MATCH:
				mtype = metric.RegexMatch
			case generic.MatchType_REGEX_NO_MATCH:
				mtype = metric.RegexNoMatch
			default:
				http.Error(w, "invalid matcher type", http.StatusBadRequest)
				return
			}
			matcher, err := metric.NewLabelMatcher(mtype, model.LabelName(matcher.GetName()), model.LabelValue(matcher.GetValue()))
			if err != nil {
				http.Error(w, fmt.Sprintf("error creating matcher: %v", err), http.StatusBadRequest)
				return
			}
			matchers = append(matchers, matcher)
		}

		start := model.Time(req.GetStartTimestampMs())
		end := model.Time(req.GetEndTimestampMs())

		res, err := querier.Query(ctx, start, end, matchers...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := &generic.GenericReadResponse{}
		for _, ss := range res {
			ts := &generic.TimeSeries{
				Name: proto.String(string(ss.Metric[model.MetricNameLabel])),
			}
			for k, v := range ss.Metric {
				if k != model.MetricNameLabel {
					ts.Labels = append(ts.Labels,
						&generic.LabelPair{
							Name:  proto.String(string(k)),
							Value: proto.String(string(v)),
						})
				}
			}
			ts.Samples = make([]*generic.Sample, 0, len(ss.Values))
			for _, s := range ss.Values {
				ts.Samples = append(ts.Samples, &generic.Sample{
					Value:       proto.Float64(float64(s.Value)),
					TimestampMs: proto.Int64(int64(s.Timestamp)),
				})
			}
			resp.Timeseries = append(resp.Timeseries, ts)
		}

		writeResponse(w, resp)
	})
}

// LabelValuesHandler handles label values
func LabelValuesHandler(querier Querier) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := &generic.GenericLabelValuesRequest{}
		ctx, abort := parseRequest(w, r, req)
		if abort {
			return
		}

		values, err := querier.LabelValuesForLabelName(ctx, model.LabelName(req.GetLabelName()))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := &generic.GenericLabelValuesResponse{}
		for _, v := range values {
			resp.LabelValues = append(resp.LabelValues, string(v))
		}

		writeResponse(w, resp)
	})
}
