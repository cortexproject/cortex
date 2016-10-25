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

package cortex

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/prometheus/prometheus/storage/remote"
	"golang.org/x/net/context"

	"github.com/weaveworks/cortex/user"
)

// IngesterClient is a client library for the ingester
type IngesterClient struct {
	address string
	client  http.Client
	timeout time.Duration
}

// NewIngesterClient makes a new IngesterClient.  This client is careful to
// propagate the user ID from Distributor -> Ingester.
func NewIngesterClient(address string, timeout time.Duration) (*IngesterClient, error) {
	client := http.Client{
		Timeout: timeout,
	}
	return &IngesterClient{
		address: address,
		client:  client,
		timeout: timeout,
	}, nil
}

// Append adds new samples to the ingester
func (c *IngesterClient) Append(ctx context.Context, samples []*model.Sample) error {
	req := &remote.WriteRequest{
		Timeseries: make([]*remote.TimeSeries, 0, len(samples)),
	}

	for _, s := range samples {
		ts := &remote.TimeSeries{
			Labels: make([]*remote.LabelPair, 0, len(s.Metric)),
		}
		for k, v := range s.Metric {
			ts.Labels = append(ts.Labels,
				&remote.LabelPair{
					Name:  string(k),
					Value: string(v),
				})
		}
		ts.Samples = []*remote.Sample{
			{
				Value:       float64(s.Value),
				TimestampMs: int64(s.Timestamp),
			},
		}
		req.Timeseries = append(req.Timeseries, ts)
	}

	return c.doRequest(ctx, "/push", req, nil, true)
}

// Query implements Querier.
func (c *IngesterClient) Query(ctx context.Context, from, to model.Time, matchers ...*metric.LabelMatcher) (model.Matrix, error) {
	req := &ReadRequest{
		StartTimestampMs: int64(from),
		EndTimestampMs:   int64(to),
	}
	for _, matcher := range matchers {
		var mType MatchType
		switch matcher.Type {
		case metric.Equal:
			mType = MatchType_EQUAL
		case metric.NotEqual:
			mType = MatchType_NOT_EQUAL
		case metric.RegexMatch:
			mType = MatchType_REGEX_MATCH
		case metric.RegexNoMatch:
			mType = MatchType_REGEX_NO_MATCH
		default:
			panic("invalid matcher type")
		}
		req.Matchers = append(req.Matchers, &LabelMatcher{
			Type:  mType,
			Name:  string(matcher.Name),
			Value: string(matcher.Value),
		})
	}

	resp := &ReadResponse{}
	err := c.doRequest(ctx, "/query", req, resp, false)
	if err != nil {
		return nil, err
	}

	m := make(model.Matrix, 0, len(resp.Timeseries))
	for _, ts := range resp.Timeseries {
		var ss model.SampleStream
		ss.Metric = model.Metric{}
		for _, l := range ts.Labels {
			ss.Metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		ss.Values = make([]model.SamplePair, 0, len(ts.Samples))
		for _, s := range ts.Samples {
			ss.Values = append(ss.Values, model.SamplePair{
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.TimestampMs),
			})
		}
		m = append(m, &ss)
	}

	return m, nil
}

// LabelValuesForLabelName returns all of the label values that are associated with a given label name.
func (c *IngesterClient) LabelValuesForLabelName(ctx context.Context, ln model.LabelName) (model.LabelValues, error) {
	req := &LabelValuesRequest{
		LabelName: string(ln),
	}
	resp := &LabelValuesResponse{}
	err := c.doRequest(ctx, "/label_values", req, resp, false)
	if err != nil {
		return nil, err
	}

	values := make(model.LabelValues, 0, len(resp.LabelValues))
	for _, v := range resp.LabelValues {
		values = append(values, model.LabelValue(v))
	}
	return values, nil
}

func (c *IngesterClient) doRequest(ctx context.Context, endpoint string, req proto.Message, resp proto.Message, compressed bool) error {
	userID, err := user.GetID(ctx)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("unable to marshal request: %v", err)
	}

	buf := bytes.Buffer{}
	var writer io.Writer = &buf
	if compressed {
		writer = snappy.NewWriter(writer)
	}
	if _, err := writer.Write(data); err != nil {
		return err
	}

	httpReq, err := http.NewRequest("POST", fmt.Sprintf("http://%s%s", c.address, endpoint), &buf)
	if err != nil {
		return fmt.Errorf("unable to create request: %v", err)
	}
	httpReq.Header.Add(userIDHeaderName, userID)
	// TODO: This isn't actually the correct Content-type.
	httpReq.Header.Set("Content-Type", string(expfmt.FmtProtoDelim))
	httpResp, err := c.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("error sending request: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode/100 != 2 {
		return fmt.Errorf("server returned HTTP status %s", httpResp.Status)
	}
	if resp == nil {
		return nil
	}

	buf.Reset()
	reader := httpResp.Body.(io.Reader)
	if compressed {
		reader = snappy.NewReader(reader)
	}
	if _, err = buf.ReadFrom(reader); err != nil {
		return fmt.Errorf("unable to read response body: %v", err)
	}

	err = proto.Unmarshal(buf.Bytes(), resp)
	if err != nil {
		return fmt.Errorf("unable to unmarshal response body: %v", err)
	}
	return nil
}
