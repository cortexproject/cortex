// Copyright The OpenTelemetry Authors
<<<<<<< HEAD
// SPDX-License-Identifier: Apache-2.0
=======
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
>>>>>>> 90dc0587b (Initial OTLP ingest support)

package pmetricotlp // import "go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

import (
	"bytes"

	"go.opentelemetry.io/collector/pdata/internal"
	otlpcollectormetrics "go.opentelemetry.io/collector/pdata/internal/data/protogen/collector/metrics/v1"
<<<<<<< HEAD
	"go.opentelemetry.io/collector/pdata/internal/json"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var jsonUnmarshaler = &pmetric.JSONUnmarshaler{}

// ExportRequest represents the request for gRPC/HTTP client/server.
// It's a wrapper for pmetric.Metrics data.
type ExportRequest struct {
	orig  *otlpcollectormetrics.ExportMetricsServiceRequest
	state *internal.State
=======
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/internal/pmetricjson"
)

// ExportRequest represents the request for gRPC/HTTP client/server.
// It's a wrapper for pmetric.Metrics data.
type ExportRequest struct {
	orig *otlpcollectormetrics.ExportMetricsServiceRequest
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

// NewExportRequest returns an empty ExportRequest.
func NewExportRequest() ExportRequest {
<<<<<<< HEAD
	state := internal.StateMutable
	return ExportRequest{
		orig:  &otlpcollectormetrics.ExportMetricsServiceRequest{},
		state: &state,
	}
=======
	return ExportRequest{orig: &otlpcollectormetrics.ExportMetricsServiceRequest{}}
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

// NewExportRequestFromMetrics returns a ExportRequest from pmetric.Metrics.
// Because ExportRequest is a wrapper for pmetric.Metrics,
// any changes to the provided Metrics struct will be reflected in the ExportRequest and vice versa.
func NewExportRequestFromMetrics(md pmetric.Metrics) ExportRequest {
<<<<<<< HEAD
	return ExportRequest{
		orig:  internal.GetOrigMetrics(internal.Metrics(md)),
		state: internal.GetMetricsState(internal.Metrics(md)),
	}
}

// MarshalProto marshals ExportRequest into proto bytes.
func (ms ExportRequest) MarshalProto() ([]byte, error) {
	return ms.orig.Marshal()
}

// UnmarshalProto unmarshalls ExportRequest from proto bytes.
func (ms ExportRequest) UnmarshalProto(data []byte) error {
	return ms.orig.Unmarshal(data)
}

// MarshalJSON marshals ExportRequest into JSON bytes.
func (ms ExportRequest) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	if err := json.Marshal(&buf, ms.orig); err != nil {
=======
	return ExportRequest{orig: internal.GetOrigMetrics(internal.Metrics(md))}
}

// MarshalProto marshals ExportRequest into proto bytes.
func (mr ExportRequest) MarshalProto() ([]byte, error) {
	return mr.orig.Marshal()
}

// UnmarshalProto unmarshalls ExportRequest from proto bytes.
func (mr ExportRequest) UnmarshalProto(data []byte) error {
	return mr.orig.Unmarshal(data)
}

// MarshalJSON marshals ExportRequest into JSON bytes.
func (mr ExportRequest) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	if err := pmetricjson.JSONMarshaler.Marshal(&buf, mr.orig); err != nil {
>>>>>>> 90dc0587b (Initial OTLP ingest support)
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalJSON unmarshalls ExportRequest from JSON bytes.
<<<<<<< HEAD
func (ms ExportRequest) UnmarshalJSON(data []byte) error {
	md, err := jsonUnmarshaler.UnmarshalMetrics(data)
	if err != nil {
		return err
	}
	*ms.orig = *internal.GetOrigMetrics(internal.Metrics(md))
	return nil
}

func (ms ExportRequest) Metrics() pmetric.Metrics {
	return pmetric.Metrics(internal.NewMetrics(ms.orig, ms.state))
=======
func (mr ExportRequest) UnmarshalJSON(data []byte) error {
	return pmetricjson.UnmarshalExportMetricsServiceRequest(data, mr.orig)
}

func (mr ExportRequest) Metrics() pmetric.Metrics {
	return pmetric.Metrics(internal.NewMetrics(mr.orig))
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}
