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

<<<<<<< HEAD
	jsoniter "github.com/json-iterator/go"

	"go.opentelemetry.io/collector/pdata/internal"
	otlpcollectormetrics "go.opentelemetry.io/collector/pdata/internal/data/protogen/collector/metrics/v1"
	"go.opentelemetry.io/collector/pdata/internal/json"
=======
	otlpcollectormetrics "go.opentelemetry.io/collector/pdata/internal/data/protogen/collector/metrics/v1"
	"go.opentelemetry.io/collector/pdata/pmetric/internal/pmetricjson"
>>>>>>> 90dc0587b (Initial OTLP ingest support)
)

// ExportResponse represents the response for gRPC/HTTP client/server.
type ExportResponse struct {
<<<<<<< HEAD
	orig  *otlpcollectormetrics.ExportMetricsServiceResponse
	state *internal.State
=======
	orig *otlpcollectormetrics.ExportMetricsServiceResponse
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

// NewExportResponse returns an empty ExportResponse.
func NewExportResponse() ExportResponse {
<<<<<<< HEAD
	state := internal.StateMutable
	return ExportResponse{
		orig:  &otlpcollectormetrics.ExportMetricsServiceResponse{},
		state: &state,
	}
=======
	return ExportResponse{orig: &otlpcollectormetrics.ExportMetricsServiceResponse{}}
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

// MarshalProto marshals ExportResponse into proto bytes.
func (ms ExportResponse) MarshalProto() ([]byte, error) {
	return ms.orig.Marshal()
}

// UnmarshalProto unmarshalls ExportResponse from proto bytes.
func (ms ExportResponse) UnmarshalProto(data []byte) error {
	return ms.orig.Unmarshal(data)
}

// MarshalJSON marshals ExportResponse into JSON bytes.
func (ms ExportResponse) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
<<<<<<< HEAD
	if err := json.Marshal(&buf, ms.orig); err != nil {
=======
	if err := pmetricjson.JSONMarshaler.Marshal(&buf, ms.orig); err != nil {
>>>>>>> 90dc0587b (Initial OTLP ingest support)
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalJSON unmarshalls ExportResponse from JSON bytes.
func (ms ExportResponse) UnmarshalJSON(data []byte) error {
<<<<<<< HEAD
	iter := jsoniter.ConfigFastest.BorrowIterator(data)
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	ms.unmarshalJsoniter(iter)
	return iter.Error
=======
	return pmetricjson.UnmarshalExportMetricsServiceResponse(data, ms.orig)
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

// PartialSuccess returns the ExportLogsPartialSuccess associated with this ExportResponse.
func (ms ExportResponse) PartialSuccess() ExportPartialSuccess {
<<<<<<< HEAD
	return newExportPartialSuccess(&ms.orig.PartialSuccess, ms.state)
}

func (ms ExportResponse) unmarshalJsoniter(iter *jsoniter.Iterator) {
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, f string) bool {
		switch f {
		case "partial_success", "partialSuccess":
			ms.PartialSuccess().unmarshalJsoniter(iter)
		default:
			iter.Skip()
		}
		return true
	})
}

func (ms ExportPartialSuccess) unmarshalJsoniter(iter *jsoniter.Iterator) {
	iter.ReadObjectCB(func(iterator *jsoniter.Iterator, f string) bool {
		switch f {
		case "rejected_data_points", "rejectedDataPoints":
			ms.orig.RejectedDataPoints = json.ReadInt64(iter)
		case "error_message", "errorMessage":
			ms.orig.ErrorMessage = iter.ReadString()
		default:
			iter.Skip()
		}
		return true
	})
=======
	return newExportPartialSuccess(&ms.orig.PartialSuccess)
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}
