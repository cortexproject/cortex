// Copyright The OpenTelemetry Authors
<<<<<<< HEAD
// SPDX-License-Identifier: Apache-2.0
=======
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
>>>>>>> 90dc0587b (Initial OTLP ingest support)

package internal // import "go.opentelemetry.io/collector/pdata/internal"

import (
	otlpcollectortrace "go.opentelemetry.io/collector/pdata/internal/data/protogen/collector/trace/v1"
	otlptrace "go.opentelemetry.io/collector/pdata/internal/data/protogen/trace/v1"
)

type Traces struct {
<<<<<<< HEAD
	orig  *otlpcollectortrace.ExportTraceServiceRequest
	state *State
=======
	orig *otlpcollectortrace.ExportTraceServiceRequest
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

func GetOrigTraces(ms Traces) *otlpcollectortrace.ExportTraceServiceRequest {
	return ms.orig
}

<<<<<<< HEAD
func GetTracesState(ms Traces) *State {
	return ms.state
}

func SetTracesState(ms Traces, state State) {
	*ms.state = state
}

func NewTraces(orig *otlpcollectortrace.ExportTraceServiceRequest, state *State) Traces {
	return Traces{orig: orig, state: state}
=======
func NewTraces(orig *otlpcollectortrace.ExportTraceServiceRequest) Traces {
	return Traces{orig: orig}
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

// TracesToProto internal helper to convert Traces to protobuf representation.
func TracesToProto(l Traces) otlptrace.TracesData {
	return otlptrace.TracesData{
		ResourceSpans: l.orig.ResourceSpans,
	}
}

// TracesFromProto internal helper to convert protobuf representation to Traces.
<<<<<<< HEAD
// This function set exclusive state assuming that it's called only once per Traces.
func TracesFromProto(orig otlptrace.TracesData) Traces {
	state := StateMutable
	return NewTraces(&otlpcollectortrace.ExportTraceServiceRequest{
		ResourceSpans: orig.ResourceSpans,
	}, &state)
=======
func TracesFromProto(orig otlptrace.TracesData) Traces {
	return Traces{orig: &otlpcollectortrace.ExportTraceServiceRequest{
		ResourceSpans: orig.ResourceSpans,
	}}
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}
