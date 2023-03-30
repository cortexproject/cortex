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
	otlpcollectormetrics "go.opentelemetry.io/collector/pdata/internal/data/protogen/collector/metrics/v1"
	otlpmetrics "go.opentelemetry.io/collector/pdata/internal/data/protogen/metrics/v1"
)

type Metrics struct {
<<<<<<< HEAD
	orig  *otlpcollectormetrics.ExportMetricsServiceRequest
	state *State
=======
	orig *otlpcollectormetrics.ExportMetricsServiceRequest
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

func GetOrigMetrics(ms Metrics) *otlpcollectormetrics.ExportMetricsServiceRequest {
	return ms.orig
}

<<<<<<< HEAD
func GetMetricsState(ms Metrics) *State {
	return ms.state
}

func SetMetricsState(ms Metrics, state State) {
	*ms.state = state
}

func NewMetrics(orig *otlpcollectormetrics.ExportMetricsServiceRequest, state *State) Metrics {
	return Metrics{orig: orig, state: state}
=======
func NewMetrics(orig *otlpcollectormetrics.ExportMetricsServiceRequest) Metrics {
	return Metrics{orig: orig}
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

// MetricsToProto internal helper to convert Metrics to protobuf representation.
func MetricsToProto(l Metrics) otlpmetrics.MetricsData {
	return otlpmetrics.MetricsData{
		ResourceMetrics: l.orig.ResourceMetrics,
	}
}

// MetricsFromProto internal helper to convert protobuf representation to Metrics.
<<<<<<< HEAD
// This function set exclusive state assuming that it's called only once per Metrics.
func MetricsFromProto(orig otlpmetrics.MetricsData) Metrics {
	state := StateMutable
	return NewMetrics(&otlpcollectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: orig.ResourceMetrics,
	}, &state)
=======
func MetricsFromProto(orig otlpmetrics.MetricsData) Metrics {
	return Metrics{orig: &otlpcollectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: orig.ResourceMetrics,
	}}
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}
