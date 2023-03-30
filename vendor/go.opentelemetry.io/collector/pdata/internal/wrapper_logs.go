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
	otlpcollectorlog "go.opentelemetry.io/collector/pdata/internal/data/protogen/collector/logs/v1"
	otlplogs "go.opentelemetry.io/collector/pdata/internal/data/protogen/logs/v1"
)

type Logs struct {
<<<<<<< HEAD
	orig  *otlpcollectorlog.ExportLogsServiceRequest
	state *State
=======
	orig *otlpcollectorlog.ExportLogsServiceRequest
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

func GetOrigLogs(ms Logs) *otlpcollectorlog.ExportLogsServiceRequest {
	return ms.orig
}

<<<<<<< HEAD
func GetLogsState(ms Logs) *State {
	return ms.state
}

func SetLogsState(ms Logs, state State) {
	*ms.state = state
}

func NewLogs(orig *otlpcollectorlog.ExportLogsServiceRequest, state *State) Logs {
	return Logs{orig: orig, state: state}
=======
func NewLogs(orig *otlpcollectorlog.ExportLogsServiceRequest) Logs {
	return Logs{orig: orig}
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

// LogsToProto internal helper to convert Logs to protobuf representation.
func LogsToProto(l Logs) otlplogs.LogsData {
	return otlplogs.LogsData{
		ResourceLogs: l.orig.ResourceLogs,
	}
}

// LogsFromProto internal helper to convert protobuf representation to Logs.
<<<<<<< HEAD
// This function set exclusive state assuming that it's called only once per Logs.
func LogsFromProto(orig otlplogs.LogsData) Logs {
	state := StateMutable
	return NewLogs(&otlpcollectorlog.ExportLogsServiceRequest{
		ResourceLogs: orig.ResourceLogs,
	}, &state)
=======
func LogsFromProto(orig otlplogs.LogsData) Logs {
	return Logs{orig: &otlpcollectorlog.ExportLogsServiceRequest{
		ResourceLogs: orig.ResourceLogs,
	}}
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}
