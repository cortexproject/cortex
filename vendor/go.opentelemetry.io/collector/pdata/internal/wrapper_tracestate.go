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

type TraceState struct {
<<<<<<< HEAD
	orig  *string
	state *State
=======
	orig *string
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

func GetOrigTraceState(ms TraceState) *string {
	return ms.orig
}

<<<<<<< HEAD
func GetTraceStateState(ms TraceState) *State {
	return ms.state
}

func NewTraceState(orig *string, state *State) TraceState {
	return TraceState{orig: orig, state: state}
=======
func NewTraceState(orig *string) TraceState {
	return TraceState{orig: orig}
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

func GenerateTestTraceState() TraceState {
	var orig string
<<<<<<< HEAD
	state := StateMutable
	ms := NewTraceState(&orig, &state)
=======
	ms := NewTraceState(&orig)
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	FillTestTraceState(ms)
	return ms
}

func FillTestTraceState(dest TraceState) {
	*dest.orig = "rojo=00f067aa0ba902b7"
}
