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

package pcommon // import "go.opentelemetry.io/collector/pdata/pcommon"

import (
	"go.opentelemetry.io/collector/pdata/internal"
)

// TraceState represents the trace state from the w3c-trace-context.
type TraceState internal.TraceState

func NewTraceState() TraceState {
<<<<<<< HEAD
	state := internal.StateMutable
	return TraceState(internal.NewTraceState(new(string), &state))
=======
	return TraceState(internal.NewTraceState(new(string)))
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

func (ms TraceState) getOrig() *string {
	return internal.GetOrigTraceState(internal.TraceState(ms))
}

<<<<<<< HEAD
func (ms TraceState) getState() *internal.State {
	return internal.GetTraceStateState(internal.TraceState(ms))
}

=======
>>>>>>> 90dc0587b (Initial OTLP ingest support)
// AsRaw returns the string representation of the tracestate in w3c-trace-context format: https://www.w3.org/TR/trace-context/#tracestate-header
func (ms TraceState) AsRaw() string {
	return *ms.getOrig()
}

// FromRaw copies the string representation in w3c-trace-context format of the tracestate into this TraceState.
func (ms TraceState) FromRaw(v string) {
<<<<<<< HEAD
	ms.getState().AssertMutable()
=======
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	*ms.getOrig() = v
}

// MoveTo moves the TraceState instance overriding the destination
// and resetting the current instance to its zero value.
func (ms TraceState) MoveTo(dest TraceState) {
<<<<<<< HEAD
	ms.getState().AssertMutable()
	dest.getState().AssertMutable()
=======
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	*dest.getOrig() = *ms.getOrig()
	*ms.getOrig() = ""
}

// CopyTo copies the TraceState instance overriding the destination.
func (ms TraceState) CopyTo(dest TraceState) {
<<<<<<< HEAD
	dest.getState().AssertMutable()
=======
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	*dest.getOrig() = *ms.getOrig()
}
