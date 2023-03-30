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
	otlpcommon "go.opentelemetry.io/collector/pdata/internal/data/protogen/common/v1"
)

type Value struct {
<<<<<<< HEAD
	orig  *otlpcommon.AnyValue
	state *State
=======
	orig *otlpcommon.AnyValue
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

func GetOrigValue(ms Value) *otlpcommon.AnyValue {
	return ms.orig
}

<<<<<<< HEAD
func GetValueState(ms Value) *State {
	return ms.state
}

func NewValue(orig *otlpcommon.AnyValue, state *State) Value {
	return Value{orig: orig, state: state}
=======
func NewValue(orig *otlpcommon.AnyValue) Value {
	return Value{orig: orig}
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

func FillTestValue(dest Value) {
	dest.orig.Value = &otlpcommon.AnyValue_StringValue{StringValue: "v"}
}

func GenerateTestValue() Value {
	var orig otlpcommon.AnyValue
<<<<<<< HEAD
	state := StateMutable
	ms := NewValue(&orig, &state)
=======
	ms := NewValue(&orig)
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	FillTestValue(ms)
	return ms
}
