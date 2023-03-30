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

type Slice struct {
<<<<<<< HEAD
	orig  *[]otlpcommon.AnyValue
	state *State
=======
	orig *[]otlpcommon.AnyValue
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

func GetOrigSlice(ms Slice) *[]otlpcommon.AnyValue {
	return ms.orig
}

<<<<<<< HEAD
func GetSliceState(ms Slice) *State {
	return ms.state
}

func NewSlice(orig *[]otlpcommon.AnyValue, state *State) Slice {
	return Slice{orig: orig, state: state}
=======
func NewSlice(orig *[]otlpcommon.AnyValue) Slice {
	return Slice{orig: orig}
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

func GenerateTestSlice() Slice {
	orig := []otlpcommon.AnyValue{}
<<<<<<< HEAD
	state := StateMutable
	tv := NewSlice(&orig, &state)
=======
	tv := NewSlice(&orig)
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	FillTestSlice(tv)
	return tv
}

func FillTestSlice(tv Slice) {
	*tv.orig = make([]otlpcommon.AnyValue, 7)
	for i := 0; i < 7; i++ {
<<<<<<< HEAD
		state := StateMutable
		FillTestValue(NewValue(&(*tv.orig)[i], &state))
=======
		FillTestValue(NewValue(&(*tv.orig)[i]))
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	}
}
