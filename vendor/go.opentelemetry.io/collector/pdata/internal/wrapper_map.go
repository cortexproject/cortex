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

type Map struct {
<<<<<<< HEAD
	orig  *[]otlpcommon.KeyValue
	state *State
=======
	orig *[]otlpcommon.KeyValue
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

func GetOrigMap(ms Map) *[]otlpcommon.KeyValue {
	return ms.orig
}

<<<<<<< HEAD
func GetMapState(ms Map) *State {
	return ms.state
}

func NewMap(orig *[]otlpcommon.KeyValue, state *State) Map {
	return Map{orig: orig, state: state}
=======
func NewMap(orig *[]otlpcommon.KeyValue) Map {
	return Map{orig: orig}
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

func GenerateTestMap() Map {
	var orig []otlpcommon.KeyValue
<<<<<<< HEAD
	state := StateMutable
	ms := NewMap(&orig, &state)
=======
	ms := NewMap(&orig)
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	FillTestMap(ms)
	return ms
}

func FillTestMap(dest Map) {
	*dest.orig = nil
	*dest.orig = append(*dest.orig, otlpcommon.KeyValue{Key: "k", Value: otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "v"}}})
}
