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
	"go.uber.org/multierr"

	"go.opentelemetry.io/collector/pdata/internal"
	otlpcommon "go.opentelemetry.io/collector/pdata/internal/data/protogen/common/v1"
)

// Map stores a map of string keys to elements of Value type.
type Map internal.Map

// NewMap creates a Map with 0 elements.
func NewMap() Map {
	orig := []otlpcommon.KeyValue(nil)
<<<<<<< HEAD
	state := internal.StateMutable
	return Map(internal.NewMap(&orig, &state))
=======
	return Map(internal.NewMap(&orig))
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

func (m Map) getOrig() *[]otlpcommon.KeyValue {
	return internal.GetOrigMap(internal.Map(m))
}

<<<<<<< HEAD
func (m Map) getState() *internal.State {
	return internal.GetMapState(internal.Map(m))
}

func newMap(orig *[]otlpcommon.KeyValue, state *internal.State) Map {
	return Map(internal.NewMap(orig, state))
=======
func newMap(orig *[]otlpcommon.KeyValue) Map {
	return Map(internal.NewMap(orig))
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

// Clear erases any existing entries in this Map instance.
func (m Map) Clear() {
<<<<<<< HEAD
	m.getState().AssertMutable()
=======
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	*m.getOrig() = nil
}

// EnsureCapacity increases the capacity of this Map instance, if necessary,
// to ensure that it can hold at least the number of elements specified by the capacity argument.
func (m Map) EnsureCapacity(capacity int) {
<<<<<<< HEAD
	m.getState().AssertMutable()
	oldOrig := *m.getOrig()
	if capacity <= cap(oldOrig) {
		return
	}
	*m.getOrig() = make([]otlpcommon.KeyValue, len(oldOrig), capacity)
=======
	if capacity <= cap(*m.getOrig()) {
		return
	}
	oldOrig := *m.getOrig()
	*m.getOrig() = make([]otlpcommon.KeyValue, 0, capacity)
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	copy(*m.getOrig(), oldOrig)
}

// Get returns the Value associated with the key and true. Returned
// Value is not a copy, it is a reference to the value stored in this map.
// It is allowed to modify the returned value using Value.Set* functions.
// Such modification will be applied to the value stored in this map.
//
// If the key does not exist returns an invalid instance of the KeyValue and false.
// Calling any functions on the returned invalid instance will cause a panic.
func (m Map) Get(key string) (Value, bool) {
	for i := range *m.getOrig() {
		akv := &(*m.getOrig())[i]
		if akv.Key == key {
<<<<<<< HEAD
			return newValue(&akv.Value, m.getState()), true
		}
	}
	return newValue(nil, m.getState()), false
=======
			return newValue(&akv.Value), true
		}
	}
	return newValue(nil), false
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

// Remove removes the entry associated with the key and returns true if the key
// was present in the map, otherwise returns false.
func (m Map) Remove(key string) bool {
<<<<<<< HEAD
	m.getState().AssertMutable()
=======
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	for i := range *m.getOrig() {
		akv := &(*m.getOrig())[i]
		if akv.Key == key {
			*akv = (*m.getOrig())[len(*m.getOrig())-1]
			*m.getOrig() = (*m.getOrig())[:len(*m.getOrig())-1]
			return true
		}
	}
	return false
}

// RemoveIf removes the entries for which the function in question returns true
func (m Map) RemoveIf(f func(string, Value) bool) {
<<<<<<< HEAD
	m.getState().AssertMutable()
	newLen := 0
	for i := 0; i < len(*m.getOrig()); i++ {
		akv := &(*m.getOrig())[i]
		if f(akv.Key, newValue(&akv.Value, m.getState())) {
=======
	newLen := 0
	for i := 0; i < len(*m.getOrig()); i++ {
		akv := &(*m.getOrig())[i]
		if f(akv.Key, newValue(&akv.Value)) {
>>>>>>> 90dc0587b (Initial OTLP ingest support)
			continue
		}
		if newLen == i {
			// Nothing to move, element is at the right place.
			newLen++
			continue
		}
		(*m.getOrig())[newLen] = (*m.getOrig())[i]
		newLen++
	}
	*m.getOrig() = (*m.getOrig())[:newLen]
}

// PutEmpty inserts or updates an empty value to the map under given key
// and return the updated/inserted value.
func (m Map) PutEmpty(k string) Value {
<<<<<<< HEAD
	m.getState().AssertMutable()
	if av, existing := m.Get(k); existing {
		av.getOrig().Value = nil
		return newValue(av.getOrig(), m.getState())
	}
	*m.getOrig() = append(*m.getOrig(), otlpcommon.KeyValue{Key: k})
	return newValue(&(*m.getOrig())[len(*m.getOrig())-1].Value, m.getState())
=======
	if av, existing := m.Get(k); existing {
		av.getOrig().Value = nil
		return newValue(av.getOrig())
	}
	*m.getOrig() = append(*m.getOrig(), otlpcommon.KeyValue{Key: k})
	return newValue(&(*m.getOrig())[len(*m.getOrig())-1].Value)
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

// PutStr performs the Insert or Update action. The Value is
// inserted to the map that did not originally have the key. The key/value is
// updated to the map where the key already existed.
func (m Map) PutStr(k string, v string) {
<<<<<<< HEAD
	m.getState().AssertMutable()
=======
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	if av, existing := m.Get(k); existing {
		av.SetStr(v)
	} else {
		*m.getOrig() = append(*m.getOrig(), newKeyValueString(k, v))
	}
}

// PutInt performs the Insert or Update action. The int Value is
// inserted to the map that did not originally have the key. The key/value is
// updated to the map where the key already existed.
func (m Map) PutInt(k string, v int64) {
<<<<<<< HEAD
	m.getState().AssertMutable()
=======
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	if av, existing := m.Get(k); existing {
		av.SetInt(v)
	} else {
		*m.getOrig() = append(*m.getOrig(), newKeyValueInt(k, v))
	}
}

// PutDouble performs the Insert or Update action. The double Value is
// inserted to the map that did not originally have the key. The key/value is
// updated to the map where the key already existed.
func (m Map) PutDouble(k string, v float64) {
<<<<<<< HEAD
	m.getState().AssertMutable()
=======
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	if av, existing := m.Get(k); existing {
		av.SetDouble(v)
	} else {
		*m.getOrig() = append(*m.getOrig(), newKeyValueDouble(k, v))
	}
}

// PutBool performs the Insert or Update action. The bool Value is
// inserted to the map that did not originally have the key. The key/value is
// updated to the map where the key already existed.
func (m Map) PutBool(k string, v bool) {
<<<<<<< HEAD
	m.getState().AssertMutable()
=======
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	if av, existing := m.Get(k); existing {
		av.SetBool(v)
	} else {
		*m.getOrig() = append(*m.getOrig(), newKeyValueBool(k, v))
	}
}

// PutEmptyBytes inserts or updates an empty byte slice under given key and returns it.
func (m Map) PutEmptyBytes(k string) ByteSlice {
<<<<<<< HEAD
	m.getState().AssertMutable()
=======
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	bv := otlpcommon.AnyValue_BytesValue{}
	if av, existing := m.Get(k); existing {
		av.getOrig().Value = &bv
	} else {
		*m.getOrig() = append(*m.getOrig(), otlpcommon.KeyValue{Key: k, Value: otlpcommon.AnyValue{Value: &bv}})
	}
<<<<<<< HEAD
	return ByteSlice(internal.NewByteSlice(&bv.BytesValue, m.getState()))
=======
	return ByteSlice(internal.NewByteSlice(&bv.BytesValue))
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

// PutEmptyMap inserts or updates an empty map under given key and returns it.
func (m Map) PutEmptyMap(k string) Map {
<<<<<<< HEAD
	m.getState().AssertMutable()
=======
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	kvl := otlpcommon.AnyValue_KvlistValue{KvlistValue: &otlpcommon.KeyValueList{Values: []otlpcommon.KeyValue(nil)}}
	if av, existing := m.Get(k); existing {
		av.getOrig().Value = &kvl
	} else {
		*m.getOrig() = append(*m.getOrig(), otlpcommon.KeyValue{Key: k, Value: otlpcommon.AnyValue{Value: &kvl}})
	}
<<<<<<< HEAD
	return Map(internal.NewMap(&kvl.KvlistValue.Values, m.getState()))
=======
	return Map(internal.NewMap(&kvl.KvlistValue.Values))
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

// PutEmptySlice inserts or updates an empty slice under given key and returns it.
func (m Map) PutEmptySlice(k string) Slice {
<<<<<<< HEAD
	m.getState().AssertMutable()
=======
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	vl := otlpcommon.AnyValue_ArrayValue{ArrayValue: &otlpcommon.ArrayValue{Values: []otlpcommon.AnyValue(nil)}}
	if av, existing := m.Get(k); existing {
		av.getOrig().Value = &vl
	} else {
		*m.getOrig() = append(*m.getOrig(), otlpcommon.KeyValue{Key: k, Value: otlpcommon.AnyValue{Value: &vl}})
	}
<<<<<<< HEAD
	return Slice(internal.NewSlice(&vl.ArrayValue.Values, m.getState()))
=======
	return Slice(internal.NewSlice(&vl.ArrayValue.Values))
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

// Len returns the length of this map.
//
// Because the Map is represented internally by a slice of pointers, and the data are comping from the wire,
// it is possible that when iterating using "Range" to get access to fewer elements because nil elements are skipped.
func (m Map) Len() int {
	return len(*m.getOrig())
}

// Range calls f sequentially for each key and value present in the map. If f returns false, range stops the iteration.
//
// Example:
//
//	sm.Range(func(k string, v Value) bool {
//	    ...
//	})
func (m Map) Range(f func(k string, v Value) bool) {
	for i := range *m.getOrig() {
		kv := &(*m.getOrig())[i]
<<<<<<< HEAD
		if !f(kv.Key, Value(internal.NewValue(&kv.Value, m.getState()))) {
=======
		if !f(kv.Key, Value(internal.NewValue(&kv.Value))) {
>>>>>>> 90dc0587b (Initial OTLP ingest support)
			break
		}
	}
}

// CopyTo copies all elements from the current map overriding the destination.
func (m Map) CopyTo(dest Map) {
<<<<<<< HEAD
	dest.getState().AssertMutable()
=======
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	newLen := len(*m.getOrig())
	oldCap := cap(*dest.getOrig())
	if newLen <= oldCap {
		// New slice fits in existing slice, no need to reallocate.
		*dest.getOrig() = (*dest.getOrig())[:newLen:oldCap]
		for i := range *m.getOrig() {
			akv := &(*m.getOrig())[i]
			destAkv := &(*dest.getOrig())[i]
			destAkv.Key = akv.Key
<<<<<<< HEAD
			newValue(&akv.Value, m.getState()).CopyTo(newValue(&destAkv.Value, dest.getState()))
=======
			newValue(&akv.Value).CopyTo(newValue(&destAkv.Value))
>>>>>>> 90dc0587b (Initial OTLP ingest support)
		}
		return
	}

	// New slice is bigger than exist slice. Allocate new space.
	origs := make([]otlpcommon.KeyValue, len(*m.getOrig()))
	for i := range *m.getOrig() {
		akv := &(*m.getOrig())[i]
		origs[i].Key = akv.Key
<<<<<<< HEAD
		newValue(&akv.Value, m.getState()).CopyTo(newValue(&origs[i].Value, dest.getState()))
=======
		newValue(&akv.Value).CopyTo(newValue(&origs[i].Value))
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	}
	*dest.getOrig() = origs
}

// AsRaw returns a standard go map representation of this Map.
func (m Map) AsRaw() map[string]any {
	rawMap := make(map[string]any)
	m.Range(func(k string, v Value) bool {
		rawMap[k] = v.AsRaw()
		return true
	})
	return rawMap
}

// FromRaw overrides this Map instance from a standard go map.
func (m Map) FromRaw(rawMap map[string]any) error {
<<<<<<< HEAD
	m.getState().AssertMutable()
=======
>>>>>>> 90dc0587b (Initial OTLP ingest support)
	if len(rawMap) == 0 {
		*m.getOrig() = nil
		return nil
	}

	var errs error
	origs := make([]otlpcommon.KeyValue, len(rawMap))
	ix := 0
	for k, iv := range rawMap {
		origs[ix].Key = k
<<<<<<< HEAD
		errs = multierr.Append(errs, newValue(&origs[ix].Value, m.getState()).FromRaw(iv))
=======
		errs = multierr.Append(errs, newValue(&origs[ix].Value).FromRaw(iv))
>>>>>>> 90dc0587b (Initial OTLP ingest support)
		ix++
	}
	*m.getOrig() = origs
	return errs
}
