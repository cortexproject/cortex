// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ingester

import (
	"testing"

	"github.com/prometheus/common/model"
)

var (
	// cm11, cm12, cm13 are colliding with fp1.
	// cm21, cm22 are colliding with fp2.
	// cm31, cm32 are colliding with fp3, which is below maxMappedFP.
	// Note that fingerprints are set and not actually calculated.
	// The collision detection is independent from the actually used
	// fingerprinting algorithm.
	fp1  = model.Fingerprint(maxMappedFP + 1)
	fp2  = model.Fingerprint(maxMappedFP + 2)
	fp3  = model.Fingerprint(1)
	cm11 = model.Metric{
		"foo":   "bar",
		"dings": "bumms",
	}
	cm12 = model.Metric{
		"bar": "foo",
	}
	cm13 = model.Metric{
		"foo": "bar",
	}
	cm21 = model.Metric{
		"foo":   "bumms",
		"dings": "bar",
	}
	cm22 = model.Metric{
		"dings": "foo",
		"bar":   "bumms",
	}
	cm31 = model.Metric{
		"bumms": "dings",
	}
	cm32 = model.Metric{
		"bumms": "dings",
		"bar":   "foo",
	}
)

func TestFPMapper(t *testing.T) {
	sm := newSeriesMap()

	mapper := newFPMapper(sm)

	// Everything is empty, resolving a FP should do nothing.
	gotFP := mapper.mapFP(fp1, cm11)
	if wantFP := fp1; gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp1, cm12)
	if wantFP := fp1; gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}

	// cm11 is in sm. Adding cm11 should do nothing. Mapping cm12 should resolve
	// the collision.
	sm.put(fp1, &memorySeries{metric: cm11})
	gotFP = mapper.mapFP(fp1, cm11)
	if wantFP := fp1; gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp1, cm12)
	if wantFP := model.Fingerprint(1); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}

	// The mapped cm12 is added to sm, too. That should not change the outcome.
	sm.put(model.Fingerprint(1), &memorySeries{metric: cm12})
	gotFP = mapper.mapFP(fp1, cm11)
	if wantFP := fp1; gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp1, cm12)
	if wantFP := model.Fingerprint(1); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}

	// Now map cm13, should reproducibly result in the next mapped FP.
	gotFP = mapper.mapFP(fp1, cm13)
	if wantFP := model.Fingerprint(2); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp1, cm13)
	if wantFP := model.Fingerprint(2); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}

	// Add cm13 to sm. Should not change anything.
	sm.put(model.Fingerprint(2), &memorySeries{metric: cm13})
	gotFP = mapper.mapFP(fp1, cm11)
	if wantFP := fp1; gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp1, cm12)
	if wantFP := model.Fingerprint(1); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp1, cm13)
	if wantFP := model.Fingerprint(2); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}

	// Now add cm21 and cm22 in the same way, checking the mapped FPs.
	gotFP = mapper.mapFP(fp2, cm21)
	if wantFP := fp2; gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	sm.put(fp2, &memorySeries{metric: cm21})
	gotFP = mapper.mapFP(fp2, cm21)
	if wantFP := fp2; gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp2, cm22)
	if wantFP := model.Fingerprint(3); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	sm.put(model.Fingerprint(3), &memorySeries{metric: cm22})
	gotFP = mapper.mapFP(fp2, cm21)
	if wantFP := fp2; gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp2, cm22)
	if wantFP := model.Fingerprint(3); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}

	// Map cm31, resulting in a mapping straight away.
	gotFP = mapper.mapFP(fp3, cm31)
	if wantFP := model.Fingerprint(4); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	sm.put(model.Fingerprint(4), &memorySeries{metric: cm31})

	// Map cm32, which is now mapped for two reasons...
	gotFP = mapper.mapFP(fp3, cm32)
	if wantFP := model.Fingerprint(5); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	sm.put(model.Fingerprint(5), &memorySeries{metric: cm32})

	// Now check ALL the mappings, just to be sure.
	gotFP = mapper.mapFP(fp1, cm11)
	if wantFP := fp1; gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp1, cm12)
	if wantFP := model.Fingerprint(1); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp1, cm13)
	if wantFP := model.Fingerprint(2); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp2, cm21)
	if wantFP := fp2; gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp2, cm22)
	if wantFP := model.Fingerprint(3); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp3, cm31)
	if wantFP := model.Fingerprint(4); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp3, cm32)
	if wantFP := model.Fingerprint(5); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}

	// Remove all the fingerprints from sm, which should change nothing, as
	// the existing mappings stay and should be detected.
	sm.del(fp1)
	sm.del(fp2)
	sm.del(fp3)
	gotFP = mapper.mapFP(fp1, cm11)
	if wantFP := fp1; gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp1, cm12)
	if wantFP := model.Fingerprint(1); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp1, cm13)
	if wantFP := model.Fingerprint(2); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp2, cm21)
	if wantFP := fp2; gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp2, cm22)
	if wantFP := model.Fingerprint(3); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp3, cm31)
	if wantFP := model.Fingerprint(4); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
	gotFP = mapper.mapFP(fp3, cm32)
	if wantFP := model.Fingerprint(5); gotFP != wantFP {
		t.Errorf("got fingerprint %v, want fingerprint %v", gotFP, wantFP)
	}
}
