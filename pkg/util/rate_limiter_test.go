// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"testing"
	"time"
)

func assertCheckCredit(t *testing.T, limiter RateLimiter, itemCost float64, expected bool, expectedWait time.Duration) {
	t.Helper()

	r, wt := limiter.CheckCredit(itemCost)
	if r != expected {
		t.Errorf("expected: %v, got: %v", expected, r)
	}
	if wt != expectedWait {
		t.Errorf("expected: %v, got: %v", expectedWait, wt)
	}
}

func TestRateLimiter(t *testing.T) {
	limiter := NewRateLimiter(2.0, 2.0)
	// stop time
	ts := time.Now()
	limiter.(*rateLimiter).lastTick = ts
	limiter.(*rateLimiter).timeNow = func() time.Time {
		return ts
	}

	assertCheckCredit(t, limiter, 1.0, true, 0)
	assertCheckCredit(t, limiter, 1.0, true, 0)
	assertCheckCredit(t, limiter, 1.0, false, 500*time.Millisecond)

	// move time 250ms forward, not enough credits to pay for 1.0 item
	limiter.(*rateLimiter).timeNow = func() time.Time {
		return ts.Add(time.Second / 4)
	}

	assertCheckCredit(t, limiter, 1.0, false, 250*time.Millisecond)
	// move time 500ms forward, now enough credits to pay for 1.0 item
	limiter.(*rateLimiter).timeNow = func() time.Time {
		return ts.Add(time.Second / 2)
	}
	assertCheckCredit(t, limiter, 1.0, true, 0)
	assertCheckCredit(t, limiter, 1.0, false, 500*time.Millisecond)

	// move time 5s forward, enough to accumulate credits for 10 messages, but it should still be capped at 2
	limiter.(*rateLimiter).lastTick = ts
	limiter.(*rateLimiter).timeNow = func() time.Time {
		return ts.Add(5 * time.Second)
	}
	assertCheckCredit(t, limiter, 1.0, true, 0)
	assertCheckCredit(t, limiter, 1.0, true, 0)
	assertCheckCredit(t, limiter, 1.0, false, 500*time.Millisecond)
	assertCheckCredit(t, limiter, 1.0, false, 500*time.Millisecond)
	assertCheckCredit(t, limiter, 1.0, false, 500*time.Millisecond)
}

func TestMaxBalance(t *testing.T) {
	limiter := NewRateLimiter(0.1, 1.0)
	// stop time
	ts := time.Now()
	limiter.(*rateLimiter).lastTick = ts
	limiter.(*rateLimiter).timeNow = func() time.Time {
		return ts
	}
	// on initialization, should have enough credits for 1 message
	assertCheckCredit(t, limiter, 1.0, true, 0)

	// move time 20s forward, enough to accumulate credits for 2 messages, but it should still be capped at 1
	limiter.(*rateLimiter).timeNow = func() time.Time {
		return ts.Add(time.Second * 20)
	}
	assertCheckCredit(t, limiter, 1.0, true, 0)
	assertCheckCredit(t, limiter, 1.0, false, 10*time.Second)
}
