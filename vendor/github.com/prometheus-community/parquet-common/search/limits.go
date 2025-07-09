// Copyright The Prometheus Authors
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

// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

// This package is a modified copy from
// https://github.com/thanos-io/thanos-parquet-gateway/blob/cfc1279f605d1c629c4afe8b1e2a340e8b15ecdc/internal/limits/limit.go.

package search

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type resourceExhausted struct {
	used int64
}

func (re *resourceExhausted) Error() string {
	return fmt.Sprintf("resource exhausted (used %d)", re.used)
}

func IsResourceExhausted(err error) bool {
	var re *resourceExhausted
	return errors.As(err, &re)
}

type Quota struct {
	mu sync.Mutex
	q  int64
	u  int64
}

func NewQuota(n int64) *Quota {
	return &Quota{q: n, u: n}
}

func UnlimitedQuota() *Quota {
	return NewQuota(0)
}

func (q *Quota) Reserve(n int64) error {
	if q.q == 0 {
		return nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.u-n < 0 {
		return &resourceExhausted{used: q.q}
	}
	q.u -= n
	return nil
}

type QuotaLimitFunc func(ctx context.Context) int64

func NoopQuotaLimitFunc(ctx context.Context) int64 {
	return 0
}
