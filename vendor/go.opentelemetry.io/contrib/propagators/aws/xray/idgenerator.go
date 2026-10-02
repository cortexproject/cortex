// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package xray

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// IDGenerator is used for generating a new traceID and spanID.
//
// math/rand/v2's top-level generator is used instead of a per-instance
// math/rand.Rand seeded from crypto/rand: the former is safe for concurrent
// use without a mutex and, unlike a locally seeded math/rand.Rand, has no
// seed-read step that can silently fail and leave it deterministic.
type IDGenerator struct {
	// Mutex is embedded for backward compatibility but is not used internally.
	sync.Mutex
}

var _ sdktrace.IDGenerator = &IDGenerator{}

// NewSpanID returns a non-zero span ID from a randomly-chosen sequence.
func (gen *IDGenerator) NewSpanID(context.Context, trace.TraceID) trace.SpanID { //nolint:revive // ignore linter
	sid := trace.SpanID{}
	for {
		binary.NativeEndian.PutUint64(sid[:], rand.Uint64()) //nolint:gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand) is ignored as this is not security-sensitive.
		if sid.IsValid() {
			break
		}
	}
	return sid
}

// NewIDs returns a non-zero trace ID and a non-zero span ID.
// trace ID returned is based on AWS X-Ray TraceID format.
//   - https://docs.aws.amazon.com/xray/latest/devguide/xray-api-sendingdata.html#xray-api-traceids
//
// span ID is from a randomly-chosen sequence.
func (gen *IDGenerator) NewIDs(context.Context) (trace.TraceID, trace.SpanID) { //nolint:revive // ignore linter
	tid := trace.TraceID{}
	currentTime := getCurrentTimeHex()
	copy(tid[:4], currentTime)
	binary.NativeEndian.PutUint64(tid[4:12], rand.Uint64())  //nolint:gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand) is ignored as this is not security-sensitive.
	binary.NativeEndian.PutUint32(tid[12:16], rand.Uint32()) //nolint:gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand) is ignored as this is not security-sensitive.

	sid := trace.SpanID{}
	for {
		binary.NativeEndian.PutUint64(sid[:], rand.Uint64()) //nolint:gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand) is ignored as this is not security-sensitive.
		if sid.IsValid() {
			break
		}
	}
	return tid, sid
}

// NewIDGenerator returns an IDGenerator reference used for sending traces to AWS X-Ray.
func NewIDGenerator() *IDGenerator {
	return &IDGenerator{}
}

func getCurrentTimeHex() []uint8 {
	currentTime := time.Now().Unix()
	// Ignore error since no expected error should result from this operation
	// Odd-length strings and non-hex digits are the only 2 error conditions for hex.DecodeString()
	// strconv.FromatInt() do not produce odd-length strings or non-hex digits
	currentTimeHex, _ := hex.DecodeString(strconv.FormatInt(currentTime, 16))
	return currentTimeHex
}
