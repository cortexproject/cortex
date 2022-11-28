// Copyright The OpenTelemetry Authors
//
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

package xray // import "go.opentelemetry.io/contrib/propagators/aws/xray"

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"math/rand"
	"strconv"
	"sync"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// IDGenerator is used for generating a new traceID and spanID.
type IDGenerator struct {
	sync.Mutex
	randSource *rand.Rand
}

var _ sdktrace.IDGenerator = &IDGenerator{}

// NewSpanID returns a non-zero span ID from a randomly-chosen sequence.
func (gen *IDGenerator) NewSpanID(ctx context.Context, traceID trace.TraceID) trace.SpanID {
	gen.Lock()
	defer gen.Unlock()
	sid := trace.SpanID{}
	_, _ = gen.randSource.Read(sid[:])
	return sid
}

// NewIDs returns a non-zero trace ID and a non-zero span ID.
// trace ID returned is based on AWS X-Ray TraceID format.
//   - https://docs.aws.amazon.com/xray/latest/devguide/xray-api-sendingdata.html#xray-api-traceids
//
// span ID is from a randomly-chosen sequence.
func (gen *IDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	gen.Lock()
	defer gen.Unlock()

	tid := trace.TraceID{}
	currentTime := getCurrentTimeHex()
	copy(tid[:4], currentTime)
	_, _ = gen.randSource.Read(tid[4:])

	sid := trace.SpanID{}
	_, _ = gen.randSource.Read(sid[:])
	return tid, sid
}

// NewIDGenerator returns an IDGenerator reference used for sending traces to AWS X-Ray.
func NewIDGenerator() *IDGenerator {
	gen := &IDGenerator{}
	var rngSeed int64
	_ = binary.Read(crand.Reader, binary.LittleEndian, &rngSeed)
	gen.randSource = rand.New(rand.NewSource(rngSeed))
	return gen
}

func getCurrentTimeHex() []uint8 {
	currentTime := time.Now().Unix()
	// Ignore error since no expected error should result from this operation
	// Odd-length strings and non-hex digits are the only 2 error conditions for hex.DecodeString()
	// strconv.FromatInt() do not produce odd-length strings or non-hex digits
	currentTimeHex, _ := hex.DecodeString(strconv.FormatInt(currentTime, 16))
	return currentTimeHex
}
