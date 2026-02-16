package dynamodb

import (
	"testing"
	"time"
)

func BenchmarkWatchLoopWaitWithTimeAfter(b *testing.B) {
	ctx := b.Context()

	const interval = time.Nanosecond
	b.ReportAllocs()

	for b.Loop() {
		select {
		case <-ctx.Done():
			b.Fatal("context canceled unexpectedly")
		case <-time.After(interval):
		}
	}
}

func BenchmarkWatchLoopWaitWithReusableTimer(b *testing.B) {
	ctx := b.Context()

	const interval = time.Nanosecond
	timer := time.NewTimer(interval)
	defer timer.Stop()

	b.ReportAllocs()

	for b.Loop() {
		resetTimer(timer, interval)

		select {
		case <-ctx.Done():
			b.Fatal("context canceled unexpectedly")
		case <-timer.C:
		}
	}
}
