package dynamodb

import (
	"context"
	"testing"
	"time"
)

func BenchmarkWatchLoopWaitWithTimeAfter(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const interval = time.Nanosecond
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		select {
		case <-ctx.Done():
			b.Fatal("context canceled unexpectedly")
		case <-time.After(interval):
		}
	}
}

func BenchmarkWatchLoopWaitWithReusableTimer(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const interval = time.Nanosecond
	timer := time.NewTimer(interval)
	defer timer.Stop()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resetTimer(timer, interval)

		select {
		case <-ctx.Done():
			b.Fatal("context canceled unexpectedly")
		case <-timer.C:
		}
	}
}
