package timer

import (
	"testing"
	"time"
)

func TestStopAndDrainTimer_NilTimer(t *testing.T) {
	// Should not panic on nil timer.
	StopAndDrainTimer(nil)
}

func TestStopAndDrainTimer_UnfiredTimer(t *testing.T) {
	timer := time.NewTimer(time.Hour)
	StopAndDrainTimer(timer)

	// Channel should be empty after stop+drain.
	select {
	case <-timer.C:
		t.Fatal("expected timer channel to be drained")
	default:
	}
}

func TestStopAndDrainTimer_FiredTimer(t *testing.T) {
	timer := time.NewTimer(time.Nanosecond)
	// Wait for it to fire.
	time.Sleep(time.Millisecond)

	StopAndDrainTimer(timer)

	// Channel should be empty after stop+drain.
	select {
	case <-timer.C:
		t.Fatal("expected timer channel to be drained")
	default:
	}
}

func TestResetTimer(t *testing.T) {
	timer := time.NewTimer(time.Hour)

	// Reset to a very short duration.
	ResetTimer(timer, time.Nanosecond)

	select {
	case <-timer.C:
		// Expected.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected timer to fire after reset")
	}
}

func TestResetTimer_AfterFired(t *testing.T) {
	timer := time.NewTimer(time.Nanosecond)
	// Wait for it to fire.
	time.Sleep(time.Millisecond)
	<-timer.C

	// Reset after consuming the fired event.
	ResetTimer(timer, time.Nanosecond)

	select {
	case <-timer.C:
		// Expected.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected timer to fire after reset")
	}
}

func TestResetTimer_MultipleTimes(t *testing.T) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	for i := range 10 {
		ResetTimer(timer, time.Nanosecond)

		select {
		case <-timer.C:
			// Expected.
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("iteration %d: expected timer to fire after reset", i)
		}
	}
}
