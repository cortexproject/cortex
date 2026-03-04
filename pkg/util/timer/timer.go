package timer

import "time"

// StopAndDrainTimer stops the timer and drains its channel if a tick was already queued.
func StopAndDrainTimer(timer *time.Timer) {
	if timer == nil {
		return
	}

	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

// ResetTimer safely resets timer, handling the required stop+drain sequence first.
func ResetTimer(timer *time.Timer, d time.Duration) {
	StopAndDrainTimer(timer)
	timer.Reset(d)
}
