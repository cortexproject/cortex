package timeutil

import "time"

const (
	nanosecondsInMillisecond = int64(time.Millisecond / time.Nanosecond)
)

// TimeFromMillis is a helper to turn milliseconds -> time.Time
func TimeFromMillis(ms int64) time.Time {
	return time.Unix(0, ms*nanosecondsInMillisecond)
}

func TimeToMillis(t time.Time) int64 {
	return t.UnixNano() / nanosecondsInMillisecond
}
