package util

import "errors"

// ErrStopCortex is the error returned by a service as a hint to stop the Cortex server entirely.
var ErrStopCortex = errors.New("stop cortex")
