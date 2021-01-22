package util

import "github.com/cortexproject/cortex/pkg/util/logutil"

// WarnExperimentalUse logs a warning and increments the experimental features metric.
func WarnExperimentalUse(feature string) {
	logutil.WarnExperimentalUse(feature)
}
