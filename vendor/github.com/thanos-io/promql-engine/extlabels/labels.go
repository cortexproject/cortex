// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package extlabels

import (
	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
)

var (
	ErrDuplicateLabelSet = errors.New("vector cannot contain metrics with the same labelset")
)

// DropMetricName removes the __name__ label and returns the dropped name and remaining labels.
func DropMetricName(l labels.Labels, b labels.ScratchBuilder) (labels.Labels, labels.Label) {
	return DropLabel(l, labels.MetricName, b)
}

// DropBucketLabel removes the le label and returns the dropped name and remaining labels.
func DropBucketLabel(l labels.Labels, b labels.ScratchBuilder) (labels.Labels, labels.Label) {
	return DropLabel(l, labels.BucketLabel, b)
}

// DropLabel removes the label with name from l and returns the dropped label.
func DropLabel(l labels.Labels, name string, b labels.ScratchBuilder) (labels.Labels, labels.Label) {
	var ret labels.Label

	if l.IsEmpty() {
		return l, labels.Label{}
	}

	b.Reset()

	l.Range(func(l labels.Label) {
		if l.Name == name {
			ret = l
			return
		}

		b.Add(l.Name, l.Value)
	})

	return b.Labels(), ret
}
