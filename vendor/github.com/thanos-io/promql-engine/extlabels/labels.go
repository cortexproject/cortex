// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package extlabels

import (
	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/schema"
)

var (
	ErrDuplicateLabelSet = errors.New("vector cannot contain metrics with the same labelset")
)

const (
	MetricType = "__type__"
	MetricUnit = "__unit__"
)

// DropReserved removes all reserved labels (__name__, __type__, __unit__) and returns the remaining labels.
func DropReserved(l labels.Labels, b labels.ScratchBuilder) labels.Labels {
	return DropLabels(l, schema.IsMetadataLabel, b)
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

// DropLabels removes labels from l based on the shouldDrop function and returns the remaining labels.
func DropLabels(l labels.Labels, shouldDrop func(name string) bool, b labels.ScratchBuilder) labels.Labels {
	if l.IsEmpty() {
		return l
	}

	b.Reset()

	l.Range(func(lbl labels.Label) {
		if !shouldDrop(lbl.Name) {
			b.Add(lbl.Name, lbl.Value)
		}
	})

	return b.Labels()
}
