package main

import (
	"math"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
)

// Auditor is a struct for auditing prometheus queries
type Auditor struct{}

// Diff stores a difference between two queries
type Diff struct {
	Series      int
	Diff        float64   // avg proportional diff across all series & samples
	sampleDiffs []float64 // proportional diffs as measured by x/control
}

// Audit audits two prometheus queries
func (a *Auditor) Audit(control, x model.Value) (Diff, error) {
	if x.Type() == model.ValMatrix && control.Type() == model.ValMatrix {
		return a.auditMatrix(x.(model.Matrix), control.(model.Matrix))
	}

	if x.Type() == model.ValVector && control.Type() == model.ValVector {
		return a.auditVector(x.(model.Vector), control.(model.Vector))
	}

	return Diff{}, errors.Errorf("unsupported types for equality: got %s & %s", control.Type().String(), x.Type().String())
}

func (a *Auditor) auditMatrix(x, y model.Matrix) (diff Diff, err error) {
	// different # of returned series
	if len(x) != len(y) {
		return diff, errors.Errorf("different # of series: control=%d, other=%d", len(x), len(y))
	}

	for i := 0; i < len(x); i++ {
		xSeries, ySeries := x[i], y[i]
		if !xSeries.Metric.Equal(ySeries.Metric) {
			return diff, errors.Errorf("mismatched metrics: %v vs %v", xSeries.Metric, ySeries.Metric)
		}

		xVals, yVals := xSeries.Values, ySeries.Values
		if len(xVals) != len(yVals) {
			return diff, errors.Errorf(
				"mismatched number of samples for series %v. control=%d, other=%d",
				xSeries.Metric,
				len(xVals),
				len(yVals),
			)
		}

		for j := 0; j < len(xVals); j++ {
			xSample, ySample := xVals[j], yVals[j]

			if xSample.Timestamp != ySample.Timestamp {
				return diff, errors.Errorf(
					"mismatched timestamp for %d sample of series %v. control=%d, other=%d",
					j,
					xSeries.Metric,
					xSample.Timestamp,
					ySample.Timestamp,
				)
			}

			absDiff := math.Abs(float64(ySample.Value-xSample.Value)) / math.Abs(float64(xSample.Value))

			// 0/0 -> no diff
			if math.IsNaN(absDiff) {
				absDiff = 0
			}

			diff.sampleDiffs = append(diff.sampleDiffs, absDiff)

		}
	}

	diff.Series = len(x)
	var avgDiffProportion float64
	for _, d := range diff.sampleDiffs {
		avgDiffProportion += d
	}
	diff.Diff = avgDiffProportion / float64(len(diff.sampleDiffs))

	return diff, nil
}

func (a *Auditor) auditVector(x, y model.Vector) (Diff, error) {
	return Diff{}, errors.New("unimplemented")
}
