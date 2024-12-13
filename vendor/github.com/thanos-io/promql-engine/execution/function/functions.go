// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"math"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql/parser"
)

type functionCall func(f float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool)

var instantVectorFuncs = map[string]functionCall{
	"abs":   simpleFunc(math.Abs),
	"ceil":  simpleFunc(math.Ceil),
	"exp":   simpleFunc(math.Exp),
	"floor": simpleFunc(math.Floor),
	"sqrt":  simpleFunc(math.Sqrt),
	"ln":    simpleFunc(math.Log),
	"log2":  simpleFunc(math.Log2),
	"log10": simpleFunc(math.Log10),
	"sin":   simpleFunc(math.Sin),
	"cos":   simpleFunc(math.Cos),
	"tan":   simpleFunc(math.Tan),
	"asin":  simpleFunc(math.Asin),
	"acos":  simpleFunc(math.Acos),
	"atan":  simpleFunc(math.Atan),
	"sinh":  simpleFunc(math.Sinh),
	"cosh":  simpleFunc(math.Cosh),
	"tanh":  simpleFunc(math.Tanh),
	"asinh": simpleFunc(math.Asinh),
	"acosh": simpleFunc(math.Acosh),
	"atanh": simpleFunc(math.Atanh),
	"rad": simpleFunc(func(v float64) float64 {
		return v * math.Pi / 180
	}),
	"deg": simpleFunc(func(v float64) float64 {
		return v * 180 / math.Pi
	}),
	"sgn": simpleFunc(func(v float64) float64 {
		var sign float64
		if v > 0 {
			sign = 1
		} else if v < 0 {
			sign = -1
		}
		if math.IsNaN(v) {
			sign = math.NaN()
		}
		return sign
	}),
	"round": func(f float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool) {
		if len(vargs) > 1 {
			return 0., false
		}

		toNearest := 1.0
		if len(vargs) > 0 {
			toNearest = vargs[0]
		}
		toNearestInverse := 1.0 / toNearest
		return math.Floor(f*toNearestInverse+0.5) / toNearestInverse, true
	},
	"pi": func(float64, *histogram.FloatHistogram, ...float64) (float64, bool) {
		return math.Pi, true
	},
	"vector": func(f float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool) {
		return f, true
	},
	"clamp": func(f float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool) {
		if len(vargs) != 2 {
			return 0., false
		}

		v := f
		min := vargs[0]
		max := vargs[1]

		if max < min {
			return 0., false
		}

		return math.Max(min, math.Min(max, v)), true
	},
	"clamp_min": func(f float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool) {
		if len(vargs) != 1 {
			return 0., false
		}

		v := f
		min := vargs[0]

		return math.Max(min, v), true
	},
	"clamp_max": func(f float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool) {
		if len(vargs) != 1 {
			return 0., false
		}

		v := f
		max := vargs[0]

		return math.Min(max, v), true
	},
	"histogram_sum": func(f float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool) {
		if h == nil {
			return 0., false
		}
		return h.Sum, true
	},
	"histogram_count": func(f float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool) {
		if h == nil {
			return 0., false
		}
		return h.Count, true
	},
	"histogram_avg": func(f float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool) {
		if h == nil {
			return 0., false
		}
		return h.Sum / h.Count, true
	},
	"histogram_fraction": func(f float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool) {
		if h == nil || len(vargs) != 2 {
			return 0., false
		}
		return histogramFraction(vargs[0], vargs[1], h), true
	},
	// variants of date time functions with an argument
	"days_in_month": func(f float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool) {
		return daysInMonth(dateFromSampleValue(f)), true
	},
	"day_of_month": func(f float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool) {
		return dayOfMonth(dateFromSampleValue(f)), true
	},
	"day_of_week": func(f float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool) {
		return dayOfWeek(dateFromSampleValue(f)), true
	},
	"day_of_year": func(f float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool) {
		return dayOfYear(dateFromSampleValue(f)), true
	},
	"hour": func(f float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool) {
		return hour(dateFromSampleValue(f)), true
	},
	"minute": func(f float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool) {
		return minute(dateFromSampleValue(f)), true
	},
	"month": func(f float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool) {
		return month(dateFromSampleValue(f)), true
	},
	"year": func(f float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool) {
		return year(dateFromSampleValue(f)), true
	},
	// hack we only have sort functions as argument for "timestamp" possibly so they dont actually
	// need to sort anything. This is only for compatibility to prometheus as this sort of query does
	// not make too much sense.
	"sort": simpleFunc(func(v float64) float64 {
		return v
	}),
	"sort_desc": simpleFunc(func(v float64) float64 {
		return v
	}),
	"sort_by_label": simpleFunc(func(v float64) float64 {
		return v
	}),
	"sort_by_label_desc": simpleFunc(func(v float64) float64 {
		return v
	}),
}

type noArgFunctionCall func(t int64) float64

var noArgFuncs = map[string]noArgFunctionCall{
	"pi": func(_ int64) float64 {
		return math.Pi
	},
	"time": func(t int64) float64 {
		return float64(t) / 1000
	},
	// variants of date time functions with no argument
	"days_in_month": func(t int64) float64 {
		return daysInMonth(dateFromStepTime(t))
	},
	"day_of_month": func(t int64) float64 {
		return dayOfMonth(dateFromStepTime(t))
	},
	"day_of_week": func(t int64) float64 {
		return dayOfWeek(dateFromStepTime(t))
	},
	"day_of_year": func(t int64) float64 {
		return dayOfYear(dateFromStepTime(t))
	},
	"hour": func(t int64) float64 {
		return hour(dateFromStepTime(t))
	},
	"minute": func(t int64) float64 {
		return minute(dateFromStepTime(t))
	},
	"month": func(t int64) float64 {
		return month(dateFromStepTime(t))
	},
	"year": func(t int64) float64 {
		return year(dateFromStepTime(t))
	},
}

func simpleFunc(f func(float64) float64) functionCall {
	return func(v float64, h *histogram.FloatHistogram, vargs ...float64) (float64, bool) {
		return f(v), true
	}
}

func dateFromSampleValue(f float64) time.Time {
	return time.Unix(int64(f), 0).UTC()
}

func dateFromStepTime(t int64) time.Time {
	return time.Unix(t/1000, 0).UTC()
}

func daysInMonth(t time.Time) float64 {
	return float64(32 - time.Date(t.Year(), t.Month(), 32, 0, 0, 0, 0, time.UTC).Day())
}

func dayOfMonth(t time.Time) float64 {
	return float64(t.Day())
}

func dayOfWeek(t time.Time) float64 {
	return float64(t.Weekday())
}

func dayOfYear(t time.Time) float64 {
	return float64(t.YearDay())
}

func hour(t time.Time) float64 {
	return float64(t.Hour())
}

func minute(t time.Time) float64 {
	return float64(t.Minute())
}

func month(t time.Time) float64 {
	return float64(t.Month())
}

func year(t time.Time) float64 {
	return float64(t.Year())
}

var XFunctions = map[string]*parser.Function{
	"xdelta": {
		Name:       "xdelta",
		ArgTypes:   []parser.ValueType{parser.ValueTypeMatrix},
		ReturnType: parser.ValueTypeVector,
	},
	"xincrease": {
		Name:       "xincrease",
		ArgTypes:   []parser.ValueType{parser.ValueTypeMatrix},
		ReturnType: parser.ValueTypeVector,
	},
	"xrate": {
		Name:       "xrate",
		ArgTypes:   []parser.ValueType{parser.ValueTypeMatrix},
		ReturnType: parser.ValueTypeVector,
	},
}

// IsExtFunction is a convenience function to determine whether extended range calculations are required.
func IsExtFunction(functionName string) bool {
	_, ok := XFunctions[functionName]
	return ok
}
