package ingester

import (
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
)

func TestLabelPairsEqual(t *testing.T) {
	for _, test := range []struct {
		name  string
		a     labelPairs
		b     labels.Labels
		equal bool
	}{
		{
			name:  "both blank",
			a:     labelPairs{},
			b:     labels.Labels{},
			equal: true,
		},
		{
			name: "labelPairs nonblank; labels blank",
			a: labelPairs{
				{Name: []byte("foo"), Value: []byte("a")},
			},
			b:     labels.Labels{},
			equal: false,
		},
		{
			name: "labelPairs blank; labels nonblank",
			a:    labelPairs{},
			b: labels.Labels{
				{Name: "foo", Value: "a"},
			},
			equal: false,
		},
		{
			name: "same contents; labelPairs not sorted",
			a: labelPairs{
				{Name: []byte("foo"), Value: []byte("a")},
				{Name: []byte("bar"), Value: []byte("b")},
			},
			b: labels.Labels{
				{Name: "bar", Value: "b"},
				{Name: "foo", Value: "a"},
			},
			equal: true,
		},
		{
			name: "same contents",
			a: labelPairs{
				{Name: []byte("bar"), Value: []byte("b")},
				{Name: []byte("foo"), Value: []byte("a")},
			},
			b: labels.Labels{
				{Name: "bar", Value: "b"},
				{Name: "foo", Value: "a"},
			},
			equal: true,
		},
		{
			name: "same names, different value",
			a: labelPairs{
				{Name: []byte("bar"), Value: []byte("b")},
				{Name: []byte("foo"), Value: []byte("c")},
			},
			b: labels.Labels{
				{Name: "bar", Value: "b"},
				{Name: "foo", Value: "a"},
			},
			equal: false,
		},
		{
			name: "labels has one extra value",
			a: labelPairs{
				{Name: []byte("bar"), Value: []byte("b")},
				{Name: []byte("foo"), Value: []byte("a")},
			},
			b: labels.Labels{
				{Name: "bar", Value: "b"},
				{Name: "foo", Value: "a"},
				{Name: "firble", Value: "c"},
			},
			equal: false,
		},
		{
			name: "labelPairs has one extra value",
			a: labelPairs{
				{Name: []byte("bar"), Value: []byte("b")},
				{Name: []byte("foo"), Value: []byte("a")},
				{Name: []byte("firble"), Value: []byte("c")},
			},
			b: labels.Labels{
				{Name: "bar", Value: "b"},
				{Name: "foo", Value: "a"},
				{Name: "firble", Value: "a"},
			},
			equal: false,
		},
	} {
		if test.a.equal(test.b) != test.equal {
			t.Errorf("%s: expected equal=%t", test.name, test.equal)
		}
	}
}
