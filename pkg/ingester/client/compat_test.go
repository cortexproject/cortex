package client

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

func TestQueryRequest(t *testing.T) {
	from, to := model.Time(int64(0)), model.Time(int64(10))
	matchers := []*labels.Matcher{}
	matcher1, err := labels.NewMatcher(labels.MatchEqual, "foo", "1")
	if err != nil {
		t.Fatal(err)
	}
	matchers = append(matchers, matcher1)

	matcher2, err := labels.NewMatcher(labels.MatchNotEqual, "bar", "2")
	if err != nil {
		t.Fatal(err)
	}
	matchers = append(matchers, matcher2)

	matcher3, err := labels.NewMatcher(labels.MatchRegexp, "baz", "3")
	if err != nil {
		t.Fatal(err)
	}
	matchers = append(matchers, matcher3)

	matcher4, err := labels.NewMatcher(labels.MatchNotRegexp, "bop", "4")
	if err != nil {
		t.Fatal(err)
	}
	matchers = append(matchers, matcher4)

	req, err := ToQueryRequest(from, to, matchers)
	if err != nil {
		t.Fatal(err)
	}

	haveFrom, haveTo, haveMatchers, err := FromQueryRequest(req)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(haveFrom, from) {
		t.Fatalf("Bad from FromQueryRequest(ToQueryRequest) round trip")
	}
	if !reflect.DeepEqual(haveTo, to) {
		t.Fatalf("Bad to FromQueryRequest(ToQueryRequest) round trip")
	}
	if !matchersEqual(haveMatchers, matchers) {
		t.Fatalf("Bad have FromQueryRequest(ToQueryRequest) round trip - %v != %v", haveMatchers, matchers)
	}
}

func matchersEqual(expected, actual []*labels.Matcher) bool {
	if len(expected) != len(actual) {
		return false
	}

	for i := 0; i < len(expected); i++ {
		a := actual[i]
		e := expected[i]
		if a.Name != e.Name || a.Value != e.Value || a.Type != e.Type {
			return false
		}
	}

	return true
}

// The main usecase for `LabelsToKeyString` is to generate hashKeys
// for maps. We are benchmarking that here.
func BenchmarkSeriesMap(b *testing.B) {
	benchmarkSeriesMap(100000, b)
}

func benchmarkSeriesMap(numSeries int, b *testing.B) {
	series := makeSeries(numSeries)
	sm := make(map[string]int, numSeries)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i, s := range series {
			sm[LabelsToKeyString(s)] = i
		}

		for _, s := range series {
			_, ok := sm[LabelsToKeyString(s)]
			if !ok {
				b.Fatal("element missing")
			}
		}

		if len(sm) != numSeries {
			b.Fatal("the number of series expected:", numSeries, "got:", len(sm))
		}
	}
}

func makeSeries(n int) []labels.Labels {
	series := make([]labels.Labels, 0, n)
	for i := 0; i < n; i++ {
		series = append(series, labels.FromMap(map[string]string{
			"label0": "value0",
			"label1": "value1",
			"label2": "value2",
			"label3": "value3",
			"label4": "value4",
			"label5": "value5",
			"label6": "value6",
			"label7": "value7",
			"label8": "value8",
			"label9": strconv.Itoa(i),
		}))
	}

	return series
}
