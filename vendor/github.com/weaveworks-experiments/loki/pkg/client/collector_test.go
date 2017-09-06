package loki

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/weaveworks-experiments/loki/pkg/model"
)

func TestCollectorSpans(t *testing.T) {
	collector := NewCollector(5)

	// These two should be overwritten
	if err := collector.Collect(&model.Span{}); err != nil {
		t.Fatal(err)
	}
	if err := collector.Collect(&model.Span{}); err != nil {
		t.Fatal(err)
	}

	want := []*model.Span{}
	for i := 1; i < 6; i++ {
		span := &model.Span{
			SpanContext: model.SpanContext{
				TraceId: uint64(i),
			},
			OperationName: fmt.Sprintf("span %d", i),
		}
		want = append(want, span)
		if err := collector.Collect(span); err != nil {
			t.Fatal(err)
		}
	}

	if have := collector.gather(); !reflect.DeepEqual(want, have) {
		t.Fatalf("%s", Diff(want, have))
	}

	if want, have := []*model.Span{}, collector.gather(); !reflect.DeepEqual(want, have) {
		t.Fatalf("%s", Diff(want, have))
	}

	want = []*model.Span{}
	for i := 7; i < 11; i++ {
		span := &model.Span{
			SpanContext: model.SpanContext{
				TraceId: uint64(i),
			},
			OperationName: fmt.Sprintf("span %d", i),
		}
		want = append(want, span)
		if err := collector.Collect(span); err != nil {
			t.Fatal(err)
		}
	}

	if have := collector.gather(); !reflect.DeepEqual(want, have) {
		t.Fatalf("%s", Diff(want, have))
	}

	if want, have := []*model.Span{}, collector.gather(); !reflect.DeepEqual(want, have) {
		t.Fatalf("%s", Diff(want, have))
	}
}

func TestCollectorFuzzSpans(t *testing.T) {
	capacity := 7
	collector := NewCollector(capacity)

	iterate := func(iteration int) {
		toInsert := rand.Intn(capacity + 1)
		want := []*model.Span{}
		for i := 0; i < toInsert; i++ {
			span := &model.Span{
				SpanContext: model.SpanContext{
					TraceId: uint64(i),
				},
				OperationName: fmt.Sprintf("span %d %d", iteration, i),
			}
			want = append(want, span)
			if err := collector.Collect(span); err != nil {
				t.Fatal(err)
			}
		}

		if have := collector.gather(); !reflect.DeepEqual(want, have) {
			t.Fatalf("%s", Diff(want, have))
		}

		if want, have := []*model.Span{}, collector.gather(); !reflect.DeepEqual(want, have) {
			t.Fatalf("%s", Diff(want, have))
		}
	}

	for i := 0; i < 100; i++ {
		iterate(i)
	}
}

// Diff diffs two arbitrary data structures, giving human-readable output.
func Diff(want, have interface{}) string {
	config := spew.NewDefaultConfig()
	config.ContinueOnMethod = true
	config.SortKeys = true
	config.SpewKeys = true
	text, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        difflib.SplitLines(config.Sdump(want)),
		B:        difflib.SplitLines(config.Sdump(have)),
		FromFile: "want",
		ToFile:   "have",
		Context:  3,
	})
	return "\n" + text
}
