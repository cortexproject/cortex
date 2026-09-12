package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The helpers below build the raw test2json JSON lines the renderer consumes.

func evRun(test string) string {
	return fmt.Sprintf(`{"Action":"run","Package":"integration","Test":%q}`, test)
}

func evOut(test, output string) string {
	return fmt.Sprintf(`{"Action":"output","Package":"integration","Test":%q,"Output":%q}`, test, output)
}

func evPkgOut(output string) string {
	return fmt.Sprintf(`{"Action":"output","Package":"integration","Output":%q}`, output)
}

func evResult(action, test string, elapsed float64) string {
	return fmt.Sprintf(`{"Action":%q,"Package":"integration","Test":%q,"Elapsed":%v}`, action, test, elapsed)
}

func jsonl(lines ...string) string {
	return strings.Join(lines, "\n") + "\n"
}

func render(t *testing.T, opts options, input string) string {
	t.Helper()

	if opts.tailLines == 0 {
		opts.tailLines = defaultTailLines
	}
	if opts.maxAnnotations == 0 {
		opts.maxAnnotations = defaultMaxAnnotations
	}

	var buf bytes.Buffer
	r := newRenderer(&buf, opts)
	if err := r.run(strings.NewReader(input)); err != nil {
		t.Fatalf("run: %v", err)
	}
	return buf.String()
}

func countLines(out, line string) int {
	n := 0
	for l := range strings.SplitSeq(out, "\n") {
		if l == line {
			n++
		}
	}
	return n
}

func TestRender(t *testing.T) {
	tests := []struct {
		name        string
		opts        options
		input       string
		want        string
		contains    []string
		notContains []string
	}{
		{
			name: "passing test is grouped and followed by a result line",
			input: jsonl(
				evRun("TestFoo"),
				evOut("TestFoo", "=== RUN   TestFoo\n"),
				evOut("TestFoo", "hello\n"),
				evOut("TestFoo", "--- PASS: TestFoo (1.00s)\n"),
				evResult("pass", "TestFoo", 1),
			),
			want: "::group::TestFoo\n" +
				"=== RUN   TestFoo\n" +
				"hello\n" +
				"--- PASS: TestFoo (1.00s)\n" +
				"::endgroup::\n" +
				"PASS TestFoo (1.00s)\n",
		},
		{
			name: "each top level test gets its own group",
			input: jsonl(
				evRun("TestA"),
				evOut("TestA", "a\n"),
				evResult("pass", "TestA", 1),
				evRun("TestB"),
				evOut("TestB", "b\n"),
				evResult("skip", "TestB", 0),
			),
			want: "::group::TestA\n" +
				"a\n" +
				"::endgroup::\n" +
				"PASS TestA (1.00s)\n" +
				"::group::TestB\n" +
				"b\n" +
				"::endgroup::\n" +
				"SKIP TestB (0.00s)\n",
		},
		{
			name: "subtests share their parent's group",
			input: jsonl(
				evRun("TestFoo"),
				evOut("TestFoo", "=== RUN   TestFoo\n"),
				evRun("TestFoo/Sub"),
				evOut("TestFoo/Sub", "inside sub\n"),
				evResult("pass", "TestFoo/Sub", 0.5),
				evOut("TestFoo", "back in parent\n"),
				evResult("pass", "TestFoo", 1),
			),
			want: "::group::TestFoo\n" +
				"=== RUN   TestFoo\n" +
				"inside sub\n" +
				"back in parent\n" +
				"::endgroup::\n" +
				"PASS TestFoo (1.00s)\n",
		},
		{
			name: "package level output stays outside any group",
			input: jsonl(
				evPkgOut("before any test\n"),
				evRun("TestFoo"),
				evOut("TestFoo", "in test\n"),
				evResult("pass", "TestFoo", 1),
				evPkgOut("PASS\n"),
				evPkgOut("ok  \tintegration\t1.234s\n"),
			),
			want: "before any test\n" +
				"::group::TestFoo\n" +
				"in test\n" +
				"::endgroup::\n" +
				"PASS TestFoo (1.00s)\n" +
				"PASS\n" +
				"ok  \tintegration\t1.234s\n",
		},
		{
			name: "a line split across events is rendered once",
			input: jsonl(
				evRun("TestFoo"),
				evOut("TestFoo", "first half "),
				evOut("TestFoo", "second half\n"),
				evResult("pass", "TestFoo", 1),
			),
			want: "::group::TestFoo\n" +
				"first half second half\n" +
				"::endgroup::\n" +
				"PASS TestFoo (1.00s)\n",
		},
		{
			name: "an unterminated final line is still rendered inside its group",
			input: jsonl(
				evRun("TestFoo"),
				evOut("TestFoo", "no trailing newline"),
				evResult("pass", "TestFoo", 1),
			),
			want: "::group::TestFoo\n" +
				"no trailing newline\n" +
				"::endgroup::\n" +
				"PASS TestFoo (1.00s)\n",
		},
		{
			name: "workflow commands in test output cannot break grouping",
			input: jsonl(
				evRun("TestFoo"),
				evOut("TestFoo", "::endgroup::\n"),
				evOut("TestFoo", "  ::error::from a container\n"),
				evOut("TestFoo", "::group::sneaky\n"),
				evResult("pass", "TestFoo", 1),
			),
			want: "::group::TestFoo\n" +
				" ::endgroup::\n" +
				"   ::error::from a container\n" +
				" ::group::sneaky\n" +
				"::endgroup::\n" +
				"PASS TestFoo (1.00s)\n",
		},
		{
			name: "a truncated trailing line is passed through verbatim",
			input: jsonl(
				evRun("TestFoo"),
				evOut("TestFoo", "in test\n"),
				evResult("pass", "TestFoo", 1),
			) + `{"Action":"output","Package":"integration","Out`,
			want: "::group::TestFoo\n" +
				"in test\n" +
				"::endgroup::\n" +
				"PASS TestFoo (1.00s)\n" +
				`{"Action":"output","Package":"integration","Out` + "\n",
		},
		{
			name: "a stream that ends mid test reports the test as incomplete",
			input: jsonl(
				evRun("TestHang"),
				evOut("TestHang", "started\n"),
			),
			contains: []string{
				"::group::TestHang\n",
				"started\n",
				"::endgroup::\n",
				"FAIL TestHang (incomplete)\n",
				"===== FAILURES (1) =====",
				"--- FAIL TestHang (incomplete: the test never reported a result)",
				"::error title=TestHang::TestHang never reported a result",
			},
		},
		{
			name: "the assertion is blamed, not an earlier t.Logf",
			input: jsonl(
				evRun("TestFoo"),
				evOut("TestFoo", "    querier_test.go:10: Testing: up == 1\n"),
				evOut("TestFoo", "    querier_test.go:20: Query returned 3 series\n"),
				evOut("TestFoo", "    querier_test.go:31: \n"),
				evOut("TestFoo", "        \tError Trace:\t/go/src/cortex/integration/querier_test.go:31\n"),
				evOut("TestFoo", "        \tError:      \tReceived unexpected error:\n"),
				evOut("TestFoo", "        \t            \tcontext deadline exceeded\n"),
				evResult("fail", "TestFoo", 1),
			),
			contains: []string{
				"::error file=integration/querier_test.go,line=31,title=TestFoo::Error:      \tReceived unexpected error:%0Acontext deadline exceeded\n",
			},
			notContains: []string{
				"line=10",
				"line=20",
			},
		},
		{
			name: "testify's Error Trace frames do not override the assertion's own line",
			input: jsonl(
				evRun("TestFoo"),
				evOut("TestFoo", "    ruler_test.go:495: \n"),
				evOut("TestFoo", "        \tError Trace:\t/go/src/cortex/integration/ruler_test.go:495\n"),
				evOut("TestFoo", "        \t            \t\t\t\t/go/src/cortex/integration/ruler_test.go:586\n"),
				evOut("TestFoo", "        \tError:      \tNot equal\n"),
				evResult("fail", "TestFoo", 1),
			),
			contains: []string{
				"    at integration/ruler_test.go:495\n",
				"::error file=integration/ruler_test.go,line=495,title=TestFoo::Error:      \tNot equal\n",
			},
			notContains: []string{
				// 586 is the caller of the assertion helper, not the assertion, and the
				// trace frames themselves are noise in a tooltip.
				"line=586",
				"ruler_test.go:586%0A",
			},
		},
		{
			name: "a failure is annotated with its source location and message",
			opts: options{shard: "amd64 / integration_ruler"},
			input: jsonl(
				evRun("TestFoo"),
				evOut("TestFoo", "    ruler_test.go:123: \n"),
				evOut("TestFoo", "        \tError Trace:\t/go/src/ruler_test.go:123\n"),
				evOut("TestFoo", "        \tError:      \tNot equal: 1 != 2\n"),
				evOut("TestFoo", "--- FAIL: TestFoo (1.00s)\n"),
				evResult("fail", "TestFoo", 1),
			),
			contains: []string{
				"FAIL TestFoo (1.00s)\n",
				"    at integration/ruler_test.go:123\n",
				// The message stops before the stack trace, which is noise in a tooltip.
				"::error file=integration/ruler_test.go,line=123,title=amd64 / integration_ruler / TestFoo::Error:      \tNot equal: 1 != 2\n",
			},
		},
		{
			name: "annotations name the failing subtest rather than its parent",
			input: jsonl(
				evRun("TestFoo"),
				evRun("TestFoo/Sub"),
				evOut("TestFoo/Sub", "    foo_test.go:7: boom\n"),
				evResult("fail", "TestFoo/Sub", 0.5),
				evResult("fail", "TestFoo", 1),
			),
			contains: []string{
				"    failed subtest: TestFoo/Sub\n",
				"::error file=integration/foo_test.go,line=7,title=TestFoo/Sub::boom\n",
			},
			notContains: []string{
				"title=TestFoo::",
			},
		},
		{
			name: "annotations are capped with an overflow warning",
			opts: options{maxAnnotations: 2},
			input: jsonl(
				evRun("TestA"), evResult("fail", "TestA", 1),
				evRun("TestB"), evResult("fail", "TestB", 1),
				evRun("TestC"), evResult("fail", "TestC", 1),
			),
			contains: []string{
				"===== FAILURES (3) =====",
				"::error title=TestA::TestA failed.\n",
				"::error title=TestB::TestB failed.\n",
				"::warning::1 further failure(s) not annotated; see the FAILURES section above.\n",
			},
			notContains: []string{
				"::error title=TestC::",
			},
		},
		{
			name: "the failure section repeats only the last -tail lines",
			opts: options{tailLines: 2},
			input: jsonl(
				evRun("TestFoo"),
				evOut("TestFoo", "line1\n"),
				evOut("TestFoo", "line2\n"),
				evOut("TestFoo", "line3\n"),
				evOut("TestFoo", "line4\n"),
				evResult("fail", "TestFoo", 1),
			),
			contains: []string{
				"    --- last 2 line(s) of output ---\nline3\nline4\n",
			},
			notContains: []string{
				// line1 and line2 appear once in the group but must not be repeated.
				"line1\nline2\nline3\nline4\n    --- last",
			},
		},
		{
			name: "a passing run emits no failure section and no annotations",
			input: jsonl(
				evRun("TestFoo"),
				evResult("pass", "TestFoo", 1),
			),
			notContains: []string{"FAILURES", "::error", "::warning"},
		},
		{
			name: "percent signs and newlines in a message are escaped",
			input: jsonl(
				evRun("TestFoo"),
				evOut("TestFoo", "    foo_test.go:1: 100% wrong\n"),
				evOut("TestFoo", "        second line\n"),
				evResult("fail", "TestFoo", 1),
			),
			contains: []string{
				"::error file=integration/foo_test.go,line=1,title=TestFoo::100%25 wrong%0Asecond line\n",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := render(t, tc.opts, tc.input)

			if tc.want != "" && got != tc.want {
				t.Errorf("unexpected output\n--- got ---\n%s\n--- want ---\n%s", got, tc.want)
			}
			for _, want := range tc.contains {
				if !strings.Contains(got, want) {
					t.Errorf("output does not contain %q\n--- got ---\n%s", want, got)
				}
			}
			for _, unwanted := range tc.notContains {
				if strings.Contains(got, unwanted) {
					t.Errorf("output unexpectedly contains %q\n--- got ---\n%s", unwanted, got)
				}
			}
			if open, closed := strings.Count(got, "\n::group::")+boolToInt(strings.HasPrefix(got, "::group::")), countLines(got, "::endgroup::"); open != closed {
				t.Errorf("unbalanced groups: %d ::group:: vs %d ::endgroup::\n--- got ---\n%s", open, closed, got)
			}
		})
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func TestReportAndStepSummary(t *testing.T) {
	dir := t.TempDir()
	summaryPath := filepath.Join(dir, "summary.md")
	reportPath := filepath.Join(dir, "report.json")

	input := jsonl(
		evRun("TestPasses"),
		evResult("pass", "TestPasses", 1),
		evRun("TestSkips"),
		evResult("skip", "TestSkips", 0),
		evRun("TestFails"),
		evRun("TestFails/Sub"),
		evOut("TestFails/Sub", "    ruler_test.go:42: boom\n"),
		evResult("fail", "TestFails/Sub", 0.5),
		evResult("fail", "TestFails", 1),
	)

	opts := options{
		tailLines:      defaultTailLines,
		maxAnnotations: defaultMaxAnnotations,
		shard:          "amd64 / integration_ruler",
		arch:           "amd64",
		tags:           "integration_ruler",
		reportPath:     reportPath,
		summaryPath:    summaryPath,
	}
	render(t, opts, input)

	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("reading report: %v", err)
	}
	var rep report
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatalf("unmarshalling report: %v", err)
	}

	if want := (counts{Total: 3, Passed: 1, Failed: 1, Skipped: 1}); rep.Tests != want {
		t.Errorf("counts = %+v, want %+v", rep.Tests, want)
	}
	if rep.Arch != "amd64" || rep.Tags != "integration_ruler" || rep.Shard != "amd64 / integration_ruler" {
		t.Errorf("unexpected shard identity: %+v", rep)
	}
	if len(rep.Failures) != 1 {
		t.Fatalf("failures = %+v, want 1 entry", rep.Failures)
	}
	got := rep.Failures[0]
	want := reportFailure{Test: "TestFails/Sub", TopLevel: "TestFails", File: "integration/ruler_test.go", Line: 42, Message: "boom"}
	if got != want {
		t.Errorf("failure = %+v, want %+v", got, want)
	}

	summary, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("reading step summary: %v", err)
	}
	for _, want := range []string{
		"### Integration tests — amd64 / integration_ruler",
		"3 test(s): 1 passed, 1 failed, 1 skipped",
		"<details><summary>❌ TestFails — integration/ruler_test.go:42</summary>",
		"- `TestFails/Sub`",
	} {
		if !strings.Contains(string(summary), want) {
			t.Errorf("step summary does not contain %q\n--- got ---\n%s", want, summary)
		}
	}
}

func TestStepSummaryIsSkippedWithoutAPath(t *testing.T) {
	// Nothing to write to, so nothing is written: this is how the tool behaves outside
	// GitHub Actions, and how it must behave so the unit tests above cannot append to a
	// real $GITHUB_STEP_SUMMARY.
	render(t, options{}, jsonl(evRun("TestFoo"), evResult("pass", "TestFoo", 1)))
}

func TestStepSummaryIsCapped(t *testing.T) {
	summaryPath := filepath.Join(t.TempDir(), "summary.md")

	// Two failures, each with more output than the whole summary is allowed to hold.
	var lines []string
	for i := range 2 {
		test := fmt.Sprintf("TestFail%d", i)
		lines = append(lines, evRun(test))
		for range 40 {
			lines = append(lines, evOut(test, strings.Repeat("x", 20*1024)+"\n"))
		}
		lines = append(lines, evResult("fail", test, 1))
	}
	render(t, options{tailLines: 40, summaryPath: summaryPath}, jsonl(lines...))

	summary, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("reading step summary: %v", err)
	}
	if len(summary) > maxStepSummaryBytes {
		t.Errorf("step summary is %d bytes, want at most %d", len(summary), maxStepSummaryBytes)
	}
	if !strings.Contains(string(summary), "more failure(s) omitted") {
		t.Errorf("capped step summary does not say what was omitted:\n%s", summary[:min(len(summary), 2000)])
	}
}
