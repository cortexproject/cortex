// gha-testlog turns a `go tool test2json` event stream into readable GitHub Actions output.
//
// It reads the JSON lines emitted by test2json on stdin and writes to stdout:
//
//   - one collapsible ::group:: per top-level test, holding that test's output, its
//     subtests' output and the output of any docker container it started;
//   - an ungrouped "PASS|FAIL|SKIP <Test> (12.34s)" line after each group, so the
//     collapsed log reads as a scannable index of results;
//   - a "===== FAILURES =====" section repeating the tail of every failing test;
//   - one ::error:: annotation per failure, so failures show up in the run's
//     Annotations panel and on the pull request diff.
//
// It also appends a markdown report to $GITHUB_STEP_SUMMARY and, with -report, writes a
// small JSON summary for cross-shard aggregation.
//
// The exit status is 0 unless rendering itself failed; the caller is expected to
// propagate the test binary's own status (see .github/workflows/test-build-deploy.yml).
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
)

const (
	// Number of trailing output lines repeated for each failing test.
	defaultTailLines = 60

	// GitHub only displays 10 annotations per step, so emitting more is wasted work.
	defaultMaxAnnotations = 10

	// $GITHUB_STEP_SUMMARY is rejected above 1 MiB; stay well clear of the limit.
	maxStepSummaryBytes = 900 * 1024

	// An annotation message is a tooltip, not a log: keep it short.
	maxMessageLines = 4
	maxMessageChars = 800
)

// testFileRef matches the "foo_test.go:123:" prefix `testing` puts in front of everything a
// test logs. It is deliberately anchored to the start of the line: the absolute paths inside
// testify's "Error Trace:" block would otherwise win, and its frames run outermost-last, so
// the blame would land on the caller of an assertion helper rather than on the assertion.
var testFileRef = regexp.MustCompile(`^\s*([A-Za-z0-9_.-]+_test\.go):(\d+):`)

// traceFrame matches a bare "path/to/file.go:123" stack frame, which is what the
// continuation lines of testify's "Error Trace:" block look like once trimmed.
var traceFrame = regexp.MustCompile(`^\S+\.go:\d+$`)

// event is the subset of the test2json event schema we care about. Package-level events
// carry an empty Test.
type event struct {
	Action  string  `json:"Action"`
	Test    string  `json:"Test"`
	Output  string  `json:"Output"`
	Elapsed float64 `json:"Elapsed"`
}

// testResult accumulates state for one test name. Entries exist for subtests too, keyed
// by their full "Parent/Sub" name; tail and subs are only populated for top-level tests.
type testResult struct {
	name       string
	status     string // PASS, FAIL or SKIP; empty until the result event arrives.
	elapsed    float64
	incomplete bool // No result event ever arrived (panic, timeout, killed process).

	tail []string // Ring buffer of the last N output lines, dropped once the test passes.
	subs []string // Failing subtests, in the order they failed.

	file      string   // First "foo_test.go" seen in this test's own output.
	line      int      // ...and the line number next to it.
	message   []string // First error message seen, for the annotation.
	capturing bool     // Still collecting message lines.
}

type options struct {
	tailLines      int
	maxAnnotations int
	shard          string
	arch           string
	tags           string
	reportPath     string
	summaryPath    string // $GITHUB_STEP_SUMMARY; nothing is written when empty.
}

type renderer struct {
	out  io.Writer
	opts options

	order []string               // Top-level test names, in first-seen order.
	tests map[string]*testResult // Keyed by full test name.

	openGroup  string // Top-level test whose ::group:: is currently open.
	partial    strings.Builder
	partialKey string // Test the buffered partial line belongs to.
}

func main() {
	var opts options
	flag.IntVar(&opts.tailLines, "tail", defaultTailLines, "Number of trailing output lines to repeat for each failing test.")
	flag.IntVar(&opts.maxAnnotations, "max-annotations", defaultMaxAnnotations, "Maximum number of ::error:: annotations to emit.")
	flag.StringVar(&opts.shard, "shard", "", "Human readable shard name, used in annotation titles and the job summary.")
	flag.StringVar(&opts.arch, "arch", "", "Architecture this shard ran on, recorded in the JSON report.")
	flag.StringVar(&opts.tags, "tags", "", "Build tag this shard ran, recorded in the JSON report.")
	flag.StringVar(&opts.reportPath, "report", "", "Write a JSON summary to this path, for cross-shard aggregation.")
	flag.Parse()

	opts.summaryPath = os.Getenv("GITHUB_STEP_SUMMARY")

	r := newRenderer(os.Stdout, opts)
	if err := r.run(os.Stdin); err != nil {
		fmt.Fprintf(os.Stderr, "gha-testlog: %v\n", err)
		os.Exit(1)
	}
}

func newRenderer(out io.Writer, opts options) *renderer {
	return &renderer{
		out:   out,
		opts:  opts,
		tests: map[string]*testResult{},
	}
}

// run consumes a test2json stream and renders it. It returns an error only when rendering
// itself failed; whatever was already rendered is still written out.
func (r *renderer) run(in io.Reader) error {
	sc := bufio.NewScanner(in)
	// test2json splits output events at 1 KiB, but the JSON envelope of a long line can
	// still exceed bufio's 64 KiB default.
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	for sc.Scan() {
		var ev event
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil || ev.Action == "" {
			// Not a test2json event — most likely a final line truncated by a killed
			// process. Pass it through rather than dropping the evidence.
			r.printLine(sc.Text())
			continue
		}
		r.handle(ev)
	}

	return errors.Join(sc.Err(), r.finalize())
}

func (r *renderer) handle(ev event) {
	switch ev.Action {
	case "run":
		if ev.Test != "" {
			r.ensureGroup(topLevel(ev.Test))
		}
	case "output":
		r.output(ev.Test, ev.Output)
	case "pass", "fail", "skip":
		r.finish(ev.Test, strings.ToUpper(ev.Action), ev.Elapsed)
	}
}

// output renders an output event, which is not necessarily a whole line: test2json splits
// long lines across events, so partial lines are buffered until their newline arrives.
func (r *renderer) output(test, s string) {
	if test != r.partialKey {
		// Flush first, so the buffered tail of the previous line is still rendered
		// inside the group it belongs to.
		r.flushPartial()
		r.partialKey = test
	}
	if test != "" {
		r.ensureGroup(topLevel(test))
	}

	for {
		line, rest, found := strings.Cut(s, "\n")
		if !found {
			r.partial.WriteString(s)
			return
		}
		r.partial.WriteString(line)
		complete := r.partial.String()
		r.partial.Reset()
		r.emitLine(test, complete)
		s = rest
	}
}

func (r *renderer) finish(test, status string, elapsed float64) {
	if test == "" {
		// Package-level result; the per-test result lines already say everything.
		return
	}

	t := r.entry(test)
	t.status = status
	t.elapsed = elapsed

	top := topLevel(test)
	if test != top {
		if status == "FAIL" {
			parent := r.entry(top)
			parent.subs = append(parent.subs, test)
		}
		return
	}

	r.flushPartial()
	r.closeGroup()
	if status != "FAIL" {
		t.tail = nil // Only failures need their output repeated.
	}
	r.printf("%s %s (%.2fs)\n", status, top, elapsed)
}

// finalize closes any dangling group, accounts for tests that never reported a result,
// and writes the failure section, annotations, job summary and JSON report.
func (r *renderer) finalize() error {
	r.flushPartial()
	r.closeGroup()

	for _, name := range r.order {
		t := r.tests[name]
		if t.status != "" {
			continue
		}
		// The stream ended mid-test: a panic, Go's own -test.timeout, or a killed
		// process. Report it rather than letting it vanish.
		t.status = "FAIL"
		t.incomplete = true
		r.printf("FAIL %s (incomplete)\n", name)
	}

	failed := r.failures()
	r.printFailures(failed)
	r.printAnnotations(failed)

	return errors.Join(r.writeStepSummary(), r.writeReport())
}

func (r *renderer) ensureGroup(top string) {
	if r.openGroup == top {
		return
	}
	r.closeGroup()
	r.entry(top)
	r.printf("::group::%s\n", top)
	r.openGroup = top
}

func (r *renderer) closeGroup() {
	if r.openGroup == "" {
		return
	}
	r.flushPartial()
	r.printf("::endgroup::\n")
	r.openGroup = ""
}

func (r *renderer) flushPartial() {
	if r.partial.Len() == 0 {
		return
	}
	line := r.partial.String()
	r.partial.Reset()
	r.emitLine(r.partialKey, line)
}

func (r *renderer) emitLine(test, line string) {
	if test != "" {
		r.entry(test).recordDiagnostics(line)
		if top := topLevel(test); r.opts.tailLines > 0 {
			t := r.entry(top)
			t.tail = append(t.tail, line)
			if len(t.tail) > r.opts.tailLines {
				t.tail = t.tail[1:]
			}
		}
	}
	r.printLine(line)
}

// printLine writes one line of test output. A line that looks like a workflow command is
// indented by one space first: a Cortex container logging "::endgroup::" would otherwise
// silently destroy the grouping of the whole log.
func (r *renderer) printLine(line string) {
	if strings.HasPrefix(strings.TrimSpace(line), "::") {
		line = " " + line
	}
	r.printf("%s\n", line)
}

func (r *renderer) printf(format string, args ...any) {
	// Nothing useful can be done if stdout is broken, and the caller's exit status is
	// driven by the test binary, not by us.
	_, _ = fmt.Fprintf(r.out, format, args...)
}

func (r *renderer) entry(name string) *testResult {
	if t, ok := r.tests[name]; ok {
		return t
	}
	t := &testResult{name: name}
	r.tests[name] = t
	if name == topLevel(name) {
		r.order = append(r.order, name)
	}
	return t
}

// failures returns the failing top-level tests, in the order they ran.
func (r *renderer) failures() []*testResult {
	var out []*testResult
	for _, name := range r.order {
		if t := r.tests[name]; t.status == "FAIL" {
			out = append(out, t)
		}
	}
	return out
}

func (r *renderer) printFailures(failed []*testResult) {
	if len(failed) == 0 {
		return
	}

	r.printf("\n===== FAILURES (%d) =====\n", len(failed))
	for _, t := range failed {
		if t.incomplete {
			r.printf("\n--- FAIL %s (incomplete: the test never reported a result)\n", t.name)
		} else {
			r.printf("\n--- FAIL %s (%.2fs)\n", t.name, t.elapsed)
		}
		for _, sub := range t.subs {
			r.printf("    failed subtest: %s\n", sub)
		}
		if ref := r.sourceRef(t); ref != "" {
			r.printf("    at %s\n", ref)
		}
		if len(t.tail) > 0 {
			r.printf("    --- last %d line(s) of output ---\n", len(t.tail))
			for _, line := range t.tail {
				r.printLine(line)
			}
		}
	}
}

// printAnnotations emits one ::error:: per failure, anchored on the most specific failing
// name so a failing subtest is named instead of just its parent.
func (r *renderer) printAnnotations(failed []*testResult) {
	targets := r.annotationTargets(failed)

	shown := targets
	if len(shown) > r.opts.maxAnnotations {
		shown = shown[:r.opts.maxAnnotations]
	}

	for _, t := range shown {
		title := t.name
		if r.opts.shard != "" {
			title = r.opts.shard + " / " + t.name
		}

		props := "title=" + escapeProperty(title)
		if file, line := r.location(t); file != "" {
			props = fmt.Sprintf("file=%s,line=%d,%s", escapeProperty(file), line, props)
		}
		r.printf("::error %s::%s\n", props, escapeData(r.annotationMessage(t)))
	}

	if n := len(targets) - len(shown); n > 0 {
		r.printf("::warning::%d further failure(s) not annotated; see the FAILURES section above.\n", n)
	}
}

func (r *renderer) annotationTargets(failed []*testResult) []*testResult {
	var out []*testResult
	for _, t := range failed {
		if len(t.subs) == 0 {
			out = append(out, t)
			continue
		}
		for _, sub := range t.subs {
			out = append(out, r.entry(sub))
		}
	}
	return out
}

// diagnostics returns the tests whose output may hold a source reference or error message
// for t, most specific first: t itself, then its failing subtests (a parent reports no
// detail of its own), then its parent (a shared helper is attributed to the parent).
func (r *renderer) diagnostics(t *testResult) []*testResult {
	out := []*testResult{t}
	for _, sub := range t.subs {
		if s, ok := r.tests[sub]; ok {
			out = append(out, s)
		}
	}
	if parent, ok := r.tests[topLevel(t.name)]; ok && parent != t {
		out = append(out, parent)
	}
	return out
}

func (r *renderer) location(t *testResult) (string, int) {
	for _, c := range r.diagnostics(t) {
		if c.file != "" {
			return path.Join("integration", c.file), c.line
		}
	}
	return "", 0
}

func (r *renderer) sourceRef(t *testResult) string {
	file, line := r.location(t)
	if file == "" {
		return ""
	}
	return fmt.Sprintf("%s:%d", file, line)
}

func (r *renderer) annotationMessage(t *testResult) string {
	if t.incomplete {
		return t.name + " never reported a result (panic, timeout or killed process)."
	}

	for _, c := range r.diagnostics(t) {
		if len(c.message) == 0 {
			continue
		}
		msg := strings.Join(c.message, "\n")
		if len(msg) > maxMessageChars {
			// ToValidUTF8 drops the partial rune the cut may have left behind.
			msg = strings.ToValidUTF8(msg[:maxMessageChars], "") + "…"
		}
		return msg
	}
	return t.name + " failed."
}

// recordDiagnostics keeps the source reference and error message the annotation and the job
// summary point at.
func (t *testResult) recordDiagnostics(line string) {
	if loc := testFileRef.FindStringSubmatchIndex(line); loc != nil {
		// The *last* reference wins, not the first. Integration tests log with t.Logf as
		// they go, and each of those lines carries a "foo_test.go:NN:" prefix too, so the
		// first reference would usually blame a log line rather than the assertion.
		t.file = line[loc[2]:loc[3]]
		t.line, _ = strconv.Atoi(line[loc[4]:loc[5]])
		t.message = nil
		t.capturing = true
		// Whatever follows "foo_test.go:123:" on the same line starts the message.
		if rest := strings.TrimSpace(line[loc[1]:]); rest != "" {
			t.message = append(t.message, rest)
		}
		return
	}

	if !t.capturing {
		return
	}
	trimmed := strings.TrimSpace(line)
	switch {
	case trimmed == "" || strings.HasPrefix(trimmed, "Error Trace:") || traceFrame.MatchString(trimmed):
		// Blank padding and stack frames add nothing to a one-line annotation.
		return
	case strings.HasPrefix(trimmed, "---") || strings.HasPrefix(trimmed, "==="),
		trimmed == "Diff:", // testify's rendered diff repeats expected/actual.
		len(t.message) >= maxMessageLines:
		t.capturing = false
		return
	}
	t.message = append(t.message, trimmed)
}

func topLevel(name string) string {
	top, _, _ := strings.Cut(name, "/")
	return top
}

// escapeData escapes a workflow command's message payload.
func escapeData(s string) string {
	s = strings.ReplaceAll(s, "%", "%25")
	s = strings.ReplaceAll(s, "\r", "%0D")
	return strings.ReplaceAll(s, "\n", "%0A")
}

// escapeProperty escapes a workflow command property value, where "," and ":" are
// structural.
func escapeProperty(s string) string {
	s = escapeData(s)
	s = strings.ReplaceAll(s, ":", "%3A")
	return strings.ReplaceAll(s, ",", "%2C")
}
