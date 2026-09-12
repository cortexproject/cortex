package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// counts are over top-level tests, matching the PASS/FAIL/SKIP result lines in the log.
type counts struct {
	Total   int `json:"total"`
	Passed  int `json:"passed"`
	Failed  int `json:"failed"`
	Skipped int `json:"skipped"`
}

type reportFailure struct {
	Test       string `json:"test"`
	TopLevel   string `json:"top_level"`
	File       string `json:"file,omitempty"`
	Line       int    `json:"line,omitempty"`
	Message    string `json:"message,omitempty"`
	Incomplete bool   `json:"incomplete,omitempty"`
}

// report is consumed by .github/workflows/scripts/summarize-integration-tests.sh to build
// a single table across all shards.
type report struct {
	Shard    string          `json:"shard"`
	Arch     string          `json:"arch"`
	Tags     string          `json:"tags"`
	Tests    counts          `json:"tests"`
	Failures []reportFailure `json:"failures"`
}

func (r *renderer) counts() counts {
	var c counts
	for _, name := range r.order {
		c.Total++
		switch r.tests[name].status {
		case "PASS":
			c.Passed++
		case "FAIL":
			c.Failed++
		case "SKIP":
			c.Skipped++
		}
	}
	return c
}

func (r *renderer) report() report {
	failed := r.failures()

	rep := report{
		Shard:    r.opts.shard,
		Arch:     r.opts.arch,
		Tags:     r.opts.tags,
		Tests:    r.counts(),
		Failures: []reportFailure{},
	}
	for _, t := range r.annotationTargets(failed) {
		file, line := r.location(t)
		rep.Failures = append(rep.Failures, reportFailure{
			Test:       t.name,
			TopLevel:   topLevel(t.name),
			File:       file,
			Line:       line,
			Message:    r.annotationMessage(t),
			Incomplete: t.incomplete,
		})
	}
	return rep
}

func (r *renderer) writeReport() error {
	if r.opts.reportPath == "" {
		return nil
	}
	b, err := json.MarshalIndent(r.report(), "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling report: %w", err)
	}
	if err := os.WriteFile(r.opts.reportPath, append(b, '\n'), 0o644); err != nil {
		return fmt.Errorf("writing report: %w", err)
	}
	return nil
}

func (r *renderer) writeStepSummary() error {
	if r.opts.summaryPath == "" {
		return nil
	}

	f, err := os.OpenFile(r.opts.summaryPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("opening step summary: %w", err)
	}
	_, writeErr := f.WriteString(r.buildStepSummary())
	if writeErr != nil {
		writeErr = fmt.Errorf("writing step summary: %w", writeErr)
	}
	return errors.Join(writeErr, f.Close())
}

func (r *renderer) buildStepSummary() string {
	c := r.counts()

	var b strings.Builder
	title := "Integration tests"
	if r.opts.shard != "" {
		title += " — " + r.opts.shard
	}
	fmt.Fprintf(&b, "### %s\n\n", title)
	fmt.Fprintf(&b, "%d test(s): %d passed, %d failed, %d skipped\n", c.Total, c.Passed, c.Failed, c.Skipped)

	failed := r.failures()
	if len(failed) == 0 {
		return b.String() + "\n"
	}

	b.WriteString("\n")
	for i, t := range failed {
		block := r.summaryFailureBlock(t)
		if b.Len()+len(block) > maxStepSummaryBytes {
			fmt.Fprintf(&b, "_%d more failure(s) omitted; see the job log._\n", len(failed)-i)
			break
		}
		b.WriteString(block)
	}
	return b.String()
}

func (r *renderer) summaryFailureBlock(t *testResult) string {
	var b strings.Builder

	heading := t.name
	if ref := r.sourceRef(t); ref != "" {
		heading += " — " + ref
	}
	if t.incomplete {
		heading += " (never reported a result)"
	}
	fmt.Fprintf(&b, "<details><summary>❌ %s</summary>\n\n", heading)

	if len(t.subs) > 0 {
		b.WriteString("Failed subtests:\n\n")
		for _, sub := range t.subs {
			fmt.Fprintf(&b, "- `%s`\n", sub)
		}
		b.WriteString("\n")
	}

	if len(t.tail) > 0 {
		// Four backticks so a fenced block inside the test output cannot break out.
		b.WriteString("````\n")
		for _, line := range t.tail {
			b.WriteString(line)
			b.WriteString("\n")
		}
		b.WriteString("````\n")
	}

	b.WriteString("\n</details>\n\n")
	return b.String()
}
