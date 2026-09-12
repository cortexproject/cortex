#!/usr/bin/env bash
# Renders a single cross-shard summary of the integration test matrix.
#
# Input is a directory of integration-report-<arch>-<tag>.json files, written by
# tools/gha-testlog and uploaded by the `integration` job. Only failing shards upload a
# report, so a shard missing from the summary passed.
#
# The summary is written to stdout *and*, when set, appended to $GITHUB_STEP_SUMMARY. It goes
# to stdout as well so that this job's log explains its own red X: without it the log reads
# only "Process completed with exit code 1".
#
# INTEGRATION_RESULT must carry the `integration` matrix job's aggregate result; the script
# exits non-zero unless it is "success", so this job's status mirrors the matrix.

set -euo pipefail

REPORTS_DIR="${1:-.}"
RESULT="${INTEGRATION_RESULT:-unknown}"

OUT="$(mktemp)"
flush() {
  cat "$OUT"
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    cat "$OUT" >>"$GITHUB_STEP_SUMMARY"
  fi
  rm -f "$OUT"
}
# An EXIT trap so every early `exit` below still renders what it built. The trap preserves
# the script's exit status.
trap flush EXIT

emit() {
  printf '%s\n' "${1-}" >>"$OUT"
}

REPORTS=()
if [ -d "$REPORTS_DIR" ]; then
  while IFS= read -r report; do
    REPORTS+=("$report")
  done < <(find "$REPORTS_DIR" -maxdepth 1 -name 'integration-report-*.json' | sort)
fi

emit "## Integration tests"
emit ""

if [ "${#REPORTS[@]}" -eq 0 ]; then
  if [ "$RESULT" = "success" ]; then
    emit "All integration test shards passed."
    exit 0
  fi
  emit "The \`integration\` job result was \`${RESULT}\`, but no shard uploaded a report."
  emit ""
  emit "That means a shard died before its output could be rendered — a runner failure, a"
  emit "cancelled run, or a process killed outside the test binary. Open the red"
  emit "\`integration\` jobs directly."
  exit 1
fi

emit "The \`integration\` job result was \`${RESULT}\`. A shard missing from this table either"
emit "passed, or failed before it could report — check for red \`integration\` jobs not listed here."
emit ""
emit "| Shard | Tests | Failed |"
emit "|---|---:|---:|"

VALID=()
for report in "${REPORTS[@]}"; do
  if ! jq -e . "$report" >/dev/null 2>&1; then
    emit "| \`$(basename "$report")\` (unreadable) | — | — |"
    continue
  fi
  VALID+=("$report")
  # Both columns count top-level tests, so they are directly comparable; the individual
  # failing subtests are listed in the <details> block below.
  jq -r '"| \(.shard) | \(.tests.total) | \(.tests.failed) |"' "$report" >>"$OUT"
done

emit ""

for report in "${VALID[@]+"${VALID[@]}"}"; do
  jq -r '
    if (.failures | length) == 0 then
      "<details><summary>⚠️ \(.shard) — no test failures recorded</summary>",
      "",
      "The shard failed outside the tests (setup, docker, or a killed process). See its job log.",
      "",
      "</details>",
      ""
    else
      "<details><summary>❌ \(.shard) — \(.tests.failed) of \(.tests.total) test(s) failed</summary>",
      "",
      (.failures[] |
        ("- `\(.test)`"
          + (if (.file // "") != "" then " — `\(.file):\(.line)`" else "" end)
          + (if .incomplete then " _(never reported a result)_" else "" end)),
        ((.message // "") | select(. != "") | "  > " + gsub("\n"; "\n  > "))),
      "",
      "</details>",
      ""
    end
  ' "$report" >>"$OUT"
done

if [ "$RESULT" != "success" ]; then
  exit 1
fi
