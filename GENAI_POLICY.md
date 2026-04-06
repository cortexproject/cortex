# Generative AI Contribution Policy

## Purpose

The Cortex project welcomes contributions that make use of generative AI (GenAI) tools. AI assistants can help contributors write code, explore the codebase, draft documentation, and improve productivity. However, **humans bear full responsibility** for every contribution they submit, regardless of how it was produced.

This policy applies to all repositories under the [cortexproject](https://github.com/cortexproject) GitHub organisation.

## Permitted Use of AI Tools

The following uses of AI tools are encouraged and permitted:

- **Coding assistants** - Using tools like GitHub Copilot, Claude Code, Cursor, or similar to help write, refactor, or debug code.
- **Codebase exploration** - Querying AI tools to understand project architecture, locate relevant code, or learn conventions.
- **Documentation drafting** - Generating initial drafts of documentation, comments, or commit messages.
- **PR review assistance** - Using AI to help review code, identify potential issues, or suggest improvements.
- **Maintainer-configured review bots** - Automated review bots configured by project maintainers.

## Contributor Responsibilities

When using AI tools to assist with contributions, you must:

1. **Understand every line you submit.** You must be able to independently explain any change in your contribution. "The AI wrote it" is not an acceptable justification during review.

2. **Review and validate AI output.** Never submit AI-generated content verbatim without careful review. Verify correctness, check for hallucinated APIs or dependencies, and ensure the output follows Cortex conventions.

3. **Disclose significant AI usage.** If AI generated the bulk of a contribution (e.g., an entire new feature, large refactors, or substantial documentation), note this in the PR description. Minor assistance (autocomplete, small suggestions) does not require disclosure.

4. **Honour the DCO.** Your `Signed-off-by` line on each commit certifies the [Developer Certificate of Origin](https://developercertificate.org/) for **all** content in that commit, including any AI-generated portions. You are attesting that you have the right to submit the work.

5. **Meet the same quality bar.** AI-assisted contributions are held to the same standards as any other contribution: tests, documentation, CHANGELOG entries, passing CI, and adherence to the project's [design patterns and conventions](docs/contributing/design-patterns-and-conventions.md).

## GitHub Communications

- **Issues, pull request reviews, and discussions** must be substantively human-authored. Do not submit bulk AI-generated comments, reviews, or issue reports.
- Sharing AI-generated analyses (e.g., "I asked an AI to summarise the failure modes and here is what it found") is acceptable when clearly attributed and verified by the contributor.
- Do not use AI tools to generate large volumes of low-quality issues or review comments.

## Maintainer Authority

Maintainers may:

- **Request disclosure** of AI tool usage for any contribution.
- **Close or request revision** of PRs or issues that appear to contain unreviewed AI-generated content.
- **Escalate persistent low-effort submissions** through the project's normal [Code of Conduct](https://github.com/cortexproject/cortex/blob/master/CODE_OF_CONDUCT.md) enforcement process.

## Relationship to Other Policies

| Document | Purpose |
|----------|---------|
| [Contributing Guide](CONTRIBUTING.md) | General contribution workflow and requirements |
| [Code of Conduct](code-of-conduct.md) | Community behaviour standards |
| [Governance](GOVERNANCE.md) | Project governance and decision-making |
| [AGENTS.md](AGENTS.md) | Technical guidance **to** AI coding agents working in this repo |

**AGENTS.md vs GENAI_POLICY.md:** `AGENTS.md` provides instructions that AI coding agents consume when working with the codebase (build commands, architecture, conventions). This document (`GENAI_POLICY.md`) governs how **human contributors** use AI tools when preparing their contributions.

## References

- [OpenTelemetry GenAI Contribution Policy](https://github.com/open-telemetry/community/blob/main/policies/genai.md)
- [Linux Foundation AI Guidelines](https://www.linuxfoundation.org/legal/generative-ai)
- [Developer Certificate of Origin](https://developercertificate.org/)
