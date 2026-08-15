# GitHub Actions CI/CD

The purpose of this workflow is to run all continuous integration (CI) and continuous deployment (CD) jobs when needed while respecting their internal dependencies. The continuous integration jobs serve to ensure new code passes linting, unit tests and integration tests before reaching the master branch. The continuous deployment jobs serve to deploy the latest version of the code to cortex and the website when merged with master.

## Contributing

If you wish to add a new CI or CD job, add it to the existing current test-build-deploy workflow and make sure it does not prevent any of the other jobs from passing. If you wish to change any of the build or testing images, update it in all sections are containers are often reused. If you wish to add an entirely new workflow, create a new yml file with separate triggers and filters. In all cases, clearly document any changes made to the workflows, images and dependencies below.

## Test, Build and Deploy

test-build-deploy.yml specifies a workflow that runs all Cortex continuous integration and continuous deployment jobs. The workflow is triggered on every pull request and commit to master, however the CD jobs only run when changes are merged onto master . The workflow combines both CI and CD jobs, because the CD jobs are dependent on artifacts produced the CI jobs.


## Specific Jobs

| Job                    | Description                                                                                                                   | Type |
|------------------------|-------------------------------------------------------------------------------------------------------------------------------|------|
| lint                   | Runs linting and ensures vendor directory, protos and generated documentation are consistent.                                 | CI   |
| test                   | Runs units tests on Cassandra testing framework.                                                                              | CI   |
| integration            | Runs integration tests after upgrading golang, pulling necessary docker images and downloading necessary module dependencies. | CI   |
| integration-summary    | Renders one cross-shard summary of the integration matrix into the job summary, so a red run can be triaged without opening every shard's log. | CI   |
| Security/CodeQL        | CodeQL is a semantic code analysis engine used for automating security checks.                                                | CI   |
| build                  | Builds and saves an up-to-date Cortex image and website.                                                                      | CI   |
| deploy_website         | Deploys the latest version of Cortex website to gh-pages branch. Triggered within workflow.                                   | CD   |
| deploy                 | Deploys the latest Cortex image.                                                                                              | CD   |

## Job Dependency Graph

Internal dependencies between jobs illustrated below. Jobs run concurrently where possible but do not start until all jobs they depend on have completed successfully.


![cortex_test-build-deploy](https://user-images.githubusercontent.com/20804975/95492784-9b7feb80-0969-11eb-9934-f44a4b1da498.png)

### Key Details

**Integration Test Output**

The `integration` matrix runs one shard per build tag per architecture, so a failure could
otherwise mean scrolling an undifferentiated wall of text in one of two dozen jobs. Instead the
test binary runs under `bin/test2json` and its event stream is rendered by `bin/gha-testlog`
(built from [`tools/gha-testlog`](../tools/gha-testlog) and shipped in the
`integration-tests-<arch>` artifact, so the job still needs no checkout and no Go toolchain):

- Each top-level test becomes a collapsed `::group::`, holding its own output, its subtests'
  and that of any docker container it started. Note this needs `-test.v=test2json` rather than
  plain `-test.v`, so that `testing` emits the framing markers `test2json` uses to attribute
  container output to the test that produced it.
- An ungrouped `PASS|FAIL|SKIP <Test> (12.34s)` line follows each group, turning the collapsed
  log into a scannable index of results.
- Failures are repeated in a `===== FAILURES =====` section and emitted as `::error::`
  annotations, so they also appear in the run's Annotations panel and on the pull request diff.
- Every shard appends counts and a `<details>` per failure to its own job summary;
  `integration-summary` then renders one table across all shards.

A failing shard uploads its raw `test2json` stream and JSON report as
`integration-logs-<arch>-<tag>` (7 day retention). That stream is the authoritative record:
containers run with `--rm` and their shared directory is deleted when the scenario closes, so
the captured stdout is the only surviving copy of their logs.

**Naming Convention**

Each step in a job has a clear name that encapsulates the purpose of the command. The convention we are using is each word in the name should be capitalized except articles and prepositions. This creates consistent labeling when looking at the progress of the current workflow on GitHub.

```yaml
- name: Checkout Repo
# commands
- name: Get Dependencies
# commands
```

**Symbolic Link to Expected Workspace**

A significant number of commands in the Makefile are hardcoded with an assumed file structure of the CI container. To ensure paths specified in previous commands don’t break, a symlink was created from the hardcoded “expected” working directory `/go/src/github.com/cortexproject/cortex` to the actual working directory `$GITHUB_WORKSPACE`.

```yaml
- name: Sym link expected path to github workspace
  run: |
    mkdir -p /go/src/github.com/cortexproject/cortex
    ln -s $GITHUB_WORKSPACE/* /go/src/github.com/cortexproject/cortex
```

**Sharing Artifacts Between Jobs**

As of October 2020, GitHub Actions do not persist between different jobs in the same workflow. Each job is run on a fresh virtual environment (https://docs.github.com/en/free-pro-team@latest/actions/learn-github-actions/introduction-to-github-actions#runners). As such, we need to upload and download artifacts to share data between jobs.

| Artifact                      | Stored In | Used By                                     | Purpose of Storing Artifact |
|-------------------------------|-----------|---------------------------------------------|-----------------------------|
| website public                | build     | deploy_website                              | share data between jobs     |
| Docker Images                 | build     | deploy, integration                         | share data between jobs     |
| integration-tests-\<arch\>     | build-integration-tests | integration               | share the compiled test binary, its output renderers and its testdata |
| integration-logs-\<arch\>-\<tag\> | integration (on failure) | integration-summary, humans | keep the raw test2json stream and JSON report of a failing shard |

*Note:* Docker Images are zipped before uploading as a workaround. The images contain characters that are illegal in the upload-artifact action.
```yaml
- name: Compressing Images
        run: tar -zcvf images.tar.gz /tmp/images
      - name: Cache Images
        uses: actions/upload-artifact@v4
        with:
          name: Docker Images
          path: ./images.tar.gz
```
**Tags**

As of Oct 2020, GitHub [does not support](https://github.community/t/using-regex-for-filtering/16427/2) regex for tag filtering. The regex /^v[0-9]+(\.[0-9]+){2}(-.+|[^-.]*)$/ was approximated using the available GitHub [filter patterns](https://docs.github.com/en/free-pro-team@latest/actions/reference/workflow-syntax-for-github-actions#filter-pattern-cheat-sheet)
```yaml
tags:
  - v[0-9]+.[0-9]+.[0-9]+**
```