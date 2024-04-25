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
| integration-configs-db | Integration tests for database configurations.                                                                                | CI   |
| integration            | Runs integration tests after upgrading golang, pulling necessary docker images and downloading necessary module dependencies. | CI   |
| Security/CodeQL        | CodeQL is a semantic code analysis engine used for automating security checks.                                                | CI   |
| build                  | Builds and saves an up-to-date Cortex image and website.                                                                      | CI   |
| deploy_website         | Deploys the latest version of Cortex website to gh-pages branch. Triggered within workflow.                                   | CD   |
| deploy                 | Deploys the latest Cortex image.                                                                                              | CD   |

## Job Dependency Graph

Internal dependencies between jobs illustrated below. Jobs run concurrently where possible but do not start until all jobs they depend on have completed successfully.


![cortex_test-build-deploy](https://user-images.githubusercontent.com/20804975/95492784-9b7feb80-0969-11eb-9934-f44a4b1da498.png)

### Key Details

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
| Docker Images                 | build     | deploy, integration, integrations-config-db | share data between jobs     |

*Note:* Docker Images are zipped before uploading as a workaround. The images contain characters that are illegal in the upload-artifact action.
```yaml
- name: Compressing Images
        run: tar -zcvf images.tar.gz /tmp/images
      - name: Cache Images
        uses: actions/upload-artifact@v2
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