# Contributing to Flux

## Community Slack Channel
When contributing to Flux, it is a good idea to join the [community Slack channel](https://www.influxdata.com/slack).
The development team for Flux is in the `#flux` channel and we will be able to answer any questions or give any recommendations for development work from there.

## Filing issues
Filing issues on GitHub is one of the easiest and most useful ways to contribute to Flux.
We value every request and we intend to triage every community issue within a week after it has been created.
If it takes us longer than a week, then please try to contact us in the community Slack channel.

Before you file an issue, please search existing issues in case it has already been filed or perhaps even resolved.

### Bug reports
When submitting a bug report, please include the following:

- Full details of your operating system (or distribution) e.g. 64-bit Ubuntu 14.04.
- The version of Flux you are running.
- Whether you installed it using a pre-built package or built it from source.
- A small test case that demonstrates the issue or steps to reproduce the issue.

Remember the golden rule of bug reports: **The easier you make it for us to reproduce the problem, the faster it will get fixed.
**If you have never written a bug report before, or if you want to brush up on your bug reporting skills, we recommend reading [Simon Tatham's essay "How to Report Bugs Effectively."](http://www.chiark.greenend.org.uk/~sgtatham/bugs.html)

Please note that issues are *not the place to file general questions* such as "how do I use InfluxDB with Flux?"
Questions of this nature should be sent to the [InfluxData Community](https://community.influxdata.com/), not filed as issues.
Issues like this will be closed.

## Feature requests
We really like to receive feature requests as they help us prioritize our work.
Please be clear about your requirements. Incomplete feature requests may simply
be closed if we don't understand what you would like to see added to Flux.

## Contributing to the source code

### Signing the CLA
In order to contribute back to Flux, you must sign the
[InfluxData Contributor License Agreement](https://www.influxdata.com/legal/cla/) (CLA).

### Finding an issue
The Flux team regularly adds the [community](https://github.com/influxdata/flux/issues?q=is%3Aopen+is%3Aissue+label%3Acommunity) label to issues that we think would be accessible for a community member to take.
Before starting to work on an issue, please inform us on GitHub about your intention to work on the issue by leaving a comment on the issue.
This allows us to know that you want to work on the issue, that the issue is updated with the needed information, and to be ready for a pull request when the work is done.

### Contributing to the source code
Flux uses Go modules and requires modules to be enabled to build from source.
Please refer to the Go documentation on [modules](https://golang.org/cmd/go/#hdr-Modules__module_versions__and_more) for more information about how to enable module builds.
To build and test the software, the following developer dependencies are required:

* Go 1.12 or higher
* Rust
* Ragel 7 (only if making changes to the scanner)

Flux follows the standard Go project structure

To run all tests, `make test` can be called from the root directory.

In addition to the `Makefile`, Flux supports building and testing with the standard Go tools and commands.
If you are modifying only Go code and want to only test Go code, the standard commands can be used.

```bash
$ go test ./...
```

If you use `go generate` on the scanner package, then the Ragel version above is needed to correctly generate the files.

### Regenerating the standard library
If you modify any `.flux` files in the repository, the standard library must be regenerated.
This is done by running `go generate` in the following way.

```bash
$ go generate ./stdlib
```

### Contributing Flux Packages

If you have some Flux code that you think the wider community would benifit from please consider contributing it to the Flux repo.
We have layed out specific guildines in the contrib [README](https://github.com/influxdata/flux/blob/master/stdlib/contrib/README.md).
uu

## Pull request guidelines

### Effective Go
The Flux codebase follows the guidelines from [Effective Go](https://golang.org/doc/effective_go.html).
Please familiarize yourself with these guidelines before submitting a pull request.

## Security Vulnerability Reporting
InfluxData takes security and our users' trust very seriously. If you believe you have found a security issue in any of our
open source projects, please responsibly disclose it by contacting security@influxdata.com. More details about 
security vulnerability reporting, 
including our GPG key, [can be found here](https://www.influxdata.com/how-to-report-security-vulnerabilities/).


### Use of third-party packages
A third-party package is defined as one that is not part of the standard Go distribution.
Generally speaking, we prefer to minimize our use of third-party packages and avoid
them unless absolutely necessarily. We'll often write a little bit of code rather
than pull in a third-party package. To maximize the chance your change will be accepted,
use only the standard libraries, or the third-party packages we have decided to use.

For rationale, check out the post [The Case Against Third Party Libraries](http://blog.gopheracademy.com/advent-2014/case-against-3pl/).

### Writing unit tests
Unit tests for Go code should be in the same package as the code itself.
It should be in a file named `filename_test.go` with the package name `mypackage_test` as the name of the package.
The tests should test functionality through public functions and methods.

If access to internal members or functions is needed, then the test file should be named `filename_internal_test.go` and use the same package name as the package itself.
The tests written in this file will have access to the internals of the package.
This file can also be used to create testing-specific functions that can be exposed to the tests in `mypackage_test`.

Pull requests require tests to be written for them and must pass our code coverage checks.

### Public functions and methods
When writing code, try to avoid making functions and methods used by the code public.
When creating a library that needs to be used by multiple packages within the Flux repository, an internal package is preferred.
A public function or method should only be used if using the Flux library would require the function or method to be public.

### Comments and documentation
Every new public function or method requires a valid GoDoc.
Comments within the code should be written with proper capitalization and punctuation at the end of the sentence.
Please refer to [this blog post](https://blog.golang.org/godoc-documenting-go-code) about documenting Go code.

### Updating markdown files for documentation
The Flux team uses markdown for our documentation.
When writing markdown, please use the following guidelines:

- Each sentence in a paragraph is on its own line.
- Use the `#` symbol for header sections.
- Use the `-` symbol for bullet points.

## Submitting a pull request
To submit a pull request you should fork the Flux repository and make your change on a feature branch of your fork.
Then generate a pull request from your branch against **master** of the Flux repository.
Include in your pull request details of your change -- the **why** *and* the **how** -- as well as the testing you performed.
Also, be sure to run the test suite with your change in place.
Changes that cause tests to fail cannot be merged.
The code you write must be unit tested within the same package as the code that you changed.

There will usually be some back and forth as we finalize the change, but once that completes, it may be merged.
When making changes based on the code review, please add new commits and please refrain from amending the original commit.
This helps us keep track of the changes that have been made during the course of the review.

When your change is ready to be merged, the Flux developer who is reviewing your code will approve the pull request and squash your commits into a single commit.
The first commit in your changeset will be used as the final commit message.
This first commit must follow the _conventional commit message_ format: https://www.conventionalcommits.org/en/v1.0.0-beta.3/.
Please use this commit message format for commits that will be visible in influxdata/flux history.

If your pull request only requires trivial changes before being approved and merged, the reviewer may make the change themselves and push to the branch in your fork.
This makes it easier for us to fix a small nitpick such as formatting without requiring an additional back and forth.
If the changes are non-trivial, we will always ask before changing any code.
If you do not want us to do this at all under any circumstance, then you can disable this by unselecting **Allow edits from maintainers** in the pull request itself.

To assist in review for the PR, please add the following to your pull request comment:

```md
- [ ] Sign [CLA](https://www.influxdata.com/legal/cla/) (if not already signed)
```

## Useful links
- [Useful techniques in Go](https://arslan.io/2015/10/08/ten-useful-techniques-in-go/) 
- [Go in production](http://peter.bourgon.org/go-in-production/)
- [Principles of designing Go APIs with channels](https://inconshreveable.com/07-08-2014/principles-of-designing-go-apis-with-channels/)
- [Common mistakes in Golang](http://soryy.com/blog/2014/common-mistakes-with-go-lang/).
  Especially this section `Loops, Closures, and Local Variables`
