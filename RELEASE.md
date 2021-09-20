# Releases

This page describes the release process and the currently planned schedule for upcoming releases as well as the respective release shepherd. Release shepherds are chosen on a voluntary basis.

## Release schedule

Our goal is to provide a new minor release every 6 weeks. This is a new process and everything in this document is subject to change.

| release series | date of first pre-release (year-month-day) | release shepherd                            |
|----------------|--------------------------------------------|---------------------------------------------|
| v0.1.0         | 2019-07-31                                 | Chris Marchbanks (GitHub: @csmarchbanks)    |
| v0.2.0         | 2019-08-28                                 | Goutham Veeramachaneni (Github: @gouthamve) |
| v0.3.0         | 2019-10-09                                 | Bryan Boreham (@bboreham)                   |
| v0.4.0         | 2019-11-13                                 | Tom Wilkie (@tomwilkie)                     |
| v0.5.0         | 2020-01-08                                 | _Abandoned_                                 |
| v0.6.0         | 2020-01-22                                 | Marco Pracucci (@pracucci)                  |
| v0.7.0         | 2020-03-09                                 | Marco Pracucci (@pracucci)                  |
| v1.0.0         | 2020-03-31                                 | Goutham Veeramachaneni (@gouthamve)         |
| v1.1.0         | 2020-05-11                                 | Peter Štibraný (@pstibrany)                 |
| v1.2.0         | 2020-06-24                                 | Bryan Boreham                               |
| v1.3.0         | 2020-08-03                                 | Marco Pracucci (@pracucci)                  |
| v1.4.0         | 2020-09-14                                 | Marco Pracucci (@pracucci)                  |
| v1.5.0         | 2020-10-26                                 | Chris Marchbanks (@csmarchbanks)            |
| v1.6.0         | 2020-12-07                                 | Jacob Lisi (@jtlisi)                        |
| v1.7.0         | 2021-01-18                                 | Ken Haines (@khaines)                       |
| v1.8.0         | 2021-03-08                                 | Peter Štibraný (@pstibrany)                 |
| v1.9.0         | 2021-04-29                                 | Goutham Veeramachaneni (@gouthamve)         |
| v1.10.0        | 2021-06-25                                 | Bryan Boreham (@bboreham)                   |
| v1.11.0        | 2021-08-06                                 | Marco Pracucci (@pracucci)                  |
| v1.12.0        | 2021-09-17                                 |                                             |

## Release shepherd responsibilities

The release shepherd is responsible for the entire release series of a minor release, meaning all pre- and patch releases of a minor release. The process formally starts with the initial pre-release, but some preparations should be done a few days in advance.

* We aim to keep the master branch in a working state at all times. In principle, it should be possible to cut a release from master at any time. In practice, things might not work out as nicely. A few days before the pre-release is scheduled, the shepherd should check the state of master. Following their best judgement, the shepherd should try to expedite bug fixes that are still in progress but should make it into the release. On the other hand, the shepherd may hold back merging last-minute invasive and risky changes that are better suited for the next minor release.
* On the date listed in the table above, the release shepherd cuts the first pre-release (using the suffix `-rc.0`) and creates a new branch called  `release-<major>.<minor>` starting at the commit tagged for the pre-release. In general, a pre-release is considered a release candidate (that's what `rc` stands for) and should therefore not contain any known bugs that are planned to be fixed in the final release.
* With the pre-release, the release shepherd is responsible for coordinating or running the release candidate in any [end user](https://github.com/cortexproject/cortex/blob/master/ADOPTERS.md) production environment  for 3 days. This is typically done in Grafana Labs or Weaveworks but we are looking for more volunteers!
* If regressions or critical bugs are detected, they need to get fixed before cutting a new pre-release (called `-rc.1`, `-rc.2`, etc.).

See the next section for details on cutting an individual release.

## How to cut an individual release

### Branch management and versioning strategy

We use [Semantic Versioning](https://semver.org/).

We maintain a separate branch for each minor release, named `release-<major>.<minor>`, e.g. `release-1.1`, `release-2.0`.

The usual flow is to merge new features and changes into the master branch and to merge bug fixes into the latest release branch. Bug fixes are then merged into master from the latest release branch. The master branch should always contain all commits from the latest release branch. As long as master hasn't deviated from the release branch, new commits can also go to master, followed by merging master back into the release branch.

If a bug fix got accidentally merged into master after non-bug-fix changes in master, the bug-fix commits have to be cherry-picked into the release branch, which then have to be merged back into master. Try to avoid that situation.

Maintaining the release branches for older minor releases happens on a best effort basis.

### Show that a release is in progress

This helps ongoing PRs to get their changes in the right place, and to consider whether they need cherry-picked.

1. Make a PR to update `CHANGELOG.md` on master
   - Add a new section for the new release so that "## master / unreleased" is blank and at the top.
   - New section should say "## x.y.0 in progress".
2. Get this PR reviewed and merged.
3. Comment on open PRs with a CHANGELOG entry to rebase `master` and move the CHANGELOG entry to the top under `## master / unreleased`

### Prepare your release

For a new major or minor release, create the corresponding release branch based on the master branch. For a patch release, work in the branch of the minor release you want to patch.

To prepare release branch, first create new release branch (release-X.Y) in Cortex repository from master commit of your choice, and then do the following steps on a private branch (prepare-release-X.Y) and send PR to merge this private branch to the new release branch (prepare-release-X.Y -> release-X.Y).

0. Make sure you've a GPG key associated to your GitHub account (`git tag` will be signed with that GPG key)
   - You can add a GPG key to your GitHub account following [this procedure](https://help.github.com/articles/generating-a-gpg-key/)
1. Update the version number in the `VERSION` file to say "X.Y-rc.0"
2. Update `CHANGELOG.md`
   - Ensure changelog entries for the new release are in this order:
     * `[CHANGE]`
     * `[FEATURE]`
     * `[ENHANCEMENT]`
     * `[BUGFIX]`
   - Run `./tools/release/check-changelog.sh LAST-RELEASE-TAG...master` and add any missing PR which includes user-facing changes

Once your PR with release prepartion is approved, merge it to "release-X.Y" branch, and continue with publishing.

### Publish a release candidate

To publish a release candidate:

1. Do not change the release branch directly; make a PR to the release-X.Y branch with VERSION and any CHANGELOG changes.
   1. Ensure the `VERSION` file has the `-rc.X` suffix (`X` starting from `0`)
1. After merging your PR to release branch, `git tag` the new release (see [How to tag a release](#how-to-tag-a-release)) from release branch.
1. Wait until CI pipeline succeeded (once a tag is created, the release process through GitHub actions will be triggered for this tag)
1. Create a pre-release in GitHub
   - Write the release notes (including a copy-paste of the changelog)
   - Build binaries with `make dist` and attach them to the release
   - Build packages with `make packages`, test them with `make test-packages` and attach them to the release

### Publish a stable release

To publish a stable release:

1. Do not change the release branch directly; make a PR to the release-X.Y branch with VERSION and any CHANGELOG changes.
   1. Ensure the `VERSION` file has **no** `-rc.X` suffix
   1. Update the Cortex version in the following locations:
      - Kubernetes manifests located at `k8s/`
      - Documentation located at `docs/`
1. After merging your PR to release branch, `git tag` the new release (see [How to tag a release](#how-to-tag-a-release)) from release branch.
1. Wait until CI pipeline succeeded (once a tag is created, the release process through GitHub actions will be triggered for this tag)
1. Create a release in GitHub
   - Write the release notes (including a copy-paste of the changelog)
   - Build binaries with `make dist` and attach them to the release
   - Build packages with `make packages`, test them with `make test-packages` and attach them to the release
1. Merge the release branch `release-x.y` to `master`
   - Create `merge-release-X.Y-to-master` branch **from `release-X.Y` branch** locally
   - Merge upstream `master` branch into your `merge-release-X.Y-to-master` and resolve conflicts
   - Send PR for merging your `merge-release-X.Y-to-master` branch into `master`
   - Once approved, merge the PR by using "Merge" commit.
     - This can either be done by temporarily enabling "Allow merge commits" option in "Settings > Options".
     - Alternatively, this can be done locally by merging `merge-release-X.Y-to-master` branch into `master`, and pushing resulting `master` to upstream repository. This doesn't break `master` branch protection, since PR has been approved already, and it also doesn't require removing the protection.
1. Open a PR to add the new version to the backward compatibility integration test (`integration/backward_compatibility_test.go`)

### How to tag a release

Every release is tagged with `v<major>.<minor>.<patch>`, e.g. `v0.1.3`. Note the `v` prefix.

You can do the tagging on the commandline:

```bash
$ tag=$(< VERSION)
$ git tag -s "v${tag}" -m "v${tag}"
$ git push origin "v${tag}"
```
