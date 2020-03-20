# Releases

This page describes the release process and the currently planned schedule for upcoming releases as well as the respective release shepherd. Release shepherds are chosen on a voluntary basis.

## Release schedule

Our goal is to provide a new minor release every 4 weeks. This is a new process and everything in this document is subject to change.

| release series | date of first pre-release (year-month-day) | release shepherd                            |
|----------------|--------------------------------------------|---------------------------------------------|
| v0.1.0         | 2019-07-31                                 | Chris Marchbanks (GitHub: @csmarchbanks)    |
| v0.2.0         | 2019-08-28                                 | Goutham Veeramachaneni (Github: @gouthamve) |
| v0.3.0         | 2019-10-09                                 | Bryan Boreham (@bboreham)                   |
| v0.4.0         | 2019-11-13                                 | Tom Wilkie (@tomwilkie)                     |
| v0.5.0         | 2020-01-08                                 | _Abandoned_                                 |
| v0.6.0         | 2020-01-22                                 | Marco Pracucci (@pracucci)                  |
| v0.7.0         | 2020-03-09                                 | Marco Pracucci (@pracucci)                  |

## Release shepherd responsibilities

The release shepherd is responsible for the entire release series of a minor release, meaning all pre- and patch releases of a minor release. The process formally starts with the initial pre-release, but some preparations should be done a few days in advance.

* We aim to keep the master branch in a working state at all times. In principle, it should be possible to cut a release from master at any time. In practice, things might not work out as nicely. A few days before the pre-release is scheduled, the shepherd should check the state of master. Following their best judgement, the shepherd should try to expedite bug fixes that are still in progress but should make it into the release. On the other hand, the shepherd may hold back merging last-minute invasive and risky changes that are better suited for the next minor release.
* On the date listed in the table above, the release shepherd cuts the first pre-release (using the suffix `-rc.0`) and creates a new branch called  `release-<major>.<minor>` starting at the commit tagged for the pre-release. In general, a pre-release is considered a release candidate (that's what `rc` stands for) and should therefore not contain any known bugs that are planned to be fixed in the final release.
* With the pre-release, the release shepherd is responsible for coordinating or running the release candidate in Grafana or WeaveWorks production for 3 days.
* If regressions or critical bugs are detected, they need to get fixed before cutting a new pre-release (called `-rc.1`, `-rc.2`, etc.). 

See the next section for details on cutting an individual release.

## How to cut an individual release

### Branch management and versioning strategy

We use [Semantic Versioning](https://semver.org/).

We maintain a separate branch for each minor release, named `release-<major>.<minor>`, e.g. `release-1.1`, `release-2.0`.

The usual flow is to merge new features and changes into the master branch and to merge bug fixes into the latest release branch. Bug fixes are then merged into master from the latest release branch. The master branch should always contain all commits from the latest release branch. As long as master hasn't deviated from the release branch, new commits can also go to master, followed by merging master back into the release branch.

If a bug fix got accidentally merged into master after non-bug-fix changes in master, the bug-fix commits have to be cherry-picked into the release branch, which then have to be merged back into master. Try to avoid that situation.

Maintaining the release branches for older minor releases happens on a best effort basis.

### Prepare your release

For a new major or minor release, create the corresponding release branch based on the master branch. For a patch release, work in the branch of the minor release you want to patch.

0. Make sure you've a GPG key associated to your GitHub account (`git tag` will be signed with that GPG key)
   - You can add a GPG key to your GitHub account following [this procedure](https://help.github.com/articles/generating-a-gpg-key/)
1. Update the version number in the `VERSION` file
2. Update `CHANGELOG.md`
   - Add a new section for the new release with all the changelog entries
   - Ensure changelog entries are in this order:
     * `[CHANGE]`
     * `[FEATURE]`
     * `[ENHANCEMENT]`
     * `[BUGFIX]`
   - Run `./tools/release/check-changelog.sh LAST-RELEASE-TAG...master` and add any missing PR which includes user-facing changes

### Publish a release candidate

To publish a release candidate:

1. Ensure the `VERSION` number has the `-rc.X` suffix (`X` starting from `0`)
2. `git tag` the new release (see [How to tag a release](#how-to-tag-a-release))
3. Wait until CI pipeline succeeded (once a tag is created, the release process through CircleCI will be triggered for this tag)
3. Create a pre-release in GitHub
   - Write the release notes (including a copy-paste of the changelog)
   - Build binaries with `make disk` and attach them to the release

### Publish a stable release

To publish a stable release:

1. Ensure the `VERSION` number has **no** `-rc.X` suffix
2. Update the Cortex version in the following locations:
   - Kubernetes manifests located at `k8s/`
   - Documentation located at `docs/`
3. `git tag` the new release (see [How to tag a release](#how-to-tag-a-release))
4. Wait until CI pipeline succeeded (once a tag is created, the release process through CircleCI will be triggered for this tag)
5. Create a release in GitHub
   - Write the release notes (including a copy-paste of the changelog)
   - Build binaries with `make dist` and attach them to the release
6. Merge the release branch `release-x.y` to `master`
   - Merge to `master` in the local checkout
   - Fix any conflict and `git commit -s`
   - Temporarily disable "Include administrators" in the [`master` branch protection rule](https://github.com/cortexproject/cortex/settings/branch_protection_rules)
   - Push changes to upstream (please double check before pushing!)
   - Re-enable "Include administrators" in the [`master` branch protection rule](https://github.com/cortexproject/cortex/settings/branch_protection_rules)
7. Open a PR to add the new version to the backward compatibility integration test (`integration/backward_compatibility_test.go`)

### How to tag a release

Every release is tagged with `v<major>.<minor>.<patch>`, e.g. `v0.1.3`. Note the `v` prefix.

You can do the tagging on the commandline:

```bash
$ tag=$(< VERSION)
$ git tag -s "v${tag}" -m "v${tag}"
$ git push origin "v${tag}"
```
