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
| v0.5.0         | 2019-12-11                                 | **searching for volunteer**                 |

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

For a patch release, work in the branch of the minor release you want to patch.

For a new major or minor release, create the corresponding release branch based on the master branch.

Update `CHANGELOG.md` in a proper PR as this gives others the opportunity to chime in on the release in general and on the addition to the changelog in particular.

Note that `CHANGELOG.md` should only document changes relevant to users of Cortex, including external API changes, performance improvements, and new features. Do not document changes of internal interfaces, code refactorings and clean-ups, changes to the build process, etc. People interested in these are asked to refer to the git history.

Entries in the `CHANGELOG.md` are meant to be in this order:

* `[CHANGE]`
* `[FEATURE]`
* `[ENHANCEMENT]`
* `[BUGFIX]`

### Draft the new release

Tag the new release with a tag named `v<major>.<minor>.<patch>`, e.g. `v0.1.3`. Note the `v` prefix.

You can do the tagging on the commandline:

```bash
$ tag=$(< VERSION)
$ git tag -s "v${tag}" -m "v${tag}"
$ git push --tags
```

Signing a tag with a GPG key is appreciated, but in case you can't add a GPG key to your Github account using the following [procedure](https://help.github.com/articles/generating-a-gpg-key/), you can replace the `-s` flag by `-a` flag of the `git tag` command to only annotate the tag without signing.

Once a tag is created, the release process through CircleCI will be triggered for this tag. If everything goes smoothly, create a release in the GitHub UI with the changelog for this release.
