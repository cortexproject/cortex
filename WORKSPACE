git_repository(
    name = "io_bazel_rules_go",
    remote = "https://github.com/bazelbuild/rules_go.git",
    # Required for working with bazel 0.5.3
    # https://github.com/bazelbuild/rules_go/tree/10a700af62d2c23cc6e9bbc12055c0abe2808a23
    tag = "0.5.2",
)
load("@io_bazel_rules_go//go:def.bzl", "go_repositories")

go_repositories()
