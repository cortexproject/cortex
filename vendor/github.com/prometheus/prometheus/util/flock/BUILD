load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "flock.go",
        "flock_unix.go",
    ],
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = ["flock_test.go"],
    library = ":go_default_library",
    deps = ["//vendor/github.com/prometheus/prometheus/util/testutil:go_default_library"],
)
