load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["version.go"],
    importpath = "cloud.google.com/go/internal/version",
    visibility = ["//vendor/cloud.google.com/go:__subpackages__"],
)

go_test(
    name = "go_default_test",
    srcs = ["version_test.go"],
    importpath = "cloud.google.com/go/internal/version",
    library = ":go_default_library",
)
