load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["codable.go"],
    visibility = ["//visibility:public"],
    deps = ["//vendor/github.com/prometheus/common/model:go_default_library"],
)

go_test(
    name = "go_default_test",
    srcs = ["codable_test.go"],
    library = ":go_default_library",
)
