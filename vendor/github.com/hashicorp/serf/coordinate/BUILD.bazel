load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "client.go",
        "config.go",
        "coordinate.go",
        "phantom.go",
    ],
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "client_test.go",
        "coordinate_test.go",
        "performance_test.go",
        "util_test.go",
    ],
    library = ":go_default_library",
)
