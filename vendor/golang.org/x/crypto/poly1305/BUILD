load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "poly1305.go",
        "sum_amd64.go",
        "sum_amd64.s",
    ],
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = ["poly1305_test.go"],
    library = ":go_default_library",
)
