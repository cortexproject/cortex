load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["xxHash32.go"],
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_xtest",
    srcs = [
        "example_test.go",
        "xxHash32_test.go",
    ],
    deps = ["//vendor/github.com/pierrec/xxHash/xxHash32:go_default_library"],
)
