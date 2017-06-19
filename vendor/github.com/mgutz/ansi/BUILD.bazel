load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "ansi.go",
        "doc.go",
        "print.go",
    ],
    visibility = ["//visibility:public"],
    deps = ["//vendor/github.com/mattn/go-colorable:go_default_library"],
)

go_test(
    name = "go_default_test",
    srcs = ["ansi_test.go"],
    library = ":go_default_library",
)
