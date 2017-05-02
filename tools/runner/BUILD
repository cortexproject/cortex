load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["runner.go"],
    visibility = ["//visibility:private"],
    deps = [
        "//vendor/github.com/mgutz/ansi:go_default_library",
        "//vendor/github.com/weaveworks/common/mflag:go_default_library",
    ],
)

go_binary(
    name = "runner",
    library = ":go_default_library",
    visibility = ["//visibility:public"],
)
