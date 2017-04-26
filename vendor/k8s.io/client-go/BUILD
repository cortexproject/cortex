load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["godeps-json-updater.go"],
    visibility = ["//visibility:private"],
)

go_binary(
    name = "client-go",
    library = ":go_default_library",
    visibility = ["//visibility:public"],
)
