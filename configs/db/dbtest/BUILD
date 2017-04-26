load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["unit.go"],
    visibility = ["//visibility:public"],
    deps = [
        "//configs/db:go_default_library",
        "//vendor/github.com/stretchr/testify/require:go_default_library",
        "//vendor/github.com/weaveworks/common/logging:go_default_library",
    ],
)
