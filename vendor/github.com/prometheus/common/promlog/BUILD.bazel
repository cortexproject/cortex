load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["log.go"],
    importpath = "github.com/prometheus/common/promlog",
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/go-kit/kit/log:go_default_library",
        "//vendor/github.com/go-kit/kit/log/level:go_default_library",
        "//vendor/github.com/pkg/errors:go_default_library",
    ],
)
