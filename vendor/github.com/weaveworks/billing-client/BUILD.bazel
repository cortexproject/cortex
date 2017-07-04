load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "client.go",
        "config.go",
        "event.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/Sirupsen/logrus:go_default_library",
        "//vendor/github.com/fluent/fluent-logger-golang/fluent:go_default_library",
        "//vendor/github.com/prometheus/client_golang/prometheus:go_default_library",
        "//vendor/github.com/weaveworks/common/instrument:go_default_library",
        "//vendor/golang.org/x/net/context:go_default_library",
    ],
)
