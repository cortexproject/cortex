load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["api.go"],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/golang/protobuf/ptypes:go_default_library",
        "//vendor/github.com/prometheus/alertmanager/config:go_default_library",
        "//vendor/github.com/prometheus/alertmanager/dispatch:go_default_library",
        "//vendor/github.com/prometheus/alertmanager/provider:go_default_library",
        "//vendor/github.com/prometheus/alertmanager/silence:go_default_library",
        "//vendor/github.com/prometheus/alertmanager/silence/silencepb:go_default_library",
        "//vendor/github.com/prometheus/alertmanager/types:go_default_library",
        "//vendor/github.com/prometheus/client_golang/prometheus:go_default_library",
        "//vendor/github.com/prometheus/common/log:go_default_library",
        "//vendor/github.com/prometheus/common/model:go_default_library",
        "//vendor/github.com/prometheus/common/route:go_default_library",
        "//vendor/github.com/prometheus/common/version:go_default_library",
        "//vendor/golang.org/x/net/context:go_default_library",
    ],
)
