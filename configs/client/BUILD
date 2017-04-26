load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["configs.go"],
    visibility = ["//visibility:public"],
    deps = [
        "//configs:go_default_library",
        "//vendor/github.com/prometheus/alertmanager/config:go_default_library",
        "//vendor/github.com/prometheus/common/log:go_default_library",
        "//vendor/github.com/prometheus/prometheus/promql:go_default_library",
        "//vendor/github.com/prometheus/prometheus/rules:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["configs_test.go"],
    library = ":go_default_library",
    deps = [
        "//configs:go_default_library",
        "//vendor/github.com/stretchr/testify/assert:go_default_library",
    ],
)
