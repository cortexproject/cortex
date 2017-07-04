load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "alerting.go",
        "manager.go",
        "recording.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/prometheus/client_golang/prometheus:go_default_library",
        "//vendor/github.com/prometheus/common/log:go_default_library",
        "//vendor/github.com/prometheus/common/model:go_default_library",
        "//vendor/github.com/prometheus/prometheus/config:go_default_library",
        "//vendor/github.com/prometheus/prometheus/notifier:go_default_library",
        "//vendor/github.com/prometheus/prometheus/promql:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage/local:go_default_library",
        "//vendor/github.com/prometheus/prometheus/template:go_default_library",
        "//vendor/github.com/prometheus/prometheus/util/strutil:go_default_library",
        "//vendor/golang.org/x/net/context:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = [
        "alerting_test.go",
        "manager_test.go",
        "recording_test.go",
    ],
    library = ":go_default_library",
    deps = [
        "//vendor/github.com/prometheus/common/model:go_default_library",
        "//vendor/github.com/prometheus/prometheus/promql:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage/local:go_default_library",
        "//vendor/golang.org/x/net/context:go_default_library",
    ],
)
