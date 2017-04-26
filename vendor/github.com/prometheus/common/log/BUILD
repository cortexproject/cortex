load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "log.go",
        "syslog_formatter.go",
    ],
    visibility = ["//visibility:public"],
    deps = ["//vendor/github.com/Sirupsen/logrus:go_default_library"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "log_test.go",
        "syslog_formatter_test.go",
    ],
    library = ":go_default_library",
    deps = ["//vendor/github.com/Sirupsen/logrus:go_default_library"],
)
