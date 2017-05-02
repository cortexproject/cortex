load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["logging.go"],
    visibility = ["//visibility:public"],
    deps = ["//vendor/github.com/Sirupsen/logrus:go_default_library"],
)
