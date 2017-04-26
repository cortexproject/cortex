load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_prefix")

go_prefix("github.com/weaveworks/cortex")

go_library(
    name = "go_default_library",
    srcs = ["dep.go"],
    visibility = ["//visibility:public"],
    deps = ["//vendor/github.com/gogo/protobuf/gogoproto:go_default_library"],
)
