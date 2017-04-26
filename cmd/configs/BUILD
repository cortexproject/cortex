load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["main.go"],
    visibility = ["//visibility:private"],
    deps = [
        "//pkg/configs/api:go_default_library",
        "//pkg/configs/db:go_default_library",
        "//pkg/util:go_default_library",
        "//vendor/github.com/prometheus/common/log:go_default_library",
        "//vendor/github.com/weaveworks/common/middleware:go_default_library",
        "//vendor/github.com/weaveworks/common/server:go_default_library",
        "//vendor/google.golang.org/grpc:go_default_library",
    ],
)

go_binary(
    name = "configs",
    library = ":go_default_library",
    visibility = ["//visibility:public"],
)
