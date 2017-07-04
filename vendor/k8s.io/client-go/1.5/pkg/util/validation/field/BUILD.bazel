load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "errors.go",
        "path.go",
    ],
    visibility = ["//visibility:public"],
    deps = ["//vendor/k8s.io/client-go/1.5/pkg/util/errors:go_default_library"],
)
