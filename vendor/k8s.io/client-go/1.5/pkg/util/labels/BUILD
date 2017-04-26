load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "doc.go",
        "labels.go",
    ],
    visibility = ["//visibility:public"],
    deps = ["//vendor/k8s.io/client-go/1.5/pkg/api/unversioned:go_default_library"],
)
