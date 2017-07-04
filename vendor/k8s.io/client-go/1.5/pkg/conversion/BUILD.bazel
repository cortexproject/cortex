load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "cloner.go",
        "converter.go",
        "deep_equal.go",
        "doc.go",
        "helper.go",
    ],
    visibility = ["//visibility:public"],
    deps = ["//vendor/k8s.io/client-go/1.5/pkg/third_party/forked/golang/reflect:go_default_library"],
)
