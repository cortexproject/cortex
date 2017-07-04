load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["install.go"],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/k8s.io/client-go/1.5/pkg/apimachinery/announced:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/apis/batch:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/apis/batch/v1:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/apis/batch/v2alpha1:go_default_library",
    ],
)
