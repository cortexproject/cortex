load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "authentication_client.go",
        "doc.go",
        "generated_expansion.go",
        "tokenreview.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/k8s.io/client-go/1.5/pkg/api:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/apimachinery/registered:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/runtime/serializer:go_default_library",
        "//vendor/k8s.io/client-go/1.5/rest:go_default_library",
    ],
)
