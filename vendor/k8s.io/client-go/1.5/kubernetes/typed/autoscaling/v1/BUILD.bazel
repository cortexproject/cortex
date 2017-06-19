load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "autoscaling_client.go",
        "doc.go",
        "generated_expansion.go",
        "horizontalpodautoscaler.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/k8s.io/client-go/1.5/pkg/api:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/apimachinery/registered:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/apis/autoscaling/v1:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/runtime/serializer:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/watch:go_default_library",
        "//vendor/k8s.io/client-go/1.5/rest:go_default_library",
    ],
)
