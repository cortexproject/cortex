load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "doc.go",
        "helpers.go",
        "register.go",
        "types.generated.go",
        "types.go",
        "zz_generated.deepcopy.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/ugorji/go/codec:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/api:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/api/resource:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/api/unversioned:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/apis/autoscaling:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/apis/batch:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/conversion:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/runtime:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/types:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/util/intstr:go_default_library",
    ],
)
