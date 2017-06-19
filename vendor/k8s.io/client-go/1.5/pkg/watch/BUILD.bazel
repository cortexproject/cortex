load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "doc.go",
        "filter.go",
        "mux.go",
        "streamwatcher.go",
        "until.go",
        "watch.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/golang/glog:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/api/unversioned:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/runtime:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/util/net:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/util/runtime:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/util/wait:go_default_library",
    ],
)
