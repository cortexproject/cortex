load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "balancer.go",
        "kubernetes.go",
        "models.go",
        "resolver.go",
        "stream.go",
        "util.go",
        "watcher.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/google.golang.org/grpc:go_default_library",
        "//vendor/google.golang.org/grpc/grpclog:go_default_library",
        "//vendor/google.golang.org/grpc/naming:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["balancer_test.go"],
    library = ":go_default_library",
    deps = ["//vendor/google.golang.org/grpc/naming:go_default_library"],
)
