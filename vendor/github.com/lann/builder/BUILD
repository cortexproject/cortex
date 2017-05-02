load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "builder.go",
        "reflect.go",
        "registry.go",
    ],
    visibility = ["//visibility:public"],
    deps = ["//vendor/github.com/lann/ps:go_default_library"],
)

go_test(
    name = "go_default_xtest",
    srcs = [
        "builder_test.go",
        "example_test.go",
    ],
    deps = ["//vendor/github.com/lann/builder:go_default_library"],
)
