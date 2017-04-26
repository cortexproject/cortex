load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["tags.go"],
    visibility = ["//visibility:public"],
    deps = ["//vendor/github.com/opentracing/opentracing-go:go_default_library"],
)

go_test(
    name = "go_default_xtest",
    srcs = ["tags_test.go"],
    deps = [
        "//vendor/github.com/opentracing/opentracing-go:go_default_library",
        "//vendor/github.com/opentracing/opentracing-go/ext:go_default_library",
        "//vendor/github.com/opentracing/opentracing-go/mocktracer:go_default_library",
        "//vendor/github.com/stretchr/testify/assert:go_default_library",
    ],
)
