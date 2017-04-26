load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "globaltracer.go",
        "gocontext.go",
        "noop.go",
        "propagation.go",
        "span.go",
        "tracer.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/opentracing/opentracing-go/log:go_default_library",
        "//vendor/golang.org/x/net/context:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = [
        "gocontext_test.go",
        "options_test.go",
        "propagation_test.go",
        "testtracer_test.go",
    ],
    library = ":go_default_library",
    deps = [
        "//vendor/github.com/opentracing/opentracing-go/log:go_default_library",
        "//vendor/github.com/stretchr/testify/assert:go_default_library",
        "//vendor/github.com/stretchr/testify/require:go_default_library",
        "//vendor/golang.org/x/net/context:go_default_library",
    ],
)
