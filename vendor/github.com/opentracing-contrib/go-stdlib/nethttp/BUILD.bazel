load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "client.go",
        "doc.go",
        "server.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/opentracing/opentracing-go:go_default_library",
        "//vendor/github.com/opentracing/opentracing-go/ext:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = [
        "client_test.go",
        "server_test.go",
    ],
    library = ":go_default_library",
    deps = [
        "//vendor/github.com/opentracing/opentracing-go:go_default_library",
        "//vendor/github.com/opentracing/opentracing-go/mocktracer:go_default_library",
    ],
)
