load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "fluent.go",
        "proto.go",
        "proto_gen.go",
        "version.go",
    ],
    visibility = ["//visibility:public"],
    deps = ["//vendor/github.com/tinylib/msgp/msgp:go_default_library"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "fluent_test.go",
        "proto_gen_test.go",
    ],
    library = ":go_default_library",
    deps = [
        "//vendor/github.com/bmizerany/assert:go_default_library",
        "//vendor/github.com/tinylib/msgp/msgp:go_default_library",
    ],
)
