load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "decode.go",
        "doc.go",
        "encode.go",
        "jsonstring.go",
    ],
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "decode-bench_test.go",
        "decode_test.go",
        "encode_internal_test.go",
    ],
    library = ":go_default_library",
    deps = ["//vendor/github.com/kr/logfmt:go_default_library"],
)

go_test(
    name = "go_default_xtest",
    srcs = [
        "encode_test.go",
        "example_test.go",
    ],
    deps = ["//vendor/github.com/go-logfmt/logfmt:go_default_library"],
)
