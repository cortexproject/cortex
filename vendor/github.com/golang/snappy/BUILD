load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "decode.go",
        "decode_amd64.go",
        "decode_amd64.s",
        "encode.go",
        "encode_amd64.go",
        "encode_amd64.s",
        "snappy.go",
    ],
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "golden_test.go",
        "snappy_test.go",
    ],
    library = ":go_default_library",
)
