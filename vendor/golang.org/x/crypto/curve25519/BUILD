load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "const_amd64.s",
        "const_amd64.h",
        "cswap_amd64.s",
        "doc.go",
        "freeze_amd64.s",
        "ladderstep_amd64.s",
        "mont25519_amd64.go",
        "mul_amd64.s",
        "square_amd64.s",
    ],
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = ["curve25519_test.go"],
    library = ":go_default_library",
)
