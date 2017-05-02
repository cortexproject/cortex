load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "hsalsa20.go",
        "salsa2020_amd64.s",
        "salsa208.go",
        "salsa20_amd64.go",
    ],
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = ["salsa_test.go"],
    library = ":go_default_library",
)
