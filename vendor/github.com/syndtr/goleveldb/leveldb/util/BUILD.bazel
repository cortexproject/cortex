load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "buffer.go",
        "buffer_pool.go",
        "crc32.go",
        "hash.go",
        "range.go",
        "util.go",
    ],
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "buffer_test.go",
        "hash_test.go",
    ],
    library = ":go_default_library",
)
