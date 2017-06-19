load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "block.go",
        "lz4.go",
        "reader.go",
        "writer.go",
    ],
    visibility = ["//visibility:public"],
    deps = ["//vendor/github.com/pierrec/xxHash/xxHash32:go_default_library"],
)

go_test(
    name = "go_default_test",
    srcs = ["export_test.go"],
    library = ":go_default_library",
)

go_test(
    name = "go_default_xtest",
    srcs = ["lz4_test.go"],
    deps = ["//vendor/github.com/pierrec/lz4:go_default_library"],
)
