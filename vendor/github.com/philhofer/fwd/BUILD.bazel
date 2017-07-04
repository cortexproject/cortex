load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "reader.go",
        "writer.go",
        "writer_unsafe.go",
    ],
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "reader_test.go",
        "writer_test.go",
    ],
    library = ":go_default_library",
)
