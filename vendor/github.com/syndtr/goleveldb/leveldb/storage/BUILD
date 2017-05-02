load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "file_storage.go",
        "file_storage_unix.go",
        "mem_storage.go",
        "storage.go",
    ],
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "file_storage_test.go",
        "mem_storage_test.go",
    ],
    library = ":go_default_library",
)
