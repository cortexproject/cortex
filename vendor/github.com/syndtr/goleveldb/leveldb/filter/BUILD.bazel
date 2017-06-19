load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "bloom.go",
        "filter.go",
    ],
    visibility = ["//visibility:public"],
    deps = ["//vendor/github.com/syndtr/goleveldb/leveldb/util:go_default_library"],
)

go_test(
    name = "go_default_test",
    srcs = ["bloom_test.go"],
    library = ":go_default_library",
    deps = ["//vendor/github.com/syndtr/goleveldb/leveldb/util:go_default_library"],
)
