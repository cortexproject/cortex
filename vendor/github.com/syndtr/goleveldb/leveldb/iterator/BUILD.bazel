load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "array_iter.go",
        "indexed_iter.go",
        "iter.go",
        "merged_iter.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/syndtr/goleveldb/leveldb/comparer:go_default_library",
        "//vendor/github.com/syndtr/goleveldb/leveldb/errors:go_default_library",
        "//vendor/github.com/syndtr/goleveldb/leveldb/util:go_default_library",
    ],
)

go_test(
    name = "go_default_xtest",
    srcs = [
        "array_iter_test.go",
        "indexed_iter_test.go",
        "iter_suite_test.go",
        "merged_iter_test.go",
    ],
    deps = [
        "//vendor/github.com/onsi/ginkgo:go_default_library",
        "//vendor/github.com/onsi/gomega:go_default_library",
        "//vendor/github.com/syndtr/goleveldb/leveldb/comparer:go_default_library",
        "//vendor/github.com/syndtr/goleveldb/leveldb/iterator:go_default_library",
        "//vendor/github.com/syndtr/goleveldb/leveldb/testutil:go_default_library",
    ],
)
