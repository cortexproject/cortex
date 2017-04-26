load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "index.go",
        "interface.go",
        "leveldb.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/prometheus/common/model:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage/local/codable:go_default_library",
        "//vendor/github.com/syndtr/goleveldb/leveldb:go_default_library",
        "//vendor/github.com/syndtr/goleveldb/leveldb/filter:go_default_library",
        "//vendor/github.com/syndtr/goleveldb/leveldb/iterator:go_default_library",
        "//vendor/github.com/syndtr/goleveldb/leveldb/opt:go_default_library",
        "//vendor/github.com/syndtr/goleveldb/leveldb/util:go_default_library",
    ],
)
