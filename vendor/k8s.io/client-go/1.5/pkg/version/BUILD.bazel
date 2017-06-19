load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "base.go",
        "doc.go",
        "semver.go",
        "version.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/blang/semver:go_default_library",
        "//vendor/github.com/golang/glog:go_default_library",
    ],
)
