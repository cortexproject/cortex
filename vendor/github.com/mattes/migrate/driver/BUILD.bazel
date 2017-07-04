load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "driver.go",
        "registry.go",
    ],
    visibility = ["//visibility:public"],
    deps = ["//vendor/github.com/mattes/migrate/file:go_default_library"],
)
