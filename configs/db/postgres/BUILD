load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["postgres.go"],
    visibility = ["//visibility:public"],
    deps = [
        "//configs:go_default_library",
        "//vendor/github.com/Masterminds/squirrel:go_default_library",
        "//vendor/github.com/Sirupsen/logrus:go_default_library",
        "//vendor/github.com/lib/pq:go_default_library",
        "//vendor/github.com/mattes/migrate/driver/postgres:go_default_library",
        "//vendor/github.com/mattes/migrate/migrate:go_default_library",
    ],
)
