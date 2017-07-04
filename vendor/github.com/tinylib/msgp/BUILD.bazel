load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["main.go"],
    visibility = ["//visibility:private"],
    deps = [
        "//vendor/github.com/tinylib/msgp/gen:go_default_library",
        "//vendor/github.com/tinylib/msgp/parse:go_default_library",
        "//vendor/github.com/tinylib/msgp/printer:go_default_library",
        "//vendor/github.com/ttacon/chalk:go_default_library",
    ],
)

go_binary(
    name = "msgp",
    library = ":go_default_library",
    visibility = ["//visibility:public"],
)
