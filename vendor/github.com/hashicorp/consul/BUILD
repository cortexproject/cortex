load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "commands.go",
        "main.go",
    ],
    visibility = ["//visibility:private"],
    deps = [
        "//vendor/github.com/hashicorp/consul/command:go_default_library",
        "//vendor/github.com/hashicorp/consul/command/agent:go_default_library",
        "//vendor/github.com/hashicorp/consul/command/base:go_default_library",
        "//vendor/github.com/hashicorp/consul/lib:go_default_library",
        "//vendor/github.com/hashicorp/consul/version:go_default_library",
        "//vendor/github.com/mitchellh/cli:go_default_library",
    ],
)

go_binary(
    name = "consul",
    library = ":go_default_library",
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = ["main_test.go"],
    library = ":go_default_library",
)
