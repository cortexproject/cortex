load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["config.go"],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/prometheus/common/model:go_default_library",
        "//vendor/gopkg.in/yaml.v2:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["config_test.go"],
    library = ":go_default_library",
    deps = [
        "//vendor/github.com/prometheus/common/model:go_default_library",
        "//vendor/gopkg.in/yaml.v2:go_default_library",
    ],
)
