load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "helpers.go",
        "register.go",
        "types.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/k8s.io/client-go/1.5/pkg/api/unversioned:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/runtime:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = [
        "helpers_test.go",
        "types_test.go",
    ],
    library = ":go_default_library",
    deps = ["//vendor/github.com/ghodss/yaml:go_default_library"],
)
