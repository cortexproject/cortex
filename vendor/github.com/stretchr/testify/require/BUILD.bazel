load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "doc.go",
        "forward_requirements.go",
        "require.go",
        "require_forward.go",
        "requirements.go",
    ],
    visibility = ["//visibility:public"],
    deps = ["//vendor/github.com/stretchr/testify/assert:go_default_library"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "forward_requirements_test.go",
        "requirements_test.go",
    ],
    library = ":go_default_library",
)
