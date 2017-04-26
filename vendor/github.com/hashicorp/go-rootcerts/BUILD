load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "doc.go",
        "rootcerts.go",
        "rootcerts_darwin.go",
    ],
    visibility = ["//visibility:public"],
    deps = ["//vendor/github.com/mitchellh/go-homedir:go_default_library"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "rootcerts_darwin_test.go",
        "rootcerts_test.go",
    ],
    library = ":go_default_library",
)
