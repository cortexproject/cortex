load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "auth.go",
        "credentials.go",
        "request.go",
        "resolver.go",
        "ruleset.go",
        "socks5.go",
    ],
    visibility = ["//visibility:public"],
    deps = ["//vendor/golang.org/x/net/context:go_default_library"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "auth_test.go",
        "credentials_test.go",
        "request_test.go",
        "resolver_test.go",
        "ruleset_test.go",
        "socks5_test.go",
    ],
    library = ":go_default_library",
    deps = ["//vendor/golang.org/x/net/context:go_default_library"],
)
