load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "async.go",
        "azure.go",
        "config.go",
        "devicetoken.go",
        "environments.go",
        "persist.go",
        "token.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/Azure/go-autorest/autorest:go_default_library",
        "//vendor/github.com/Azure/go-autorest/autorest/date:go_default_library",
        "//vendor/github.com/dgrijalva/jwt-go:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = [
        "async_test.go",
        "azure_test.go",
        "devicetoken_test.go",
        "environments_test.go",
        "persist_test.go",
        "token_test.go",
    ],
    library = ":go_default_library",
    deps = [
        "//vendor/github.com/Azure/go-autorest/autorest:go_default_library",
        "//vendor/github.com/Azure/go-autorest/autorest/mocks:go_default_library",
    ],
)
