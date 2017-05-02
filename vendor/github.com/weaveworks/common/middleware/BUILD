load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "errorhandler.go",
        "grpc_auth.go",
        "grpc_instrumentation.go",
        "grpc_logging.go",
        "header_adder.go",
        "http_auth.go",
        "instrument.go",
        "logging.go",
        "middleware.go",
        "path_rewrite.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/Sirupsen/logrus:go_default_library",
        "//vendor/github.com/gorilla/mux:go_default_library",
        "//vendor/github.com/prometheus/client_golang/prometheus:go_default_library",
        "//vendor/github.com/weaveworks/common/user:go_default_library",
        "//vendor/golang.org/x/net/context:go_default_library",
        "//vendor/google.golang.org/grpc:go_default_library",
    ],
)

go_test(
    name = "go_default_xtest",
    srcs = ["instrument_test.go"],
    deps = ["//vendor/github.com/weaveworks/common/middleware:go_default_library"],
)
