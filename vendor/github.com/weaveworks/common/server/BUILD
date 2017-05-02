load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["server.go"],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/Sirupsen/logrus:go_default_library",
        "//vendor/github.com/gorilla/mux:go_default_library",
        "//vendor/github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc:go_default_library",
        "//vendor/github.com/mwitkow/go-grpc-middleware:go_default_library",
        "//vendor/github.com/opentracing-contrib/go-stdlib/nethttp:go_default_library",
        "//vendor/github.com/opentracing/opentracing-go:go_default_library",
        "//vendor/github.com/prometheus/client_golang/prometheus:go_default_library",
        "//vendor/github.com/weaveworks-experiments/loki/pkg/client:go_default_library",
        "//vendor/github.com/weaveworks/common/httpgrpc:go_default_library",
        "//vendor/github.com/weaveworks/common/middleware:go_default_library",
        "//vendor/github.com/weaveworks/common/signals:go_default_library",
        "//vendor/golang.org/x/net/context:go_default_library",
        "//vendor/google.golang.org/grpc:go_default_library",
    ],
)
