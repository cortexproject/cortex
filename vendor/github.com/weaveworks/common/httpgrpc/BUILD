load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "httpgrpc.go",
        "httpgrpc.pb.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/golang/protobuf/proto:go_default_library",
        "//vendor/github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc:go_default_library",
        "//vendor/github.com/mwitkow/go-grpc-middleware:go_default_library",
        "//vendor/github.com/opentracing/opentracing-go:go_default_library",
        "//vendor/github.com/sercand/kuberesolver:go_default_library",
        "//vendor/github.com/weaveworks/common/middleware:go_default_library",
        "//vendor/golang.org/x/net/context:go_default_library",
        "//vendor/google.golang.org/grpc:go_default_library",
    ],
)

filegroup(
    name = "go_default_library_protos",
    srcs = ["httpgrpc.proto"],
    visibility = ["//visibility:public"],
)
