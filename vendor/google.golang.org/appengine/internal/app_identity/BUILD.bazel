load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["app_identity_service.pb.go"],
    visibility = ["//vendor/google.golang.org/appengine:__subpackages__"],
    deps = ["//vendor/github.com/golang/protobuf/proto:go_default_library"],
)

filegroup(
    name = "go_default_library_protos",
    srcs = ["app_identity_service.proto"],
    visibility = ["//visibility:public"],
)
