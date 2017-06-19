load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "conversion.go",
        "defaults.go",
        "doc.go",
        "generated.pb.go",
        "register.go",
        "types.generated.go",
        "types.go",
        "types_swagger_doc_generated.go",
        "zz_generated.conversion.go",
        "zz_generated.deepcopy.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/gogo/protobuf/proto:go_default_library",
        "//vendor/github.com/gogo/protobuf/sortkeys:go_default_library",
        "//vendor/github.com/ugorji/go/codec:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/api:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/api/resource:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/api/unversioned:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/api/v1:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/apis/autoscaling:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/apis/batch:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/apis/extensions:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/conversion:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/runtime:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/types:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/util/intstr:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/watch/versioned:go_default_library",
    ],
)

filegroup(
    name = "go_default_library_protos",
    srcs = ["generated.proto"],
    visibility = ["//visibility:public"],
)
