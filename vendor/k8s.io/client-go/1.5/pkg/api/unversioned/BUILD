load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "doc.go",
        "duration.go",
        "generated.pb.go",
        "group_version.go",
        "helpers.go",
        "meta.go",
        "register.go",
        "time.go",
        "time_proto.go",
        "types.go",
        "types_swagger_doc_generated.go",
        "well_known_labels.go",
        "zz_generated.deepcopy.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/go-openapi/spec:go_default_library",
        "//vendor/github.com/gogo/protobuf/proto:go_default_library",
        "//vendor/github.com/gogo/protobuf/sortkeys:go_default_library",
        "//vendor/github.com/google/gofuzz:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/conversion:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/genericapiserver/openapi/common:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/labels:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/selection:go_default_library",
        "//vendor/k8s.io/client-go/1.5/pkg/util/sets:go_default_library",
    ],
)

filegroup(
    name = "go_default_library_protos",
    srcs = ["generated.proto"],
    visibility = ["//visibility:public"],
)
