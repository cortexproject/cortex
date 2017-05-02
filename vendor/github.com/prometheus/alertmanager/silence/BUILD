load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["silence.go"],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/golang/protobuf/ptypes:go_default_library",
        "//vendor/github.com/golang/protobuf/ptypes/timestamp:go_default_library",
        "//vendor/github.com/matttproud/golang_protobuf_extensions/pbutil:go_default_library",
        "//vendor/github.com/prometheus/alertmanager/silence/silencepb:go_default_library",
        "//vendor/github.com/prometheus/alertmanager/types:go_default_library",
        "//vendor/github.com/prometheus/client_golang/prometheus:go_default_library",
        "//vendor/github.com/prometheus/common/log:go_default_library",
        "//vendor/github.com/prometheus/common/model:go_default_library",
        "//vendor/github.com/satori/go.uuid:go_default_library",
        "//vendor/github.com/weaveworks/mesh:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["silence_test.go"],
    library = ":go_default_library",
    deps = [
        "//vendor/github.com/golang/protobuf/ptypes:go_default_library",
        "//vendor/github.com/golang/protobuf/ptypes/timestamp:go_default_library",
        "//vendor/github.com/prometheus/alertmanager/silence/silencepb:go_default_library",
        "//vendor/github.com/prometheus/common/model:go_default_library",
        "//vendor/github.com/stretchr/testify/require:go_default_library",
        "//vendor/github.com/weaveworks/mesh:go_default_library",
    ],
)
