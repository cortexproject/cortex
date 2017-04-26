load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "compat.go",
        "error.go",
        "flags.go",
        "hash_bucket_histogram.go",
        "http.go",
        "matchers.go",
        "math.go",
        "net.go",
        "priority_queue.go",
        "util.go",
        "validate.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//ingester/client:go_default_library",
        "//vendor/github.com/gogo/protobuf/proto:go_default_library",
        "//vendor/github.com/golang/snappy:go_default_library",
        "//vendor/github.com/prometheus/client_golang/prometheus:go_default_library",
        "//vendor/github.com/prometheus/common/model:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage/metric:go_default_library",
        "//vendor/github.com/weaveworks/common/errors:go_default_library",
        "//vendor/github.com/weaveworks/common/instrument:go_default_library",
        "//vendor/golang.org/x/net/context:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = [
        "compat_test.go",
        "matchers_test.go",
        "priority_queue_test.go",
        "validate_test.go",
    ],
    library = ":go_default_library",
    deps = [
        "//vendor/github.com/prometheus/common/model:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage/metric:go_default_library",
        "//vendor/github.com/stretchr/testify/assert:go_default_library",
        "//vendor/github.com/weaveworks/common/test:go_default_library",
    ],
)
