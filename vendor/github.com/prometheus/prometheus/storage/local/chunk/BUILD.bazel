load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "chunk.go",
        "delta.go",
        "delta_helpers.go",
        "doubledelta.go",
        "instrumentation.go",
        "varbit.go",
        "varbit_helpers.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/prometheus/client_golang/prometheus:go_default_library",
        "//vendor/github.com/prometheus/common/model:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage/metric:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = [
        "chunk_test.go",
        "delta_test.go",
        "varbit_test.go",
    ],
    library = ":go_default_library",
    deps = ["//vendor/github.com/prometheus/common/model:go_default_library"],
)
