load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "crashrecovery.go",
        "heads.go",
        "instrumentation.go",
        "interface.go",
        "locker.go",
        "mapper.go",
        "noop_storage.go",
        "persistence.go",
        "series.go",
        "storage.go",
        "test_helpers.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/prometheus/client_golang/prometheus:go_default_library",
        "//vendor/github.com/prometheus/common/log:go_default_library",
        "//vendor/github.com/prometheus/common/model:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage/local/chunk:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage/local/codable:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage/local/index:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage/metric:go_default_library",
        "//vendor/github.com/prometheus/prometheus/util/flock:go_default_library",
        "//vendor/github.com/prometheus/prometheus/util/testutil:go_default_library",
        "//vendor/golang.org/x/net/context:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = [
        "locker_test.go",
        "mapper_test.go",
        "persistence_test.go",
        "series_test.go",
        "storage_test.go",
    ],
    library = ":go_default_library",
    deps = [
        "//vendor/github.com/prometheus/common/log:go_default_library",
        "//vendor/github.com/prometheus/common/model:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage/local/chunk:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage/local/codable:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage/local/index:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage/metric:go_default_library",
        "//vendor/github.com/prometheus/prometheus/util/testutil:go_default_library",
        "//vendor/golang.org/x/net/context:go_default_library",
    ],
)
