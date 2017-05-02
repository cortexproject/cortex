load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "ast.go",
        "bench.go",
        "engine.go",
        "functions.go",
        "lex.go",
        "parse.go",
        "printer.go",
        "quantile.go",
        "test.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/prometheus/client_golang/prometheus:go_default_library",
        "//vendor/github.com/prometheus/common/log:go_default_library",
        "//vendor/github.com/prometheus/common/model:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage/local:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage/metric:go_default_library",
        "//vendor/github.com/prometheus/prometheus/util/stats:go_default_library",
        "//vendor/github.com/prometheus/prometheus/util/strutil:go_default_library",
        "//vendor/github.com/prometheus/prometheus/util/testutil:go_default_library",
        "//vendor/golang.org/x/net/context:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = [
        "engine_test.go",
        "functions_test.go",
        "lex_test.go",
        "parse_test.go",
        "printer_test.go",
        "promql_test.go",
    ],
    library = ":go_default_library",
    deps = [
        "//vendor/github.com/prometheus/common/model:go_default_library",
        "//vendor/github.com/prometheus/prometheus/storage/metric:go_default_library",
        "//vendor/golang.org/x/net/context:go_default_library",
    ],
)
