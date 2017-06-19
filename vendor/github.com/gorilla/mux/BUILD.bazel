load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "context_native.go",
        "doc.go",
        "mux.go",
        "regexp.go",
        "route.go",
    ],
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "bench_test.go",
        "context_native_test.go",
        "mux_test.go",
        "old_test.go",
    ],
    library = ":go_default_library",
)
