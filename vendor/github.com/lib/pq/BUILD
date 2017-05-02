load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "array.go",
        "buf.go",
        "conn.go",
        "conn_go18.go",
        "copy.go",
        "doc.go",
        "encode.go",
        "error.go",
        "notify.go",
        "ssl.go",
        "ssl_go1.7.go",
        "ssl_permissions.go",
        "url.go",
        "user_posix.go",
        "uuid.go",
    ],
    visibility = ["//visibility:public"],
    deps = ["//vendor/github.com/lib/pq/oid:go_default_library"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "array_test.go",
        "bench_test.go",
        "conn_test.go",
        "copy_test.go",
        "encode_test.go",
        "go18_test.go",
        "issues_test.go",
        "notify_test.go",
        "ssl_test.go",
        "url_test.go",
        "uuid_test.go",
    ],
    library = ":go_default_library",
    deps = ["//vendor/github.com/lib/pq/oid:go_default_library"],
)
