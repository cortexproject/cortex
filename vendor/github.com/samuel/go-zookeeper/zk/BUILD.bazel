load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "conn.go",
        "constants.go",
        "dnshostprovider.go",
        "flw.go",
        "lock.go",
        "server_help.go",
        "server_java.go",
        "structs.go",
        "util.go",
    ],
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "cluster_test.go",
        "constants_test.go",
        "dnshostprovider_test.go",
        "flw_test.go",
        "lock_test.go",
        "structs_test.go",
        "throttle_test.go",
        "util_test.go",
        "zk_test.go",
    ],
    library = ":go_default_library",
)
