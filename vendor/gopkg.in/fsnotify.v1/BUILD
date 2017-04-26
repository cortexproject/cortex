load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "fsnotify.go",
        "kqueue.go",
        "open_mode_darwin.go",
    ],
    visibility = ["//visibility:public"],
    deps = ["//vendor/golang.org/x/sys/unix:go_default_library"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "fsnotify_test.go",
        "integration_darwin_test.go",
        "integration_test.go",
    ],
    library = ":go_default_library",
    deps = ["//vendor/golang.org/x/sys/unix:go_default_library"],
)

go_test(
    name = "go_default_xtest",
    srcs = ["example_test.go"],
    deps = ["//vendor/github.com/fsnotify/fsnotify:go_default_library"],
)
