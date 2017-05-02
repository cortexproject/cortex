load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["api.go"],
    visibility = ["//visibility:public"],
    deps = [
        "//pkg/configs:go_default_library",
        "//pkg/configs/db:go_default_library",
        "//pkg/util:go_default_library",
        "//vendor/github.com/Sirupsen/logrus:go_default_library",
        "//vendor/github.com/gorilla/mux:go_default_library",
        "//vendor/github.com/prometheus/alertmanager/config:go_default_library",
        "//vendor/github.com/weaveworks/common/user:go_default_library",
    ],
)

go_test(
    name = "go_default_xtest",
    srcs = [
        "api_test.go",
        "helpers_test.go",
    ],
    deps = [
        ":go_default_library",
        "//pkg/configs:go_default_library",
        "//pkg/configs/db:go_default_library",
        "//pkg/configs/db/dbtest:go_default_library",
        "//vendor/github.com/stretchr/testify/assert:go_default_library",
        "//vendor/github.com/stretchr/testify/require:go_default_library",
        "//vendor/github.com/weaveworks/common/user:go_default_library",
    ],
)
