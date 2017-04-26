load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "compat.go",
        "ruler.go",
        "scheduler.go",
        "scheduling_queue.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//pkg/chunk:go_default_library",
        "//pkg/configs:go_default_library",
        "//pkg/configs/client:go_default_library",
        "//pkg/distributor:go_default_library",
        "//pkg/ingester/client:go_default_library",
        "//pkg/querier:go_default_library",
        "//pkg/util:go_default_library",
        "//vendor/github.com/jonboulle/clockwork:go_default_library",
        "//vendor/github.com/prometheus/client_golang/prometheus:go_default_library",
        "//vendor/github.com/prometheus/common/log:go_default_library",
        "//vendor/github.com/prometheus/common/model:go_default_library",
        "//vendor/github.com/prometheus/prometheus/config:go_default_library",
        "//vendor/github.com/prometheus/prometheus/notifier:go_default_library",
        "//vendor/github.com/prometheus/prometheus/promql:go_default_library",
        "//vendor/github.com/prometheus/prometheus/rules:go_default_library",
        "//vendor/github.com/weaveworks/common/instrument:go_default_library",
        "//vendor/github.com/weaveworks/common/user:go_default_library",
        "//vendor/golang.org/x/net/context:go_default_library",
        "//vendor/golang.org/x/net/context/ctxhttp:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = [
        "ruler_test.go",
        "scheduling_queue_test.go",
    ],
    library = ":go_default_library",
    deps = [
        "//vendor/github.com/jonboulle/clockwork:go_default_library",
        "//vendor/github.com/prometheus/common/model:go_default_library",
        "//vendor/github.com/stretchr/testify/assert:go_default_library",
        "//vendor/github.com/weaveworks/common/user:go_default_library",
    ],
)
