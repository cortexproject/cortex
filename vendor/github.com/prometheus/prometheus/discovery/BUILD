load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["discovery.go"],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/prometheus/common/log:go_default_library",
        "//vendor/github.com/prometheus/prometheus/config:go_default_library",
        "//vendor/github.com/prometheus/prometheus/discovery/azure:go_default_library",
        "//vendor/github.com/prometheus/prometheus/discovery/consul:go_default_library",
        "//vendor/github.com/prometheus/prometheus/discovery/dns:go_default_library",
        "//vendor/github.com/prometheus/prometheus/discovery/ec2:go_default_library",
        "//vendor/github.com/prometheus/prometheus/discovery/file:go_default_library",
        "//vendor/github.com/prometheus/prometheus/discovery/gce:go_default_library",
        "//vendor/github.com/prometheus/prometheus/discovery/kubernetes:go_default_library",
        "//vendor/github.com/prometheus/prometheus/discovery/marathon:go_default_library",
        "//vendor/github.com/prometheus/prometheus/discovery/triton:go_default_library",
        "//vendor/github.com/prometheus/prometheus/discovery/zookeeper:go_default_library",
        "//vendor/golang.org/x/net/context:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["discovery_test.go"],
    library = ":go_default_library",
    deps = [
        "//vendor/github.com/prometheus/prometheus/config:go_default_library",
        "//vendor/golang.org/x/net/context:go_default_library",
        "//vendor/gopkg.in/yaml.v2:go_default_library",
    ],
)
