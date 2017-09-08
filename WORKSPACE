http_archive(
    name = "io_bazel_rules_go",
    url = "https://codeload.github.com/bazelbuild/rules_go/zip/bd13f2d59c804acae7ca8c18fdeb4bf0ecfa1e93",
    strip_prefix = "rules_go-bd13f2d59c804acae7ca8c18fdeb4bf0ecfa1e93",
    sha256 = "c69276b005648bfd6f9961f943b14742b221958cc88ff71ffda30e2605e3b599",
    type = "zip",
)

load("@io_bazel_rules_go//go:def.bzl", "go_rules_dependencies", "go_register_toolchains")
load("@io_bazel_rules_go//proto:def.bzl", "proto_register_toolchains")
go_rules_dependencies()
go_register_toolchains()
proto_register_toolchains()
