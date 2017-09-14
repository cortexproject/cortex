workspace(name = "com_github_weaveworks_cortex")

http_archive(
    name = "io_bazel_rules_go",
    url = "https://codeload.github.com/bazelbuild/rules_go/zip/0fb90c43c5fab2a0b2d7a8684f26f6995d9aa212",
    strip_prefix = "rules_go-0fb90c43c5fab2a0b2d7a8684f26f6995d9aa212",
    type = "zip",
)

load("//tools/build/bazel:gogo.bzl", "gogo_dependencies")
load("@io_bazel_rules_go//go:def.bzl", "go_rules_dependencies", "go_register_toolchains")
load("@io_bazel_rules_go//proto:def.bzl", "proto_register_toolchains")

gogo_dependencies()
go_rules_dependencies()

go_register_toolchains()
proto_register_toolchains()
