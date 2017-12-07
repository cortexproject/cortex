workspace(name = "com_github_weaveworks_cortex")

http_archive(
    name = "io_bazel_rules_go",
    url = "https://codeload.github.com/bazelbuild/rules_go/zip/5a08d3fc11190fede27b55bded93bda152abae68",
    strip_prefix = "rules_go-5a08d3fc11190fede27b55bded93bda152abae68",
    type = "zip",
)

load("//tools/build/bazel:gogo.bzl", "gogo_dependencies")
load("@io_bazel_rules_go//go:def.bzl", "go_rules_dependencies", "go_register_toolchains")

gogo_dependencies()
go_rules_dependencies()

go_register_toolchains()
