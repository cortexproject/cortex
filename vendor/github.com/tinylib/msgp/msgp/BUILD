load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "advise_other.go",
        "circular.go",
        "defs.go",
        "edit.go",
        "elsize.go",
        "errors.go",
        "extension.go",
        "file.go",
        "integers.go",
        "json.go",
        "json_bytes.go",
        "number.go",
        "read.go",
        "read_bytes.go",
        "size.go",
        "unsafe.go",
        "write.go",
        "write_bytes.go",
    ],
    visibility = ["//visibility:public"],
    deps = ["//vendor/github.com/philhofer/fwd:go_default_library"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "edit_test.go",
        "extension_test.go",
        "floatbench_test.go",
        "json_bytes_test.go",
        "json_test.go",
        "number_test.go",
        "raw_test.go",
        "read_bytes_test.go",
        "read_test.go",
        "write_bytes_test.go",
        "write_test.go",
    ],
    library = ":go_default_library",
)

go_test(
    name = "go_default_xtest",
    srcs = [
        "defs_test.go",
        "file_test.go",
    ],
    deps = ["//vendor/github.com/tinylib/msgp/msgp:go_default_library"],
)
