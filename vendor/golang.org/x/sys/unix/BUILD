load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "asm_darwin_amd64.s",
        "constants.go",
        "dirent.go",
        "endian_little.go",
        "env_unix.go",
        "env_unset.go",
        "flock.go",
        "race0.go",
        "sockcmsg_unix.go",
        "str.go",
        "syscall.go",
        "syscall_bsd.go",
        "syscall_darwin.go",
        "syscall_darwin_amd64.go",
        "syscall_unix.go",
        "syscall_unix_gc.go",
        "zerrors_darwin_amd64.go",
        "zsyscall_darwin_amd64.go",
        "zsysnum_darwin_amd64.go",
        "ztypes_darwin_amd64.go",
    ],
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = ["export_test.go"],
    library = ":go_default_library",
)

go_test(
    name = "go_default_xtest",
    srcs = [
        "mmap_unix_test.go",
        "syscall_bsd_test.go",
        "syscall_test.go",
        "syscall_unix_test.go",
    ],
    deps = ["//vendor/golang.org/x/sys/unix:go_default_library"],
)
