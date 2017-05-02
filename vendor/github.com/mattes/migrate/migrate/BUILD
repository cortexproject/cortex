load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["migrate.go"],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/github.com/mattes/migrate/driver:go_default_library",
        "//vendor/github.com/mattes/migrate/file:go_default_library",
        "//vendor/github.com/mattes/migrate/migrate/direction:go_default_library",
        "//vendor/github.com/mattes/migrate/pipe:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["migrate_test.go"],
    library = ":go_default_library",
    deps = [
        "//vendor/github.com/mattes/migrate/driver/postgres:go_default_library",
        "//vendor/github.com/mattes/migrate/driver/ql:go_default_library",
        "//vendor/github.com/mattes/migrate/driver/sqlite3:go_default_library",
    ],
)
