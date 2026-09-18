kslog
===

kslog is a plug-in package to use [slog](https://pkg.go.dev/log/slog) as a [`kgo.Logger`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Logger)

To use,

```go
cl, err := kgo.NewClient(
        kgo.WithLogger(kslog.New(slog.Default())),
        // ...other opts
)
```
