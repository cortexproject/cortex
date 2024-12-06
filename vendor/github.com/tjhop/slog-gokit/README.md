# Go slog-gokit Adapter

This library provides a custom slog.Handler that wraps a go-kit Logger, so that loggers created via `slog.New()` chain their log calls to the internal go-kit Logger.

## Install

```bash
go get github.com/tjhop/slog-gokit
```

## Example

```go
package main

import (
	"log/slog"
	"os"

	"github.com/go-kit/log"
	slgk "github.com/tjhop/slog-gokit"
)

func main() {
	// Take an existing go-kit/log Logger:
	gklogger := log.NewLogfmtLogger(os.Stderr)

	// Create an slog Logger that chains log calls to the go-kit/log Logger:
	slogger := slog.New(slgk.NewGoKitHandler(gklogger, nil))
	slogger.WithGroup("example_group").With("foo", "bar").Info("hello world")

	// The slog Logger produced logs at slog.LevelInfo by default.
	// Optionally create an slog.Leveler to dynamically adjust the level of
	// the slog Logger.
	lvl := &slog.LevelVar{}
	lvl.Set(slog.LevelDebug)
	slogger = slog.New(slgk.NewGoKitHandler(gklogger, lvl))
	slogger.WithGroup("example_group").With("foo", "bar").Info("hello world")
}
```
