# fasthash [![CircleCI](https://circleci.com/gh/segmentio/fasthash.svg?style=shield)](https://circleci.com/gh/segmentio/fasthash) [![Go Report Card](https://goreportcard.com/badge/github.com/segmentio/fasthash)](https://goreportcard.com/report/github.com/segmentio/fasthash) [![GoDoc](https://godoc.org/github.com/segmentio/fasthash?status.svg)](https://godoc.org/github.com/segmentio/fasthash)
Go package porting the standard hashing algorithms to a more efficient implementation.

## Motivations

Go has great support for hashing algorithms in the standard library, but the
APIs are all exposed as interfaces, which means passing strings or byte slices
to those require dynamic memory allocations. Hashing a string typically requires
2 allocations, one for the Hash value, and one to covert the string to a byte
slice.

This package attempts to solve this issue by exposing functions that implement
string hashing algorithms and don't require dynamic memory alloations.

## Testing

To ensure consistency between the `fasthash` package and the standard library,
all tests must be implemented to run against the standard hash functions and
validate that both packages produced the same results.

## Benchmarks

The implementations also have to prove that they are more efficient in terms of
CPU and memory usage than the functions found in the standard library.  
Here's an example with fnv-1a:
```
BenchmarkHash64/standard_hash_function 20000000   105.0 ns/op   342.31 MB/s   56 B/op   2 allocs/op
BenchmarkHash64/hash_function          50000000    38.6 ns/op   932.35 MB/s    0 B/op   0 allocs/op
```

# Usage Example: FNV-1a

```go
package main

import (
    "fmt"

    "github.com/segmentio/fasthash/fnv1a"
)

func main() {
    // Hash a single string.
    h1 := fnv1a.HashString64("Hello World!")
    fmt.Println("FNV-1a hash of 'Hello World!':", h1)

    // Incrementally compute a hash value from a sequence of strings.
    h2 := fnv1a.Init64
    h2 = fnv1a.AddString64(h2, "A")
    h2 = fnv1a.AddString64(h2, "B")
    h2 = fnv1a.AddString64(h2, "C")
    fmt.Println("FNV-1a hash of 'ABC':", h2)
}
```
