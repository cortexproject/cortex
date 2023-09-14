// Copyright 2020 Brad Fitzpatrick. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package go4.org/unsafe/assume-no-moving-gc exists so you can depend
// on it from unsafe code that wants to declare that it assumes that
// the Go runtime does not using a moving garbage collector. Specifically,
// it asserts that the caller is playing stupid games with the addresses
// of heap-allocated values. It says nothing about values that Go's escape
// analysis keeps on the stack. Ensuring things aren't stack-allocated
// is the caller's responsibility.
//
// This package is then updated as needed for new Go versions when
// that is still the case and explodes at runtime with a failure
// otherwise, with the idea that it's better to not start at all than
// to silently corrupt your data at runtime.
//
// To use:
//
//     import _ "go4.org/unsafe/assume-no-moving-gc"
//
// There is no API.
//
// As of Go 1.21, this package asks the Go runtime whether it can move
// heap objects around. If you get an error on versions prior to that,
// go get go4.org/unsafe/assume-no-moving-gc@latest and things will
// work.
//
// The GitHub repo is at https://github.com/go4org/unsafe-assume-no-moving-gc
package assume_no_moving_gc

const env = "ASSUME_NO_MOVING_GC_UNSAFE"
