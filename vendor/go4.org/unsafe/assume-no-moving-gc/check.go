// Copyright 2020 Brad Fitzpatrick. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build go1.21
// +build go1.21

package assume_no_moving_gc

import (
	"os"
	_ "unsafe"
)

//go:linkname heapObjectsCanMove runtime.heapObjectsCanMove
func heapObjectsCanMove() bool

func init() {
	if !heapObjectsCanMove() {
		// The unsafe assumptions made by the package
		// importing this package still hold. All's good. (at
		// least unless they made other assumption this
		// package doesn't concern itself with)
		return
	}
	if os.Getenv(env) == "play-with-fire" {
		return
	}
	panic(`
Something in this program imports go4.org/unsafe/assume-no-moving-gc to
declare that it assumes a non-moving garbage collector, but the version
of Go you're using declares that its heap objects can now move around.
This program is no longer safe. You should update your packages which import
go4.org/unsafe/assume-no-moving-gc. To risk it and bypass this check, set
ASSUME_NO_MOVING_GC_UNSAFE=play-with-fire and cross your fingers.`)
}
