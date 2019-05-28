// Package mmap implements a portable mmap interface which works on UNIX alikes and especially windows.
// common.go:   used by all platforms
// unix.go:     basic wrapper around unix.Mmap()
// windows.go:  same API as the unix one, but entirely different because of .. Windows
package mmap

const (
	// READ maps the memory read-only.
	// Attempts to write to the MMap object will result in undefined behavior.
	READ = 0

	// WRITE maps the memory as read-write. Writes to the MMap object will update the
	// underlying file.
	WRITE = 1 << iota

	// SHARED makes changes be written back to original file
	SHARED = 0
)
