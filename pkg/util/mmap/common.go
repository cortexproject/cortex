package mmap

const (
	// RDONLY maps the memory read-only.
	// Attempts to write to the MMap object will result in undefined behavior.
	READ = 0

	// RDWR maps the memory as read-write. Writes to the MMap object will update the
	// underlying file.
	WRITE = 1 << iota

	// Write changes back to original file
	SHARED = 0
)

type MMap []byte
