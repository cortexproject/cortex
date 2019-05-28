// +build darwin dragonfly freebsd linux openbsd solaris netbsd

package mmap

import (
	"errors"

	"golang.org/x/sys/unix"
)

// Mmap creates a new mmap
func Mmap(fd uintptr, offset int64, length int, inprot, inflags int) ([]byte, error) {
	if inflags != SHARED {
		return nil, errors.New("flags other than mmap.SHARED are not implemented")
	}
	flags := unix.MAP_SHARED

	prot := unix.PROT_READ
	if inprot&WRITE != 0 {
		prot |= unix.PROT_WRITE
	}

	buf, err := unix.Mmap(int(fd), offset, length, prot, flags)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// Unmap removes the mmap
func Unmap(buf []byte) error {
	return unix.Munmap(buf)
}
