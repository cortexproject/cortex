// +build windows

// This code is taken from
// https://github.com/edsrzf/mmap-go/blob/master/mmap_windows.go
// and has been slightly refactored maintain API compatibility with unix.Mmap()
// Originally licensed under BSD3 license, all credits belong to the original author Evan Shaw.

package mmap

import (
	"errors"
	"os"
	"reflect"
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"
)

type addrinfo struct {
	file     windows.Handle
	mapview  windows.Handle
	writable bool
}

var handleLock sync.Mutex
var handleMap = map[uintptr]*addrinfo{}

func Mmap(fd uintptr, offset int64, length int, inprot, inflags int) ([]byte, error) {
	prot := uint32(windows.PAGE_READONLY)
	flags := uint32(windows.FILE_MAP_READ)
	writable := false

	if prot&WRITE != 0 {
		prot = windows.PAGE_READWRITE
		flags = windows.FILE_MAP_WRITE
		writable = true
	}

	maxSizeHigh := uint32((offset + int64(length)) >> 32)
	maxSizeLow := uint32((offset + int64(length)) & 0xFFFFFFFF)

	handle, errno := windows.CreateFileMapping(windows.Handle(fd), nil, prot, maxSizeHigh, maxSizeLow, nil)
	if handle == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	fileOffsetHigh := uint32(offset >> 32)
	fileOffsetLow := uint32(offset & 0xFFFFFFFF)

	addr, errno := windows.MapViewOfFile(handle, flags, fileOffsetHigh, fileOffsetLow, uintptr(length))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}

	handleLock.Lock()
	handleMap[addr] = &addrinfo{
		file:     windows.Handle(fd),
		mapview:  handle,
		writable: writable,
	}
	handleLock.Unlock()

	m := MMap{}
	dh := m.header()
	dh.Data = addr
	dh.Len = length
	dh.Cap = length

	return m, nil
}

func (m *MMap) header() *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(m))
}

func (m *MMap) addrLen() (uintptr, uintptr) {
	header := m.header()
	return header.Data, uintptr(header.Len)
}

func (m MMap) flush() error {
	addr, len := m.addrLen()
	errno := windows.FlushViewOfFile(addr, len)
	if errno != nil {
		return os.NewSyscallError("FlushViewOfFile", errno)
	}

	handleLock.Lock()
	defer handleLock.Unlock()
	handle, ok := handleMap[addr]
	if !ok {
		// should be impossible; we would've errored above
		return errors.New("unknown base address")
	}

	if handle.writable {
		if err := windows.FlushFileBuffers(handle.file); err != nil {
			return os.NewSyscallError("FlushFileBuffers", err)
		}
	}

	return nil
}

func Unmap(buf MMap) error {
	if err := buf.flush(); err != nil {
		return err
	}

	addr := buf.header().Data
	handleLock.Lock()
	defer handleLock.Unlock()

	if err := windows.UnmapViewOfFile(addr); err != nil {
		return err
	}

	handle, ok := handleMap[addr]
	if !ok {
		return errors.New("unknown base address")
	}
	delete(handleMap, addr)

	err := windows.CloseHandle(windows.Handle(handle.mapview))
	return os.NewSyscallError("CloseHandle", err)
}
