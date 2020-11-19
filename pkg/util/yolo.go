package util

import "unsafe"

func YoloBuf(s string) []byte {
	return *((*[]byte)(unsafe.Pointer(&s)))
}

func YoloString(buf []byte) string {
	return *((*string)(unsafe.Pointer(&buf)))
}
