package storage

import (
	"bytes"
	"encoding/binary"
)

func EncodeStringSlice(s []string) []byte {
	b := new(bytes.Buffer)
	for i := 0; i < len(s); i++ {
		b.WriteString(s[i])
		b.WriteByte(0)
	}

	return b.Bytes()
}

func DecodeStringSlice(b []byte) ([]string, error) {
	buffer := bytes.NewBuffer(b)
	r := make([]string, 0, 10)

	for {
		s, err := buffer.ReadString(0)
		if err != nil {
			break
		}
		r = append(r, s[:len(s)-1])
	}

	return r, nil
}

func EncodeUintSlice(s []uint64) []byte {
	l := make([]byte, binary.MaxVarintLen32)
	r := make([]byte, 0, len(s)*binary.MaxVarintLen32)

	// size
	n := binary.PutUvarint(l[:], uint64(len(s)))
	r = append(r, l[:n]...)

	for i := 0; i < len(s); i++ {
		n := binary.PutUvarint(l[:], s[i])
		r = append(r, l[:n]...)
	}

	return r
}

func DecodeUintSlice(b []byte) ([]uint64, error) {
	buffer := bytes.NewBuffer(b)

	// size
	s, err := binary.ReadUvarint(buffer)

	if err != nil {
		return nil, err
	}

	r := make([]uint64, 0, s)

	for i := uint64(0); i < s; i++ {
		v, err := binary.ReadUvarint(buffer)

		if err != nil {
			return nil, err
		}
		r = append(r, v)
	}

	return r, nil
}
