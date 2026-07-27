// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	enc "encoding/binary"
	"hash/fnv"
	"io"
)

// NodeFingerprint returns a deterministic 64-bit fingerprint for the subtree
// rooted at n. Structurally identical subtrees always produce the same value.
func NodeFingerprint(n Node) uint64 {
	h := fnv.New64a()
	data, err := Marshal(n)
	if err != nil {
		// Marshal does not support distributed-execution nodes because they hold
		// runtime Engine references. Fall back to type + String() + children.
		_, _ = io.WriteString(h, string(n.Type()))
		_, _ = io.WriteString(h, n.String())
		for _, child := range n.Children() {
			if child == nil || *child == nil {
				continue
			}
			var buf [8]byte
			enc.LittleEndian.PutUint64(buf[:], NodeFingerprint(*child))
			h.Write(buf[:])
		}
	} else {
		h.Write(data)
	}
	return h.Sum64()
}
