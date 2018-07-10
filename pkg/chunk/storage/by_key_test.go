package storage

import (
	"github.com/weaveworks/cortex/pkg/chunk"
)

// ByKey allow you to sort chunks by ID
type ByKey []chunk.Chunk

func (cs ByKey) Len() int           { return len(cs) }
func (cs ByKey) Swap(i, j int)      { cs[i], cs[j] = cs[j], cs[i] }
func (cs ByKey) Less(i, j int) bool { return lessByKey(cs[i], cs[j]) }

// This comparison uses all the same information as Chunk.ExternalKey()
func lessByKey(a, b chunk.Chunk) bool {
	return a.UserID < b.UserID ||
		(a.UserID == b.UserID && (a.Fingerprint < b.Fingerprint ||
			(a.Fingerprint == b.Fingerprint && (a.From < b.From ||
				(a.From == b.From && (a.Through < b.Through ||
					(a.Through == b.Through && a.Checksum < b.Checksum)))))))
}
