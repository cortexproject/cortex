package cortexpb

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

// Inline and byte-free variant of hash/fnv's fnv64a.
// Ref: https://github.com/prometheus/common/blob/main/model/fnv.go

func LabelsToFingerprint(lset labels.Labels) model.Fingerprint {
	if len(lset) == 0 {
		return model.Fingerprint(hashNew())
	}

	sum := hashNew()
	lset.Range(func(l labels.Label) {
		sum = hashAdd(sum, string(l.Name))
		sum = hashAddByte(sum, model.SeparatorByte)
		sum = hashAdd(sum, string(l.Value))
		sum = hashAddByte(sum, model.SeparatorByte)
	})
	return model.Fingerprint(sum)
}

const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
)

// hashNew initializes a new fnv64a hash value.
func hashNew() uint64 {
	return offset64
}

// hashAdd adds a string to a fnv64a hash value, returning the updated hash.
func hashAdd(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

// hashAddByte adds a byte to a fnv64a hash value, returning the updated hash.
func hashAddByte(h uint64, b byte) uint64 {
	h ^= uint64(b)
	h *= prime64
	return h
}
