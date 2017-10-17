package bits

import (
	"testing"
	"testing/quick"
)

func testQuick(t *testing.T, which string, ffast, fslow func(x uint64) uint64) {
	f := func(x uint64) bool {
		return ffast(x) == fslow(x)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Errorf("fast%v != slow%v: %v: ", which, which, err)
	}
}

func ctzSlow(x uint64) uint64 {
	var n uint64
	for x&1 == 0 {
		n++
		x >>= 1
	}
	return n
}

func TestQuickCtz(t *testing.T) { testQuick(t, "ctz", Ctz, ctzSlow) }

func clzSlow(x uint64) uint64 {
	var n uint64
	for x&0x8000000000000000 == 0 {
		n++
		x <<= 1
	}
	return n
}

func TestQuickClz(t *testing.T) { testQuick(t, "clz", Clz, clzSlow) }

func popcntSlow(x uint64) uint64 {
	var n uint64
	for x != 0 {
		if x&1 == 1 {
			n++
		}
		x >>= 1
	}
	return n
}

func TestQuickPopcnt(t *testing.T) { testQuick(t, "popcnt", Popcnt, popcntSlow) }
