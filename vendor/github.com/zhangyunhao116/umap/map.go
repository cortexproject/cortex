package umap

import (
	"unsafe"
)

const (
	emptySlot   uint8 = 0b1111_1111
	deletedSlot uint8 = 0b1000_0000

	msbs uint64 = 0x8080_8080_8080_8080
	lsbs uint64 = 0x0101_0101_0101_0101

	allEmpty uint64 = uint64(emptySlot) + uint64(emptySlot)<<8 + uint64(emptySlot)<<16 +
		uint64(emptySlot)<<24 + uint64(emptySlot)<<32 + uint64(emptySlot)<<40 +
		uint64(emptySlot)<<48 + uint64(emptySlot)<<56

	// LoadFactor.
	// 0 -> invalid
	// 1 -> 0.5(min)
	// 2 -> 0.75
	// 3 -> 0.875(max)
	// 4 -> 0.9375, invalid if bucketCnt=8
	loadFactorShift   = 3
	emptyItemInBucket = bucketCnt >> loadFactorShift // used for optimazation.
	maxItemInBucket   = bucketCnt - (bucketCnt >> loadFactorShift)
)

type Uint64Map struct {
	count      int
	growthLeft int
	bucketmask uint64
	buckets    unsafe.Pointer
}

// bmapuint64 represents a bucket.
type bmapuint64 struct {
	tophash [bucketCnt]uint8
	data    [bucketCnt]uint64kv
}

type uint64kv struct {
	key   uint64
	value uint64
}

// New64 returns an empty Uint64Map.
//
// If length <= 0, it returns an empty map(including a pre-allocated bucket).
func New64(length int) *Uint64Map {
	// Make sure the compiler can inline New64(cost < 80).
	bucketnum := uint(1)

	if length >= bucketCnt {
		minBucket := uint(nextPowerOfTwo(length) / bucketCnt)
		// Make sure (capcity * LoadFactor) >= length.
		if minBucket*maxItemInBucket < uint(length) {
			minBucket *= 2
		}
		bucketnum = minBucket
	}

	x := make([]bmapuint64, bucketnum)
	for i := range x {
		*(*uint64)(unsafe.Pointer(&x[i].tophash)) = allEmpty
	}

	return &Uint64Map{
		buckets:    (*sliceHeader)(unsafe.Pointer(&x)).Data,
		bucketmask: uint64(bucketnum) - 1,
		growthLeft: int(bucketnum * bucketCnt),
	}
}

func (h *Uint64Map) Load(key uint64) (value uint64, ok bool) {
	hashres := hashUint64(uint64(key))
	tophash := tophash(hashres)

	indexMask := uint64(h.bucketmask)
	index := hashres & indexMask
	indexStride := uint64(0)

	for {
		bucket := bmapPointer(h.buckets, uint(index))
		status := matchTopHash(bucket.tophash, tophash)
		for {
			sloti := status.NextMatch()
			if sloti >= bucketCnt {
				break
			}
			if bucket.data[sloti].key == key {
				return bucket.data[sloti].value, true
			}
			status.RemoveLowestBit()
		}
		if bucket.MatchEmpty().AnyMatch() {
			return
		}
		// Update index, go to next bucket.
		indexStride += 1
		index += indexStride
		index &= indexMask
	}
}

// Store sets the value for a key.
func (h *Uint64Map) Store(key uint64, value uint64) {
	if h.needGrow() {
		h.growWork()
	}

	hashres := hashUint64(uint64(key))
	tophash := tophash(hashres)

	indexMask := uint64(h.bucketmask)
	index := hashres & indexMask
	indexStride := uint64(0)

	var (
		bucket *bmapuint64
		status bitmask64
	)
	// Check if the key is in the map.
	for {
		bucket = bmapPointer(h.buckets, uint(index))
		status = matchTopHash(bucket.tophash, tophash)
		for {
			sloti := status.NextMatch()
			if sloti >= bucketCnt {
				break
			}
			if bucket.data[sloti].key == key {
				// Found.
				bucket.data[sloti].value = value
				return
			}
			status.RemoveLowestBit()
		}
		if bucket.MatchEmpty().AnyMatch() {
			break
		}
		// Update index, go to next bucket.
		indexStride += 1
		index += indexStride
		index &= indexMask
	}

	// The key is not in the map.
	index = hashres & indexMask
	indexStride = 0
	for {
		bucket = bmapPointer(h.buckets, uint(index))
		// Can't find the key in this bucket.
		// Check empty slot or deleted slot.
		status = bucket.MatchEmptyOrDeleted()
		sloti := status.NextMatch()
		if sloti < bucketCnt {
			bucket.tophash[sloti] = tophash
			bucket.data[sloti].key = key
			bucket.data[sloti].value = value
			h.growthLeft -= 1
			h.count += 1
			return
		}
		// No idle slot
		// Update index, go to next bucket.
		indexStride += 1
		index += indexStride
		index &= indexMask
	}
}

// storeWithoutGrow inserts values into the hashmap without growing.
// This function also assumes that
// - There are always enough empty slots in the hashmap.
// - No deleted slots in the hashmap. (not used for now)
func (h *Uint64Map) storeWithoutGrow(key uint64, value uint64) {
	hashres := hashUint64(uint64(key))
	tophash := tophash(hashres)

	indexMask := uint64(h.bucketmask)
	index := hashres & indexMask
	indexStride := uint64(0)

	for {
		bucket := bmapPointer(h.buckets, uint(index))
		// Just checking the empty slot is fine, but using this function is faster.
		status := bucket.MatchEmptyOrDeleted()
		sloti := status.NextMatch()
		if sloti < bucketCnt {
			bucket.tophash[sloti] = tophash
			bucket.data[sloti].key = key
			bucket.data[sloti].value = value
			return
		}
		// No idle slot, update index then go to next bucket.
		indexStride += 1
		index += indexStride
		index &= indexMask
	}
}

// Delete deletes the value for a key.
func (h *Uint64Map) Delete(key uint64) {
	hashres := hashUint64(uint64(key))
	tophash := tophash(hashres)

	indexMask := uint64(h.bucketmask)
	index := hashres & indexMask
	indexStride := uint64(0)

	for {
		bucket := bmapPointer(h.buckets, uint(index))
		status := matchTopHash(bucket.tophash, tophash)
		for {
			sloti := status.NextMatch()
			if sloti >= bucketCnt {
				break
			}
			if bucket.data[sloti].key == key {
				// Found this key.
				h.count -= 1
				if !bucket.MatchEmpty().AnyMatch() {
					bucket.tophash[sloti] = deletedSlot
				} else {
					h.growthLeft += 1
					bucket.tophash[sloti] = emptySlot
				}
				return
			}
			status.RemoveLowestBit()
		}
		if bucket.MatchEmpty().AnyMatch() {
			// The key is not in this map.
			return
		}
		// Update index, go to next bucket.
		indexStride += 1
		index += indexStride
		index &= indexMask
	}
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration.
func (h *Uint64Map) Range(f func(k uint64, v uint64) bool) {
	initBuckets := h.buckets
	bucketNum := int(h.bucketmask + 1)

	indexMask := uint64(h.bucketmask)
	index := uint64(0)
	indexStride := uint64(0)

	for indexStride < uint64(bucketNum) {
		bucket := bmapPointer(initBuckets, uint(index))
		for sloti := 0; sloti < bucketCnt; sloti++ {
			if isDeletedOrEmpty(bucket.tophash[sloti]) {
				continue
			}
			kv := bucket.data[sloti]
			key := kv.key
			value := kv.value
			if !f(key, value) {
				return
			}
		}
		// Update index, go to next bucket.
		indexStride += 1
		index += indexStride
		index &= indexMask
	}
}

// Len returns the length of the hashmap.
func (h *Uint64Map) Len() int {
	return int(h.count)
}

func (h *Uint64Map) growWork() {
	oldCap := (h.bucketmask + 1) * bucketCnt
	if uint64(h.count) < oldCap>>1 {
		// Too many slots are locked by deleted items.
		h.sameSizeGrow()
	} else {
		h.grow()
	}
}

func (h *Uint64Map) sameSizeGrow() {
	// Algorithm:
	// - mark all DELETED slots as EMPTY
	// - mark all FULL slots as DELETED
	// - for each slot marked as DELETED
	//     hash = Hash(element)
	//     target = find_first_non_full(hash)
	//     if target is in the same group
	//       mark slot as FULL
	//     else if target is EMPTY
	//       transfer element to target
	//       mark slot as EMPTY
	//       mark target as FULL
	//     else if target is DELETED
	//       swap current element with target element
	//       mark target as FULL
	//       repeat procedure for current slot with moved from element (target)
	// - hashmap grow left = cap - items

	bucketNum := uint(h.bucketmask + 1)
	for index := uint(0); index < bucketNum; index++ {
		bucket := bmapPointer(h.buckets, uint(index))
		bucket.PrepareSameSizeGrow()
	}
	for index := uint(0); index < bucketNum; index++ {
		bucket := bmapPointer(h.buckets, uint(index))

		for sloti := uint(0); sloti < bucketCnt; {
			if bucket.tophash[sloti] != deletedSlot {
				sloti += 1
				continue
			}
			slotHashRes := hashUint64(uint64(bucket.data[sloti].key))
			tophash := tophash(slotHashRes)
			targetBucket, targetBucketIdx, targetSlotIdx := h.findFirstNotNull(slotHashRes)
			if targetBucketIdx == index {
				// Just mark the slot as FULL.
				bucket.tophash[sloti] = tophash
				sloti += 1
				continue
			}
			switch targetBucket.tophash[targetSlotIdx] {
			case emptySlot:
				// 1. Transfer element to target
				// 2. Mark target as FULL
				// 3. Mark slot as EMPTY
				targetBucket.data[targetSlotIdx].key = bucket.data[sloti].key
				targetBucket.data[targetSlotIdx].value = bucket.data[sloti].value
				targetBucket.tophash[targetSlotIdx] = tophash
				bucket.tophash[sloti] = emptySlot
				sloti++
			case deletedSlot:
				// 1. Swap current element with target element
				// 2. Mark target as FULL
				// 3. Repeat procedure for current slot with moved from element (target)
				targetBucket.data[targetSlotIdx].key, bucket.data[sloti].key = bucket.data[sloti].key, targetBucket.data[targetSlotIdx].key
				targetBucket.data[targetSlotIdx].value, bucket.data[sloti].value = bucket.data[sloti].value, targetBucket.data[targetSlotIdx].value
				targetBucket.tophash[targetSlotIdx] = tophash
			}
		}
	}
	h.growthLeft = int(bucketNum*bucketCnt) - h.count
}

func (h *Uint64Map) findFirstNotNull(hashres uint64) (bucket *bmapuint64, bucketi, sloti uint) {
	indexMask := uint64(h.bucketmask)
	index := hashres & indexMask
	indexStride := uint64(0)

	for {
		bucket := bmapPointer(h.buckets, uint(index))
		status := bucket.MatchEmptyOrDeleted()
		sloti := status.NextMatch()
		if sloti < bucketCnt {
			return bucket, uint(index), uint(sloti)
		}
		// Update index, go to next bucket.
		indexStride += 1
		index += indexStride
		index &= indexMask
	}
}

func (h *Uint64Map) grow() {
	oldBucketnum := h.bucketmask + 1
	newBucketnum := oldBucketnum * 2
	newBucketMask := newBucketnum - 1
	newCap := newBucketnum * bucketCnt
	newMap := &Uint64Map{
		buckets:    makeUint64BucketArray(int(newBucketnum)),
		bucketmask: newBucketMask,
	}

	for index := uint64(0); index < oldBucketnum; index++ {
		bucket := bmapPointer(h.buckets, uint(index))
		for i := 0; i < bucketCnt; i++ {
			if isFull(bucket.tophash[i]) {
				kv := bucket.data[i]
				newMap.storeWithoutGrow(kv.key, kv.value)
			}
		}
	}

	h.bucketmask = newBucketMask
	// h.items is the same.
	h.buckets = newMap.buckets
	h.growthLeft = int(newCap) - h.count
}

func (h *Uint64Map) needGrow() bool {
	return h.growthLeft <= growThresholdUint64(h.bucketmask)
}

func tophash(v uint64) uint8 {
	return uint8(v >> 57)
}

// isDeletedOrEmpty returns true if the slot is deleted or empty.
func isDeletedOrEmpty(v uint8) bool {
	return v >= 128
}

// isFull returns true if the slot is full.
func isFull(v uint8) bool {
	return v < 128
}
