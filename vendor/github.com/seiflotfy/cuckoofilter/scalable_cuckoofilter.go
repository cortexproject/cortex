package cuckoo

import (
	"bytes"
	"encoding/gob"
)

const (
	DefaultLoadFactor = 0.9
	DefaultCapacity   = 10000
)

type ScalableCuckooFilter struct {
	filters    []*Filter
	loadFactor float32
	//when scale(last filter size * loadFactor >= capacity) get new filter capacity
	scaleFactor func(capacity uint) uint
}

type option func(*ScalableCuckooFilter)

type Store struct {
	Bytes      [][]byte
	LoadFactor float32
}

/*
 by default option the grow capacity is:
 capacity , total
 4096  4096
 8192  12288
16384  28672
32768  61440
65536  126,976
*/
func NewScalableCuckooFilter(opts ...option) *ScalableCuckooFilter {
	sfilter := new(ScalableCuckooFilter)
	for _, opt := range opts {
		opt(sfilter)
	}
	configure(sfilter)
	return sfilter
}

func (sf *ScalableCuckooFilter) Lookup(data []byte) bool {
	for _, filter := range sf.filters {
		if filter.Lookup(data) {
			return true
		}
	}
	return false
}

func (sf *ScalableCuckooFilter) Reset() {
	for _, filter := range sf.filters {
		filter.Reset()
	}
}

func (sf *ScalableCuckooFilter) Insert(data []byte) bool {
	needScale := false
	lastFilter := sf.filters[len(sf.filters)-1]
	if (float32(lastFilter.count) / float32(len(lastFilter.buckets))) > sf.loadFactor {
		needScale = true
	} else {
		b := lastFilter.Insert(data)
		needScale = !b
	}
	if !needScale {
		return true
	}
	newFilter := NewFilter(sf.scaleFactor(uint(len(lastFilter.buckets))))
	sf.filters = append(sf.filters, newFilter)
	return newFilter.Insert(data)
}

func (sf *ScalableCuckooFilter) InsertUnique(data []byte) bool {
	if sf.Lookup(data) {
		return false
	}
	return sf.Insert(data)
}

func (sf *ScalableCuckooFilter) Delete(data []byte) bool {
	for _, filter := range sf.filters {
		if filter.Delete(data) {
			return true
		}
	}
	return false
}

func (sf *ScalableCuckooFilter) Count() uint {
	var sum uint
	for _, filter := range sf.filters {
		sum += filter.count
	}
	return sum

}

func (sf *ScalableCuckooFilter) Encode() []byte {
	slice := make([][]byte, len(sf.filters))
	for i, filter := range sf.filters {
		encode := filter.Encode()
		slice[i] = encode
	}
	store := &Store{
		Bytes:      slice,
		LoadFactor: sf.loadFactor,
	}
	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(store)
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

func (sf *ScalableCuckooFilter) DecodeWithParam(fBytes []byte, opts ...option) (*ScalableCuckooFilter, error) {
	instance, err := DecodeScalableFilter(fBytes)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt(instance)
	}
	return instance, nil
}

func DecodeScalableFilter(fBytes []byte) (*ScalableCuckooFilter, error) {
	buf := bytes.NewBuffer(fBytes)
	dec := gob.NewDecoder(buf)
	store := &Store{}
	err := dec.Decode(store)
	if err != nil {
		return nil, err
	}
	filterSize := len(store.Bytes)
	instance := NewScalableCuckooFilter(func(filter *ScalableCuckooFilter) {
		filter.filters = make([]*Filter, filterSize)
	}, func(filter *ScalableCuckooFilter) {
		filter.loadFactor = store.LoadFactor
	})
	for i, oneBytes := range store.Bytes {
		filter, err := Decode(oneBytes)
		if err != nil {
			return nil, err
		}
		instance.filters[i] = filter
	}
	return instance, nil

}

func configure(sfilter *ScalableCuckooFilter) {
	if sfilter.loadFactor == 0 {
		sfilter.loadFactor = DefaultLoadFactor
	}
	if sfilter.scaleFactor == nil {
		sfilter.scaleFactor = func(currentSize uint) uint {
			return currentSize * bucketSize * 2
		}
	}
	if sfilter.filters == nil {
		initFilter := NewFilter(DefaultCapacity)
		sfilter.filters = []*Filter{initFilter}
	}
}
