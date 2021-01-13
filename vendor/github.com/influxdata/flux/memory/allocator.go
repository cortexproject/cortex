package memory

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow/go/arrow/memory"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
)

// DefaultAllocator is the default memory allocator for Flux.
//
// This implements the memory.Allocator interface from arrow.
var DefaultAllocator = memory.DefaultAllocator

var _ memory.Allocator = (*Allocator)(nil)

// Allocator tracks the amount of memory being consumed by a query.
type Allocator struct {
	// Variables accessed with atomic operations should be at
	// the beginning of the struct to ensure byte alignment is correct.
	// https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	allocationLimit int64
	bytesAllocated  int64
	maxAllocated    int64
	totalAllocated  int64
	mu              sync.Mutex

	// Limit is the limit on the amount of memory that this allocator
	// can assign. If this is null, there is no limit.
	Limit *int64

	// Manager holds the Manager for this Allocator.
	// If this Allocator has a limit set and the limit is to be exceeded,
	// it will attempt to use the Manager to request more memory.
	// If this fails, then the Allocator will panic.
	Manager Manager

	// Allocator is the underlying memory allocator used to
	// allocate and free memory.
	// If this is unset, the DefaultAllocator is used.
	Allocator memory.Allocator
}

// Allocate will ensure that the requested memory is available and
// record that it is in use.
func (a *Allocator) Allocate(size int) []byte {
	if a == nil {
		return DefaultAllocator.Allocate(size)
	}

	if size < 0 {
		panic(errors.New(codes.Internal, "cannot allocate negative memory"))
	} else if size == 0 {
		return nil
	}

	// Account for the size requested.
	if err := a.count(size); err != nil {
		panic(err)
	}

	// Allocate the amount of memory.
	// TODO(jsternberg): It's technically possible for this to allocate
	// more memory than we requested. How do we deal with that since we
	// likely want to use that feature?
	alloc := a.allocator()
	return alloc.Allocate(size)
}

func (a *Allocator) Reallocate(size int, b []byte) []byte {
	if a == nil {
		return DefaultAllocator.Reallocate(size, b)
	}

	sizediff := size - cap(b)
	if err := a.Account(sizediff); err != nil {
		panic(err)
	}

	alloc := a.allocator()
	return alloc.Reallocate(size, b)
}

// Account will manually account for the amount of memory being used.
// This is typically used for memory that is allocated outside of the
// Allocator that must be recorded in some way.
func (a *Allocator) Account(size int) error {
	if size == 0 {
		return nil
	}
	return a.count(size)
}

// Allocated returns the amount of currently allocated memory.
func (a *Allocator) Allocated() int64 {
	return atomic.LoadInt64(&a.bytesAllocated)
}

// MaxAllocated reports the maximum amount of allocated memory at any point in the query.
func (a *Allocator) MaxAllocated() int64 {
	return atomic.LoadInt64(&a.maxAllocated)
}

// TotalAllocated reports the total amount of memory allocated.
// It counts all memory that was allocated at any time even if it
// was released.
func (a *Allocator) TotalAllocated() int64 {
	return atomic.LoadInt64(&a.totalAllocated)
}

// Free will reduce the amount of memory used by this Allocator.
// In general, memory should be freed using the Reference returned
// by Allocate. Not all code is capable of using this though so this
// method provides a low-level way of releasing the memory without
// using a Reference.
// Free will release the memory associated with the byte slice.
func (a *Allocator) Free(b []byte) {
	if a == nil {
		DefaultAllocator.Free(b)
		return
	}

	size := len(b)

	// Release the memory to the allocator first.
	alloc := a.allocator()
	alloc.Free(b)

	// Release the memory in our accounting.
	atomic.AddInt64(&a.bytesAllocated, int64(-size))
}

func (a *Allocator) count(size int) error {
	var c int64
	if a.Limit != nil {
		// We need to load the current bytes allocated, add to it, and
		// compare if it is greater than the limit. If it is not, we need
		// to modify the bytes allocated.
		for {
			allocated := atomic.LoadInt64(&a.bytesAllocated)
			limit := atomic.LoadInt64(&a.allocationLimit)
			if want := allocated + int64(size); want > limit {
				if err := a.requestMemory(allocated, want); err != nil {
					return err
				}
				// The request for additional memory succeeded so try again.
			} else if atomic.CompareAndSwapInt64(&a.bytesAllocated, allocated, want) {
				c = want
				break
			}
			// We did not succeed at swapping the bytes allocated so try again.
		}
	} else {
		// Otherwise, add the size directly to the bytes allocated and
		// compare and swap to modify the max allocated.
		c = atomic.AddInt64(&a.bytesAllocated, int64(size))
	}

	// Increment the total allocated if the amount is positive. This counter
	// will only increment.
	if size > 0 {
		atomic.AddInt64(&a.totalAllocated, int64(size))
	}

	// Modify the max allocated if the amount we just allocated is greater.
	for max := atomic.LoadInt64(&a.maxAllocated); c > max; max = atomic.LoadInt64(&a.maxAllocated) {
		if atomic.CompareAndSwapInt64(&a.maxAllocated, max, c) {
			break
		}
	}
	return nil
}

func (a *Allocator) requestMemory(allocated, want int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Confirm that we still need to request more memory.
	// This is because we did the initial check outside of the lock.
	// This also acts as the way to initialize the allocation limit.
	if want <= *a.Limit {
		atomic.StoreInt64(&a.allocationLimit, *a.Limit)
		return nil
	}

	// If we do not have a memory manager, then there is no
	// way to increase our allocation limit.
	if a.Manager != nil {
		// Request that additional memory is needed from the manager.
		need := want - *a.Limit
		n, err := a.Manager.RequestMemory(need)
		if err == nil {
			// Increase the limit by the amount the manager gave us.
			*a.Limit += n
			atomic.StoreInt64(&a.allocationLimit, *a.Limit)
			return nil
		}
		// Ignore the error. We use our own custom one so we just
		// needed to know it failed.
	}
	return errors.Wrap(LimitExceededError{
		Limit:     *a.Limit,
		Allocated: allocated,
		Wanted:    want - allocated,
	}, codes.ResourceExhausted)
}

// allocator returns the underlying memory.Allocator that should be used.
func (a *Allocator) allocator() memory.Allocator {
	if a.Allocator == nil {
		return DefaultAllocator
	}
	return a.Allocator
}

// Manager will manage the memory allowed for the Allocator.
// The Allocator may use the Manager to request additional memory or to
// give back memory that is currently in use by the Allocator
// when/if it is no longer needed.
type Manager interface {
	// RequestMemory will request that the given amount of memory
	// be reserved for the caller. The manager will return the number
	// of bytes that were successfully reserved. The n value will be
	// either equal to or greater than the requested number of bytes.
	// If the manager cannot reserve at least bytes in memory, then
	// it will return an error with the details.
	RequestMemory(want int64) (got int64, err error)

	// FreeMemory will inform the memory manager that this memory
	// is not being used anymore.
	// It is not required for this to be called similar to how
	// it is not necessary for a program to free the memory.
	// It is the responsibility of the manager itself to identify
	// when this allocator is not used anymore and to reclaim any
	// unfreed memory when the resource is dead.
	FreeMemory(bytes int64)
}

// LimitExceededError is an error when the allocation limit is exceeded.
type LimitExceededError struct {
	Limit     int64
	Allocated int64
	Wanted    int64
}

func (a LimitExceededError) Error() string {
	return fmt.Sprintf("memory allocation limit reached: limit %d bytes, allocated: %d, wanted: %d", a.Limit, a.Allocated, a.Wanted)
}
