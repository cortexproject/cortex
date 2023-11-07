# umap

umap is a fast, reliable, simple hashmap that only supports the uint64 key/value pair. It is faster than the runtime hashmap in most cases.

The umap is mainly based on the SwissTable(https://github.com/abseil/abseil-cpp/blob/master/absl/container/internal/raw_hash_set.h), it is portable for all platforms and doesn't require SSE or AVX.

## QuickStart

```golang
package main

import "github.com/zhangyunhao116/umap"

func main() {
	m := umap.New64(0)
	m.Store(1, 2)
	m.Store(3, 4)
	m.Range(func(k, v uint64) bool {
		println("range found", k, v)
		return true
	})
	m.Delete(1)
	println("map length", m.Len())
	println(m.Load(1))
}

```

## Goals

- Fast, reliable, and simple.



## Non-goals

- Support other key/value types. Other types require more complex logic, if you need a fast map that supports any type, see https://github.com/golang/go/issues/54766
- Panic in some illegal cases. umap disables all these features, such as panic when multiple goroutines insert items. It's the user's responsibility to check that.
- Concurrent safe. A mutex is required if you need this feature.



## Compared to map[uint64]uint64

The umap is compatible with map[uint64]uint64 for most cases, except:

- DO NOT support inserting items during iterating the map.

> Inserting items during iterating the runtime map is undefined behavior for most cases, compatible with this feature requires lots of work, so we ban this feature in the umap. **But deleting items during iterating the map is fine.**
>
> i.e. This code prints different numbers each time.
>
> ```golang
> func main() {
> 	m := make(map[int]int, 0)
> 	for i := 0; i < 100; i++ {
> 		m[i] = i
> 	}
> 	j := 1000
> 	for k, _ := range m {
> 		m[j+k] = j
> 		j++
> 	}
> 	println(len(m))
> }
> ```
>
> It means that the code below is INVALID! Although it will not panic.
>
> ```golang
> 	m.Range(func(key, value uint64) bool {
> 		m.Store(key+100, value)
> 		return true
> 	})
> ```
>
> 



## Benchmark

Go version: go version devel go1.20-a813be86df

CPU: Intel 11700k

OS: 22.04.1 LTS (Jammy Jellyfish)

MEMORY: 16G x 2 (3200MHz)

```
name                            old time/op  new time/op   delta
MapAccessHit/Uint64/6-16        3.68ns ± 0%   2.89ns ± 1%   -21.48%  (p=0.000 n=10+10)
MapAccessHit/Uint64/12-16       5.22ns ± 6%   2.88ns ± 1%   -44.81%  (p=0.000 n=10+10)
MapAccessHit/Uint64/18-16       5.39ns ±13%   2.89ns ± 0%   -46.42%  (p=0.000 n=9+10)
MapAccessHit/Uint64/24-16       5.24ns ± 5%   2.89ns ± 1%   -44.87%  (p=0.000 n=10+10)
MapAccessHit/Uint64/30-16       4.61ns ± 6%   2.89ns ± 1%   -37.27%  (p=0.000 n=10+10)
MapAccessHit/Uint64/64-16       4.76ns ± 2%   2.89ns ± 1%   -39.30%  (p=0.000 n=9+10)
MapAccessHit/Uint64/128-16      4.80ns ± 4%   2.89ns ± 1%   -39.92%  (p=0.000 n=10+10)
MapAccessHit/Uint64/256-16      4.78ns ± 3%   2.88ns ± 1%   -39.71%  (p=0.000 n=10+9)
MapAccessHit/Uint64/512-16      4.78ns ± 1%   2.89ns ± 1%   -39.59%  (p=0.000 n=10+9)
MapAccessHit/Uint64/1024-16     4.80ns ± 1%   2.89ns ± 1%   -39.76%  (p=0.000 n=9+10)
MapAccessHit/Uint64/2048-16     6.06ns ±12%   2.99ns ± 0%   -50.67%  (p=0.000 n=10+8)
MapAccessHit/Uint64/4096-16     11.2ns ± 2%    3.1ns ± 0%   -72.48%  (p=0.000 n=10+10)
MapAccessHit/Uint64/8192-16     13.0ns ± 1%    3.2ns ± 2%   -75.49%  (p=0.000 n=10+10)
MapAccessHit/Uint64/65536-16    16.9ns ± 0%    4.4ns ± 0%   -73.89%  (p=0.000 n=7+8)
MapRange/Uint64/6-16            48.7ns ± 1%   11.9ns ± 1%   -75.50%  (p=0.000 n=10+8)
MapRange/Uint64/12-16           84.2ns ± 1%   22.8ns ± 1%   -72.85%  (p=0.000 n=8+10)
MapRange/Uint64/18-16            133ns ± 2%     40ns ± 1%   -70.28%  (p=0.000 n=9+10)
MapRange/Uint64/24-16            160ns ± 3%     45ns ± 1%   -72.14%  (p=0.000 n=10+10)
MapRange/Uint64/30-16            214ns ± 2%     71ns ± 0%   -67.05%  (p=0.000 n=9+10)
MapRange/Uint64/64-16            424ns ± 4%    148ns ± 1%   -65.17%  (p=0.000 n=10+10)
MapRange/Uint64/128-16           827ns ± 4%    289ns ± 0%   -65.04%  (p=0.000 n=10+10)
MapRange/Uint64/256-16          1.72µs ± 3%   0.57µs ± 1%   -66.64%  (p=0.000 n=10+10)
MapRange/Uint64/512-16          3.78µs ± 3%   1.14µs ± 0%   -69.91%  (p=0.000 n=10+10)
MapRange/Uint64/1024-16         8.01µs ± 3%   2.27µs ± 0%   -71.64%  (p=0.000 n=10+10)
MapRange/Uint64/2048-16         16.4µs ± 2%    4.5µs ± 1%   -72.48%  (p=0.000 n=9+9)
MapRange/Uint64/4096-16         33.2µs ± 1%    9.1µs ± 0%   -72.69%  (p=0.000 n=10+8)
MapRange/Uint64/8192-16         66.8µs ± 2%   18.5µs ± 1%   -72.23%  (p=0.000 n=10+10)
MapRange/Uint64/65536-16         529µs ± 0%    183µs ± 2%   -65.42%  (p=0.000 n=8+10)
MapAssignGrow/Uint64/6-16       54.0ns ± 0%  209.3ns ±37%  +287.75%  (p=0.000 n=9+10)
MapAssignGrow/Uint64/12-16       629ns ±10%    544ns ±23%   -13.50%  (p=0.011 n=10+10)
MapAssignGrow/Uint64/18-16      1.75µs ±23%   1.16µs ±24%   -33.63%  (p=0.000 n=10+10)
MapAssignGrow/Uint64/24-16      1.79µs ±16%   1.28µs ±31%   -28.11%  (p=0.000 n=9+10)
MapAssignGrow/Uint64/30-16      3.66µs ±28%   1.98µs ±29%   -46.02%  (p=0.000 n=10+10)
MapAssignGrow/Uint64/64-16      9.36µs ±15%   4.31µs ±31%   -54.01%  (p=0.000 n=10+9)
MapAssignGrow/Uint64/128-16     18.7µs ± 8%    7.6µs ±40%   -59.46%  (p=0.000 n=9+10)
MapAssignGrow/Uint64/256-16     38.9µs ±16%    9.0µs ±25%   -76.90%  (p=0.000 n=9+9)
MapAssignGrow/Uint64/512-16     82.5µs ±16%   10.6µs ±46%   -87.13%  (p=0.000 n=10+9)
MapAssignGrow/Uint64/1024-16     137µs ±29%     17µs ± 1%   -87.43%  (p=0.000 n=10+8)
MapAssignGrow/Uint64/2048-16     292µs ±21%     45µs ±54%   -84.67%  (p=0.000 n=10+10)
MapAssignGrow/Uint64/4096-16     572µs ±17%    220µs ±58%   -61.47%  (p=0.000 n=10+10)
MapAssignGrow/Uint64/8192-16    1.15ms ±23%   0.47ms ±18%   -59.30%  (p=0.000 n=10+9)
MapAssignGrow/Uint64/65536-16   7.56ms ±13%   5.27ms ±15%   -30.28%  (p=0.000 n=10+10)
MapAssignReuse/Uint64/6-16       151ns ± 1%     92ns ± 0%   -38.85%  (p=0.000 n=10+10)
MapAssignReuse/Uint64/12-16      399ns ± 0%    163ns ± 1%   -59.19%  (p=0.000 n=10+10)
MapAssignReuse/Uint64/18-16      636ns ± 0%    214ns ± 1%   -66.39%  (p=0.000 n=9+10)
MapAssignReuse/Uint64/24-16      866ns ± 0%    296ns ± 1%   -65.85%  (p=0.000 n=9+10)
MapAssignReuse/Uint64/30-16     1.06µs ± 0%   0.36µs ± 0%   -66.28%  (p=0.000 n=10+10)
MapAssignReuse/Uint64/64-16     2.23µs ± 0%   0.76µs ± 0%   -65.90%  (p=0.000 n=10+10)
MapAssignReuse/Uint64/128-16    4.46µs ± 1%   1.51µs ± 0%   -66.04%  (p=0.000 n=10+8)
MapAssignReuse/Uint64/256-16    8.83µs ± 0%   3.02µs ± 0%   -65.75%  (p=0.000 n=9+8)
MapAssignReuse/Uint64/512-16    17.5µs ± 1%    6.1µs ± 0%   -65.47%  (p=0.000 n=10+9)
MapAssignReuse/Uint64/1024-16   35.1µs ± 0%   12.1µs ± 0%   -65.48%  (p=0.000 n=9+10)
MapAssignReuse/Uint64/2048-16   70.6µs ± 0%   24.6µs ± 0%   -65.18%  (p=0.000 n=9+9)
MapAssignReuse/Uint64/4096-16    143µs ± 1%     50µs ± 1%   -65.17%  (p=0.000 n=10+10)
MapAssignReuse/Uint64/8192-16    290µs ± 0%     99µs ± 0%   -65.74%  (p=0.000 n=10+9)
MapAssignReuse/Uint64/65536-16  2.58ms ± 0%   0.94ms ± 2%   -63.60%  (p=0.000 n=8+10)
```

Tips:

- umap is slower in `MapAssignGrow/Uint64/6`, the reason is that in this case, the runtime hashmap is allocated in the stack instead of the heap. umap is always allocated in the heap.

- You can run this benchmark via

  ```bash
  $ env BENCH_TYPE=runtime go test -bench=. -count=10 -timeout=10h > a.txt
  $ go test -bench=. -count=10 -timeout=10h > b.txt
  $ benchstat a.txt b.txt
  ```

