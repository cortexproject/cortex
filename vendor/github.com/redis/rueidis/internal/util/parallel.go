package util

import (
	"sync"
)

func ParallelKeys[K comparable, V any](maxp int, p map[K]V, fn func(k K)) {
	ch := make(chan K, len(p))
	for k := range p {
		ch <- k
	}
	closeThenParallel(maxp, ch, fn)
}

func ParallelVals[K comparable, V any](maxp int, p map[K]V, fn func(k V)) {
	ch := make(chan V, len(p))
	for _, v := range p {
		ch <- v
	}
	closeThenParallel(maxp, ch, fn)
}

func closeThenParallel[V any](maxp int, ch chan V, fn func(k V)) {
	close(ch)
	concurrency := len(ch)
	if concurrency > maxp {
		concurrency = maxp
	}
	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	for i := 1; i < concurrency; i++ {
		go func(wg *sync.WaitGroup) {
			for v := range ch {
				fn(v)
			}
			wg.Done()
		}(&wg)
	}
	for v := range ch {
		fn(v)
	}
	wg.Done()
	wg.Wait()
}
