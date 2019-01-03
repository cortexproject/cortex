package grpcclient

import (
	"google.golang.org/grpc/naming"
)

type poolResolver struct {
	size int
	next naming.Resolver
}

// NewPoolResolver returns a PoolResolver.
func NewPoolResolver(size int) naming.Resolver {
	resolver, _ := naming.NewDNSResolver()
	return &poolResolver{
		size: size,
		next: resolver,
	}
}

func (r *poolResolver) Resolve(target string) (naming.Watcher, error) {
	watcher, err := r.next.Resolve(target)
	if err != nil {
		return nil, err
	}

	return &poolWatcher{
		size: r.size,
		next: watcher,
	}, nil
}

type poolWatcher struct {
	size int
	next naming.Watcher
}

func (w *poolWatcher) Next() ([]*naming.Update, error) {
	updates, err := w.next.Next()
	if err != nil {
		return nil, err
	}

	result := make([]*naming.Update, 0, len(updates)*w.size)
	for i := 0; i < len(updates)*w.size; i++ {
		result[i] = updates[i/w.size]
	}
	return result, nil
}

func (w *poolWatcher) Close() {
	w.next.Close()
}
