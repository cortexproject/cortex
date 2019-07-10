package kv

import (
	"context"
)

// Client is a high-level client for key-value stores (such as Etcd and
// Consul) that exposes operations such as CAS and Watch which take callbacks.
// It also deals with serialisation by using a Codec and having a instance of
// the the desired type passed in to methods ala json.Unmarshal.
type Client interface {
	CAS(ctx context.Context, key string, f CASCallback) error

	// WatchKey calls f whenever the value stored under key changes.
	WatchKey(ctx context.Context, key string, f func(interface{}) bool)

	// WatchPrefix calls f whenever any value stored under prefix changes.
	WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool)

	Get(ctx context.Context, key string) (interface{}, error)
	PutBytes(ctx context.Context, key string, buf []byte) error
}

// CASCallback is the type of the callback to CAS.  If err is nil, out must be non-nil.
type CASCallback func(in interface{}) (out interface{}, retry bool, err error)
