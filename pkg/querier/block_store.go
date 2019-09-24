package querier

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/alecthomas/units"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/store"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// UserStore is a multi-tenant version of Thanos BucketStore
type UserStore struct {
	logger log.Logger
	cfg    s3.Config
	bucket objstore.BucketReader
	stores map[string]*store.BucketStore
	client storepb.StoreClient
}

// NewUserStore returns a new UserStore
func NewUserStore(logger log.Logger, s3cfg s3.Config) (*UserStore, error) {
	bkt, err := s3.NewBucketWithConfig(logger, s3cfg, "cortex-userstore")
	if err != nil {
		return nil, err
	}

	u := &UserStore{
		logger: logger,
		cfg:    s3cfg,
		bucket: bkt,
		stores: make(map[string]*store.BucketStore),
	}

	serv := grpc.NewServer()
	storepb.RegisterStoreServer(serv, u)
	l, err := net.Listen("tcp", "")
	if err != nil {
		return nil, err
	}
	go serv.Serve(l)

	cc, err := grpc.Dial(l.Addr().String(), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	u.client = storepb.NewStoreClient(cc)

	return u, nil
}

// InitialSync iterates over the s3 bucket creating user bucket stores, and calling InitialSync on each of them
func (u *UserStore) InitialSync(ctx context.Context) error {
	if err := u.syncUserStores(ctx, func(ctx context.Context, s *store.BucketStore) error {
		return s.InitialSync(ctx)
	}); err != nil {
		return err
	}

	return nil
}

// SyncStores iterates over the s3 bucket creating user bucket stores
func (u *UserStore) SyncStores(ctx context.Context) error {
	if err := u.syncUserStores(ctx, func(ctx context.Context, s *store.BucketStore) error {
		return s.SyncBlocks(ctx)
	}); err != nil {
		return err
	}

	return nil
}

func (u *UserStore) syncUserStores(ctx context.Context, f func(context.Context, *store.BucketStore) error) error {
	mint, maxt := &model.TimeOrDurationValue{}, &model.TimeOrDurationValue{}
	mint.Set("0000-01-01T00:00:00Z")
	maxt.Set("9999-12-31T23:59:59Z")

	wg := &sync.WaitGroup{}
	err := u.bucket.Iter(ctx, "", func(s string) error {
		user := strings.TrimSuffix(s, "/")

		var bs *store.BucketStore
		var ok bool
		if bs, ok = u.stores[user]; !ok {

			level.Info(u.logger).Log("msg", "creating user bucket store", "user", user)
			bkt, err := s3.NewBucketWithConfig(u.logger, u.cfg, fmt.Sprintf("cortex-%s", user))
			if err != nil {
				return err
			}

			// Bucket with the user wrapper
			userBkt := &ingester.Bucket{
				UserID: user,
				Bucket: bkt,
			}

			indexCacheSizeBytes := uint64(250 * units.Mebibyte)
			maxItemSizeBytes := indexCacheSizeBytes / 2
			indexCache, err := storecache.NewIndexCache(u.logger, nil, storecache.Opts{
				MaxSizeBytes:     indexCacheSizeBytes,
				MaxItemSizeBytes: maxItemSizeBytes,
			})
			if err != nil {
				return err
			}
			bs, err = store.NewBucketStore(u.logger,
				nil,
				userBkt,
				user,
				indexCache,
				uint64(2*units.Gibibyte),
				0,
				20,
				false,
				20,
				&store.FilterConfig{
					MinTime: *mint,
					MaxTime: *maxt,
				},
			)
			if err != nil {
				return err
			}

			u.stores[user] = bs
		}

		wg.Add(1)
		go func(userID string, s *store.BucketStore) {
			defer wg.Done()
			if err := f(ctx, s); err != nil {
				level.Warn(u.logger).Log("msg", "user sync failed", "user", userID)
			}
		}(user, bs)

		return nil
	})

	wg.Wait()

	return err
}

// Info makes an info request to the underlying user store
func (u *UserStore) Info(ctx context.Context, req *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no metadata")
	}

	v := md.Get("user")
	if len(v) == 0 {
		return nil, fmt.Errorf("no userID")
	}

	store, ok := u.stores[v[0]]
	if !ok {
		return nil, nil
	}

	return store.Info(ctx, req)
}

// Series makes a series request to the underlying user store
func (u *UserStore) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	md, ok := metadata.FromIncomingContext(srv.Context())
	if !ok {
		return fmt.Errorf("no metadata")
	}

	v := md.Get("user")
	if len(v) == 0 {
		return fmt.Errorf("no userID")
	}

	store, ok := u.stores[v[0]]
	if !ok {
		return nil
	}

	return store.Series(req, srv)
}

// LabelNames makes a labelnames request to the underlying user store
func (u *UserStore) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no metadata")
	}

	v := md.Get("user")
	if len(v) == 0 {
		return nil, fmt.Errorf("no userID")
	}

	store, ok := u.stores[v[0]]
	if !ok {
		return nil, nil
	}

	return store.LabelNames(ctx, req)
}

// LabelValues makes a labelvalues request to the underlying user store
func (u *UserStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no metadata")
	}

	v := md.Get("user")
	if len(v) == 0 {
		return nil, fmt.Errorf("no userID")
	}

	store, ok := u.stores[v[0]]
	if !ok {
		return nil, nil
	}

	return store.LabelValues(ctx, req)
}
