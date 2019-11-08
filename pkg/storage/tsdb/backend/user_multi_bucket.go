package s3

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
)

// MultiBuckets is a wrapper around a slice of objstore.Bucket that prepends writes with a userID and shards them based on root name
type MultiBuckets struct {
	UserID  string
	Buckets []PeriodBucket // The list of period buckets should be sorted
}

// PeriodBucket is a bucket that only is utilized after the given timestamp
type PeriodBucket struct {
	objstore.Bucket
	Start uint64 // start is the timestamp in ms
}

// SortableBuckets is a wrapper type around a slice of PeriodBuckets that implements sort interface
type SortableBuckets []PeriodBucket

// Len implements the sort interface
func (s SortableBuckets) Len() int { return len(s) }

// Less implements the sort interface
func (s SortableBuckets) Less(i, j int) bool {
	if s[i].Start == s[j].Start { // if same start timestamp sort by name
		return s[i].Name() < s[j].Name()
	}

	return s[i].Start < s[j].Start
}

// Swap implements the sort interface
func (s SortableBuckets) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// NewMultiBucketClient returns a new MultiBucket client
// TODO dont' take configs but take a slice of PeriodBuckets
func NewMultiBucketClient(cfg *MultiBucketConfig, name string, logger log.Logger) (*MultiBuckets, error) {

	bkts := make([]PeriodBucket, 0, len(cfg.Endpoints))
	for _, b := range cfg.Endpoints {
		s3Cfg := s3.Config{
			Bucket:    BucketName(b),
			Endpoint:  Endpoint(b),
			AccessKey: cfg.AccessKeyID,
			SecretKey: cfg.SecretAccessKey,
			Insecure:  cfg.Insecure,
		}
		var err error
		bkt, err := s3.NewBucketWithConfig(util.Logger, s3Cfg, fmt.Sprintf("cortex-%s", b))
		if err != nil {
			return nil, err
		}

		t, err := time.Parse("2006-01-02", From(b))
		if err != nil {
			return nil, err
		}

		bkts = append(bkts, PeriodBucket{
			Bucket: bkt,
			Start:  ulid.Timestamp(t),
		})
	}

	// Sort the buckets
	sort.Sort(SortableBuckets(bkts))

	return &MultiBuckets{
		UserID:  name,
		Buckets: bkts,
	}, nil
}

// Close implements io.Closer
func (b *MultiBuckets) Close() error {
	var closeErr error
	for _, bkt := range b.Buckets {
		if err := bkt.Close(); err != nil {
			closeErr = err
		}
	}

	return closeErr
}

// Upload the contents of the reader as an object into the bucket.
func (b *MultiBuckets) Upload(ctx context.Context, name string, r io.Reader) error {
	bkt, err := b.bucket(name)
	if err != nil {
		return err
	}

	return bkt.Upload(ctx, b.fullName(name), r)
}

// Delete removes the object with the given name.
func (b *MultiBuckets) Delete(ctx context.Context, name string) error {
	bkt, err := b.bucket(name)
	if err != nil {
		return err
	}

	return bkt.Delete(ctx, b.fullName(name))
}

func (b *MultiBuckets) fullName(name string) string {
	return fmt.Sprintf("%s/%s", b.UserID, name)
}

// Name returns the bucket name for the provider
func (b *MultiBuckets) Name() string { return b.UserID }

// Iter reads blocks ID's from a channel and passes them to the iter func f
func (b *MultiBuckets) Iter(ctx context.Context, dir string, f func(string) error) error {

	queue := make(chan string, 20) // NOTE: currently sized at the default of blocksyncconcurrency

	wg := sync.WaitGroup{}
	for _, bkt := range b.Buckets {
		/*
		   Since all objects are prefixed with the userID we need to strip the userID
		   upon passing to the processing function
		*/
		wg.Add(1)
		go func(bkt objstore.Bucket) {
			defer wg.Done()
			err := bkt.Iter(ctx, b.fullName(dir), func(s string) error {
				queue <- s
				return nil
			})
			if err != nil {
				level.Warn(util.Logger).Log("msg", "bkt iter error", "err", err)
			}
		}(bkt)
	}

	go func() {
		wg.Wait()
		close(queue)
	}()

	// NOTE: because the SyncBlocks function doesn't support concurrent calls, we have to serialize all calls here
	for s := range queue {
		if err := f(strings.Join(strings.Split(s, "/")[1:], "/")); err != nil {
			level.Warn(util.Logger).Log("msg", "sync process error", "err", err)
		}
	}

	return nil
}

// Get returns a reader for the given object name.
func (b *MultiBuckets) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	bkt, err := b.bucket(name)
	if err != nil {
		return nil, err
	}

	return bkt.Get(ctx, b.fullName(name))
}

// GetRange returns a new range reader for the given object name and range.
func (b *MultiBuckets) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	bkt, err := b.bucket(name)
	if err != nil {
		return nil, err
	}

	return bkt.GetRange(ctx, b.fullName(name), off, length)
}

// Exists checks if the given object exists in the bucket.
func (b *MultiBuckets) Exists(ctx context.Context, name string) (bool, error) {
	bkt, err := b.bucket(name)
	if err != nil {
		return false, err
	}

	return bkt.Exists(ctx, b.fullName(name))
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *MultiBuckets) IsObjNotFoundErr(err error) bool {
	return b.Buckets[0].IsObjNotFoundErr(err)
}

func (b *MultiBuckets) bucketList(name string) (string, []PeriodBucket, error) {
	// Parse out the ulid from the name
	dirs := strings.Split(name, "/")
	u := dirs[0]
	if filepath.Dir(name) == block.DebugMetas {
		u = strings.TrimSuffix(dirs[2], ".json")
	}

	ui, err := ulid.Parse(u)
	if err != nil {
		return "", nil, errors.Wrap(err, fmt.Sprintf("root key invalid ulid: %s", name))
	}

	t := ui.Time()
	// Find the list of buckets that are included in this timestamp
	idx := 0
	for i, b := range b.Buckets {
		if b.Start > t {
			break
		}
		idx = i
	}

	return u, b.Buckets[:idx+1], nil
}

// bucket returns the bucket shard based on the given name
func (b *MultiBuckets) bucket(name string) (objstore.Bucket, error) {
	u, list, err := b.bucketList(name)
	if err != nil {
		return nil, err
	}

	hasher := fnv.New32a()
	hasher.Write([]byte(u))
	hash := hasher.Sum32()

	return list[hash%uint32(len(list))], nil
}
