package bucket

import (
	"context"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/util/concurrency"
)

// DeletePrefix removes all objects with given prefix, recursively.
// It returns number of deleted objects.
// If deletion of any object fails, it returns error and stops.
func DeletePrefix(ctx context.Context, bkt objstore.Bucket, prefix string, logger log.Logger, maxConcurrency int) (int, error) {
	keys, err := ListPrefixes(ctx, bkt, prefix, logger)
	if err != nil {
		return 0, err
	}

	result := atomic.NewInt32(0)
	err = concurrency.ForEach(ctx, concurrency.CreateJobsFromStrings(keys), maxConcurrency, func(ctx context.Context, key any) error {
		name := key.(string)
		if err := bkt.Delete(ctx, name); err != nil {
			return err
		}
		result.Inc()
		level.Debug(logger).Log("msg", "deleted file", "file", name)
		return nil
	})

	return int(result.Load()), err
}

func ListPrefixes(ctx context.Context, bkt objstore.Bucket, prefix string, logger log.Logger) ([]string, error) {
	var keys []string
	err := bkt.Iter(ctx, prefix, func(name string) error {
		if strings.HasSuffix(name, objstore.DirDelim) {
			moreKeys, err := ListPrefixes(ctx, bkt, name, logger)
			keys = append(keys, moreKeys...)
			return err
		}

		keys = append(keys, name)
		return nil
	})
	return keys, err
}

func IsOneOfTheExpectedErrors(f ...objstore.IsOpFailureExpectedFunc) objstore.IsOpFailureExpectedFunc {
	return func(err error) bool {
		for _, f := range f {
			if f(err) {
				return true
			}
		}
		return false
	}
}
