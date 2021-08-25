package thanosconvert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/weaveworks/common/logging"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	utillog "github.com/cortexproject/cortex/pkg/util/log"
)

func stringNotEmpty(v string) bool {
	return v != ""
}

type fakeBucket map[string]map[string]metadata.Meta

var (
	block1                 = ulid.MustNew(1, nil).String()
	block2                 = ulid.MustNew(2, nil).String()
	block3                 = ulid.MustNew(3, nil).String()
	blockWithGetFailure    = ulid.MustNew(1000, nil).String()
	blockWithUploadFailure = ulid.MustNew(1001, nil).String()
	blockWithMalformedMeta = ulid.MustNew(1002, nil).String()
)

func TestThanosBlockConverter(t *testing.T) {
	tests := []struct {
		name       string
		bucketData fakeBucket
		assertions func(*testing.T, *bucket.ClientMock, Results, error)
	}{
		{
			name:       "empty bucket is a noop",
			bucketData: fakeBucket{},
			assertions: func(t *testing.T, bkt *bucket.ClientMock, results Results, err error) {
				bkt.AssertNotCalled(t, "Get", mock.Anything, mock.Anything)
				bkt.AssertNotCalled(t, "Upload", mock.Anything, mock.Anything, mock.Anything)
				assert.Len(t, results, 0, "expected no users in results")
			},
		},
		{
			name:       "user with no blocks is a noop",
			bucketData: fakeBucket{"user1": map[string]metadata.Meta{}},
			assertions: func(t *testing.T, bkt *bucket.ClientMock, results Results, err error) {
				bkt.AssertNotCalled(t, "Get", mock.Anything, mock.Anything)
				bkt.AssertNotCalled(t, "Upload", mock.Anything, mock.Anything, mock.Anything)
				assert.Contains(t, results, "user1")
				assert.Len(t, results["user1"].FailedBlocks, 0)
				assert.Len(t, results["user1"].ConvertedBlocks, 0)
				assert.Len(t, results["user1"].UnchangedBlocks, 0)
			},
		},
		{
			name: "bucket fully converted is a noop",
			bucketData: fakeBucket{
				"user1": map[string]metadata.Meta{
					block1: cortexMeta("user1"),
					block2: cortexMeta("user1"),
					block3: cortexMeta("user1"),
				},
				"user2": map[string]metadata.Meta{
					block1: cortexMeta("user2"),
					block2: cortexMeta("user2"),
				},
				"user3": map[string]metadata.Meta{
					block1: cortexMeta("user3"),
				},
			},
			assertions: func(t *testing.T, bkt *bucket.ClientMock, results Results, err error) {
				bkt.AssertNotCalled(t, "Upload", mock.Anything, mock.Anything, mock.Anything)
				assert.Len(t, results, 3, "expected users in results")

				assert.Len(t, results["user1"].FailedBlocks, 0)
				assert.Len(t, results["user1"].ConvertedBlocks, 0)
				assert.ElementsMatch(t, results["user1"].UnchangedBlocks, []string{block1, block2, block3})

				assert.Len(t, results["user2"].FailedBlocks, 0)
				assert.Len(t, results["user2"].ConvertedBlocks, 0)
				assert.ElementsMatch(t, results["user2"].UnchangedBlocks, []string{block1, block2})

				assert.Len(t, results["user3"].FailedBlocks, 0)
				assert.Len(t, results["user3"].ConvertedBlocks, 0)
				assert.ElementsMatch(t, results["user3"].UnchangedBlocks, []string{block1})

			},
		},
		{
			name: "bucket with some blocks to convert",
			bucketData: fakeBucket{
				"user1": map[string]metadata.Meta{
					block1: cortexMeta("user1"),
					block2: thanosMeta(),
					block3: cortexMeta("user1"),
				},
				"user2": map[string]metadata.Meta{
					block1: cortexMeta("user2"),
					block2: cortexMeta("user2"),
				},
				"user3": map[string]metadata.Meta{
					block1: thanosMeta(),
				},
			},
			assertions: func(t *testing.T, bkt *bucket.ClientMock, results Results, err error) {
				assert.Len(t, results, 3, "expected users in results")

				assert.Len(t, results["user1"].FailedBlocks, 0)
				assert.ElementsMatch(t, results["user1"].ConvertedBlocks, []string{block2})
				assert.ElementsMatch(t, results["user1"].UnchangedBlocks, []string{block1, block3})

				assert.Len(t, results["user2"].FailedBlocks, 0)
				assert.Len(t, results["user2"].ConvertedBlocks, 0)
				assert.ElementsMatch(t, results["user2"].UnchangedBlocks, []string{block1, block2})

				assert.Len(t, results["user3"].FailedBlocks, 0)
				assert.ElementsMatch(t, results["user3"].ConvertedBlocks, []string{block1})
				assert.Len(t, results["user3"].UnchangedBlocks, 0)

				bkt.AssertNumberOfCalls(t, "Upload", 2)
			},
		},
		{
			name: "bucket with failed blocks",
			bucketData: fakeBucket{
				"user1": map[string]metadata.Meta{
					block1:              cortexMeta("user1"),
					blockWithGetFailure: cortexMeta("user1"),
					block3:              cortexMeta("user1"),
				},
				"user2": map[string]metadata.Meta{
					block1: cortexMeta("user2"),
					block2: cortexMeta("user2"),
				},
				"user3": map[string]metadata.Meta{
					blockWithUploadFailure: thanosMeta(),
					blockWithMalformedMeta: thanosMeta(),
				},
			},
			assertions: func(t *testing.T, bkt *bucket.ClientMock, results Results, err error) {
				assert.Len(t, results["user1"].FailedBlocks, 1)
				assert.Len(t, results["user2"].FailedBlocks, 0)
				assert.Len(t, results["user3"].FailedBlocks, 2)
			},
		},
	}

	for _, test := range tests {

		t.Run(test.name, func(t *testing.T) {

			bkt := mockBucket(test.bucketData)

			converter := &ThanosBlockConverter{
				logger: getLogger(),
				dryRun: false,
				bkt:    bkt,
			}

			ctx := context.Background()
			results, err := converter.Run(ctx)
			test.assertions(t, bkt, results, err)
		})
	}
}

func cortexMeta(user string) metadata.Meta {
	return metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			Version: metadata.ThanosVersion1,
		},
		Thanos: metadata.Thanos{
			Labels: map[string]string{
				cortex_tsdb.TenantIDExternalLabel: user,
			},
		},
	}
}

func thanosMeta() metadata.Meta {
	return metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			Version: metadata.ThanosVersion1,
		},
		Thanos: metadata.Thanos{
			Labels: map[string]string{
				"cluster": "foo",
			},
		},
	}
}

func getLogger() log.Logger {

	l := logging.Level{}
	if err := l.Set("info"); err != nil {
		panic(err)
	}
	f := logging.Format{}
	if err := f.Set("logfmt"); err != nil {
		panic(err)
	}

	logger, err := utillog.NewPrometheusLogger(l, f)
	if err != nil {
		panic(err)
	}
	return logger
}

func mockBucket(data fakeBucket) *bucket.ClientMock {

	bkt := bucket.ClientMock{}

	// UsersScanner checks for deletion marks using Exist
	bkt.On("Exists", mock.Anything, mock.Anything).Return(false, nil)

	bkt.On("Iter", mock.Anything, "", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		// we're iterating over the top level users
		f := args.Get(2).(func(string) error)
		for user := range data {
			if err := f(user); err != nil {
				panic(err)
			}
		}
	})

	bkt.On("Iter", mock.Anything, mock.MatchedBy(stringNotEmpty), mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		// we're iterating over blocks within a user
		user := strings.TrimSuffix(args.String(1), "/")
		f := args.Get(2).(func(string) error)
		if _, ok := data[user]; !ok {
			panic(fmt.Sprintf("key %s not found in fake bucket data", user))
		}
		for block := range data[user] {
			if err := f(fmt.Sprintf("%s/%s/", user, block)); err != nil {
				panic(err)
			}
		}
	})

	for user, blocks := range data {
		for block, meta := range blocks {

			var body bytes.Buffer
			enc := json.NewEncoder(&body)
			if err := enc.Encode(meta); err != nil {
				panic(err)
			}
			key := fmt.Sprintf("%s/%s/meta.json", user, block)
			switch block {
			case blockWithGetFailure:
				bkt.On("Get", mock.Anything, key).Return(nil, errors.Errorf("test error in Get"))
			case blockWithMalformedMeta:
				bkt.On("Get", mock.Anything, key).Return(ioutil.NopCloser(bytes.NewBufferString("invalid json")), nil)
			default:
				bkt.On("Get", mock.Anything, key).Return(ioutil.NopCloser(&body), nil)
			}

			switch block {
			case blockWithUploadFailure:
				bkt.On("Upload", mock.Anything, key, mock.Anything).Return(errors.Errorf("test error in Upload"))
			default:
				bkt.On("Upload", mock.Anything, key, mock.Anything).Return(nil)
			}
		}
	}

	return &bkt
}

func TestConvertMetadata(t *testing.T) {
	tests := []struct {
		name            string
		expectedUser    string
		in              metadata.Meta
		out             metadata.Meta
		changesRequired []string
	}{
		{
			name:         "no changes required",
			expectedUser: "user1",
			in: metadata.Meta{
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						cortex_tsdb.TenantIDExternalLabel: "user1",
					},
				},
			},
			out: metadata.Meta{
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						cortex_tsdb.TenantIDExternalLabel: "user1",
					},
				},
			},
			changesRequired: []string{},
		},
		{
			name:         "add __org_id__ label",
			expectedUser: "user1",
			in: metadata.Meta{
				Thanos: metadata.Thanos{
					Labels: map[string]string{},
				},
			},
			out: metadata.Meta{
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						cortex_tsdb.TenantIDExternalLabel: "user1",
					},
				},
			},
			changesRequired: []string{"add __org_id__ label"},
		},
		{
			name:         "nil labels map",
			expectedUser: "user1",
			in: metadata.Meta{
				Thanos: metadata.Thanos{
					Labels: nil,
				},
			},
			out: metadata.Meta{
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						cortex_tsdb.TenantIDExternalLabel: "user1",
					},
				},
			},
			changesRequired: []string{"add __org_id__ label"},
		},
		{
			name:         "remove extra Thanos labels",
			expectedUser: "user1",
			in: metadata.Meta{
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						cortex_tsdb.TenantIDExternalLabel: "user1",
						"extra":                           "label",
						"cluster":                         "foo",
					},
				},
			},
			out: metadata.Meta{
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						cortex_tsdb.TenantIDExternalLabel: "user1",
					},
				},
			},
			changesRequired: []string{"remove extra Thanos labels"},
		},
		{
			name:         "fix __org_id__",
			expectedUser: "user1",
			in: metadata.Meta{
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						cortex_tsdb.TenantIDExternalLabel: "wrong_user",
					},
				},
			},
			out: metadata.Meta{
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						cortex_tsdb.TenantIDExternalLabel: "user1",
					},
				},
			},
			changesRequired: []string{"change __org_id__ from wrong_user to user1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out, changesRequired := convertMetadata(test.in, test.expectedUser)
			assert.Equal(t, test.out, out)
			assert.ElementsMatch(t, changesRequired, test.changesRequired)
		})
	}
}
