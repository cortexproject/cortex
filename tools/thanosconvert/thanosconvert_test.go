package thanosconvert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/weaveworks/common/logging"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/util"
)

func stringNotEmpty(v string) bool {
	return v != ""
}

type fakeBucket map[string]map[string]metadata.Meta

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
					"block1": CortexMeta("user1"),
					"block2": CortexMeta("user1"),
					"block3": CortexMeta("user1"),
				},
				"user2": map[string]metadata.Meta{
					"block1": CortexMeta("user2"),
					"block2": CortexMeta("user2"),
				},
				"user3": map[string]metadata.Meta{
					"block1": CortexMeta("user3"),
				},
			},
			assertions: func(t *testing.T, bkt *bucket.ClientMock, results Results, err error) {
				bkt.AssertNotCalled(t, "Upload", mock.Anything, mock.Anything, mock.Anything)
				assert.Len(t, results, 3, "expected users in results")

				assert.Len(t, results["user1"].FailedBlocks, 0)
				assert.Len(t, results["user1"].ConvertedBlocks, 0)
				assert.ElementsMatch(t, results["user1"].UnchangedBlocks, []string{"block1", "block2", "block3"})

				assert.Len(t, results["user2"].FailedBlocks, 0)
				assert.Len(t, results["user2"].ConvertedBlocks, 0)
				assert.ElementsMatch(t, results["user2"].UnchangedBlocks, []string{"block1", "block2"})

				assert.Len(t, results["user3"].FailedBlocks, 0)
				assert.Len(t, results["user3"].ConvertedBlocks, 0)
				assert.ElementsMatch(t, results["user3"].UnchangedBlocks, []string{"block1"})

			},
		},
		{
			name: "bucket with some blocks to convert",
			bucketData: fakeBucket{
				"user1": map[string]metadata.Meta{
					"block1": CortexMeta("user1"),
					"block2": ThanosMeta(),
					"block3": CortexMeta("user1"),
				},
				"user2": map[string]metadata.Meta{
					"block1": CortexMeta("user2"),
					"block2": CortexMeta("user2"),
				},
				"user3": map[string]metadata.Meta{
					"block1": ThanosMeta(),
				},
			},
			assertions: func(t *testing.T, bkt *bucket.ClientMock, results Results, err error) {
				assert.Len(t, results, 3, "expected users in results")

				assert.Len(t, results["user1"].FailedBlocks, 0)
				assert.ElementsMatch(t, results["user1"].ConvertedBlocks, []string{"block2"})
				assert.ElementsMatch(t, results["user1"].UnchangedBlocks, []string{"block1", "block3"})

				assert.Len(t, results["user2"].FailedBlocks, 0)
				assert.Len(t, results["user2"].ConvertedBlocks, 0)
				assert.ElementsMatch(t, results["user2"].UnchangedBlocks, []string{"block1", "block2"})

				assert.Len(t, results["user3"].FailedBlocks, 0)
				assert.ElementsMatch(t, results["user3"].ConvertedBlocks, []string{"block1"})
				assert.Len(t, results["user3"].UnchangedBlocks, 0)

				bkt.AssertNumberOfCalls(t, "Upload", 2)
			},
		},
		{
			name: "bucket with failed blocks",
			bucketData: fakeBucket{
				"user1": map[string]metadata.Meta{
					"block1":                 CortexMeta("user1"),
					"block_with_get_failure": CortexMeta("user1"),
					"block3":                 CortexMeta("user1"),
				},
				"user2": map[string]metadata.Meta{
					"block1": CortexMeta("user2"),
					"block2": CortexMeta("user2"),
				},
				"user3": map[string]metadata.Meta{
					"block_with_upload_failure": ThanosMeta(),
					"block_with_malformed_meta": ThanosMeta(),
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

			bkt := MockBucket(test.bucketData)

			converter := &ThanosBlockConverter{
				logger: GetLogger(),
				dryRun: false,
				bkt:    bkt,
			}

			ctx := context.Background()
			results, err := converter.Run(ctx)
			test.assertions(t, bkt, results, err)
		})
	}
}

func CortexMeta(user string) metadata.Meta {
	return metadata.Meta{
		Thanos: metadata.Thanos{
			Labels: map[string]string{
				"__org_id__": user,
			},
		},
	}
}

func ThanosMeta() metadata.Meta {
	return metadata.Meta{
		Thanos: metadata.Thanos{
			Labels: map[string]string{
				"cluster": "foo",
			},
		},
	}
}

func GetLogger() log.Logger {

	l := logging.Level{}
	if err := l.Set("info"); err != nil {
		panic(err)
	}
	f := logging.Format{}
	if err := f.Set("logfmt"); err != nil {
		panic(err)
	}

	logger, err := util.NewPrometheusLogger(l, f)
	if err != nil {
		panic(err)
	}
	return logger
}

func MockBucket(data fakeBucket) *bucket.ClientMock {

	bkt := bucket.ClientMock{}
	bkt.On("Iter", mock.Anything, "", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		// we're iterating over the top level users
		f := args.Get(2).(func(string) error)
		for user := range data {
			if err := f(user); err != nil {
				panic(err)
			}
		}
	})

	bkt.On("Iter", mock.Anything, mock.MatchedBy(stringNotEmpty), mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		// we're iterating over blocks within a user
		user := args.String(1)
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
			case "block_with_get_failure":
				bkt.On("Get", mock.Anything, key).Return(nil, errors.Errorf("test error in Get"))
			case "block_with_malformed_meta":
				bkt.On("Get", mock.Anything, key).Return(ioutil.NopCloser(bytes.NewBufferString("invalid json")), nil)
			default:
				bkt.On("Get", mock.Anything, key).Return(ioutil.NopCloser(&body), nil)
			}

			switch block {
			case "block_with_upload_failure":
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
						"__org_id__": "user1",
					},
				},
			},
			out: metadata.Meta{
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						"__org_id__": "user1",
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
						"__org_id__": "user1",
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
						"__org_id__": "user1",
						"extra":      "label",
						"cluster":    "foo",
					},
				},
			},
			out: metadata.Meta{
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						"__org_id__": "user1",
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
						"__org_id__": "wrong_user",
					},
				},
			},
			out: metadata.Meta{
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						"__org_id__": "user1",
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
