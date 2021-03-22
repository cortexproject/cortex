package bucketclient

import (
	"bytes"
	"context"
	"io/ioutil"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
)

// ProtobufStore is used to read/write protobuf messages to object storage backend. It is implemented
// using the Thanos objstore.Bucket interface
type ProtobufStore struct {
	bucket      objstore.Bucket
	cfgProvider bucket.TenantConfigProvider
	logger      log.Logger
}

func NewProtobufStore(prefix string, bkt objstore.Bucket, cfgProvider bucket.TenantConfigProvider, logger log.Logger) *ProtobufStore {
	return &ProtobufStore{
		bucket:      bucket.NewPrefixedBucketClient(bkt, prefix),
		cfgProvider: cfgProvider,
		logger:      logger,
	}
}

// ListAllUsers returns all users with an entry in the store.
func (s *ProtobufStore) ListAllUsers(ctx context.Context) ([]string, error) {
	var userIDs []string

	err := s.bucket.Iter(ctx, "", func(key string) error {
		userIDs = append(userIDs, key)
		return nil
	})

	return userIDs, err
}

// Set writes the content for an entry in the store for a particular user.
func (s *ProtobufStore) Set(ctx context.Context, userID string, msg proto.Message) error {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	return s.getUserBucket(userID).Upload(ctx, userID, bytes.NewBuffer(msgBytes))
}

// Delete removes the entry in the store for a particular user.
func (s *ProtobufStore) Delete(ctx context.Context, userID string) error {
	userBkt := s.getUserBucket(userID)

	err := userBkt.Delete(ctx, userID)
	if userBkt.IsObjNotFoundErr(err) {
		return nil
	}
	return err
}

// Get reads the content for an entry in the store for a particular user.
func (s *ProtobufStore) Get(ctx context.Context, userID string, msg proto.Message) error {
	readCloser, err := s.getUserBucket(userID).Get(ctx, userID)
	if err != nil {
		return err
	}

	defer runutil.CloseWithLogOnErr(s.logger, readCloser, "close bucket reader")

	buf, err := ioutil.ReadAll(readCloser)
	if err != nil {
		return err
	}

	err = proto.Unmarshal(buf, msg)
	if err != nil {
		return err
	}

	return nil
}

// IsObjNotFoundErr returns true if the error indicates the entry being read does not exist.
func (s *ProtobufStore) IsNotFoundErr(err error) bool {
	return s.bucket.IsObjNotFoundErr(err)
}

func (s *ProtobufStore) getUserBucket(userID string) objstore.Bucket {
	// Inject server-side encryption based on the tenant config.
	return bucket.NewSSEBucketClient(userID, s.bucket, s.cfgProvider)
}
