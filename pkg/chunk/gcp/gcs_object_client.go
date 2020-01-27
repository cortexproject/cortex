package gcp

import (
	"context"
	"flag"
	"io/ioutil"
	"strings"
	"time"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/util"
)

type GCSObjectClient struct {
	cfg    GCSConfig
	client *storage.Client
	bucket *storage.BucketHandle
}

// GCSConfig is config for the GCS Chunk Client.
type GCSConfig struct {
	BucketName      string        `yaml:"bucket_name"`
	ChunkBufferSize int           `yaml:"chunk_buffer_size"`
	RequestTimeout  time.Duration `yaml:"request_timeout"`
}

// RegisterFlags registers flags.
func (cfg *GCSConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix registers flags with prefix.
func (cfg *GCSConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.BucketName, prefix+"gcs.bucketname", "", "Name of GCS bucket to put chunks in.")
	f.IntVar(&cfg.ChunkBufferSize, prefix+"gcs.chunk-buffer-size", 0, "The size of the buffer that GCS client for each PUT request. 0 to disable buffering.")
	f.DurationVar(&cfg.RequestTimeout, prefix+"gcs.request-timeout", 0, "The duration after which the requests to GCS should be timed out.")
}

// NewGCSObjectClient makes a new chunk.ObjectClient that writes chunks to GCS.
func NewGCSObjectClient(ctx context.Context, cfg GCSConfig) (*GCSObjectClient, error) {
	option, err := gcsInstrumentation(ctx, storage.ScopeReadWrite)
	if err != nil {
		return nil, err
	}

	client, err := storage.NewClient(ctx, option)
	if err != nil {
		return nil, err
	}
	return newGCSObjectClient(cfg, client), nil
}

func newGCSObjectClient(cfg GCSConfig, client *storage.Client) *GCSObjectClient {
	bucket := client.Bucket(cfg.BucketName)
	return &GCSObjectClient{
		cfg:    cfg,
		client: client,
		bucket: bucket,
	}
}

func (s *GCSObjectClient) Stop() {
	s.client.Close()
}

func (s *GCSObjectClient) PutChunks(ctx context.Context, chunks []chunk.Chunk) error {
	for _, chunk := range chunks {
		buf, err := chunk.Encoded()
		if err != nil {
			return err
		}

		if err := s.PutObject(ctx, chunk.ExternalKey(), buf); err != nil {
			return err
		}
	}
	return nil
}

func (s *GCSObjectClient) GetChunks(ctx context.Context, input []chunk.Chunk) ([]chunk.Chunk, error) {
	return util.GetParallelChunks(ctx, input, s.getChunk)
}

func (s *GCSObjectClient) getChunk(ctx context.Context, decodeContext *chunk.DecodeContext, input chunk.Chunk) (chunk.Chunk, error) {
	buf, err := s.GetObject(ctx, input.ExternalKey())
	if err != nil {
		return chunk.Chunk{}, errors.WithStack(err)
	}

	if err := input.Decode(decodeContext, buf); err != nil {
		return chunk.Chunk{}, err
	}

	return input, nil
}

// Get object from the store
func (s *GCSObjectClient) GetObject(ctx context.Context, objectName string) ([]byte, error) {
	if s.cfg.RequestTimeout > 0 {
		// The context will be cancelled with the timeout or when the parent context is cancelled, whichever occurs first.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.RequestTimeout)
		defer cancel()
	}

	reader, err := s.bucket.Object(objectName).NewReader(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer reader.Close()

	return ioutil.ReadAll(reader)
}

// Put object into the store
func (s *GCSObjectClient) PutObject(ctx context.Context, objectName string, object []byte) error {
	writer := s.bucket.Object(objectName).NewWriter(ctx)
	// Default GCSChunkSize is 8M and for each call, 8M is allocated xD
	// By setting it to 0, we just upload the object in a single a request
	// which should work for our chunk sizes.
	writer.ChunkSize = s.cfg.ChunkBufferSize

	if _, err := writer.Write(object); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}

	return nil
}

// List objects from the store
func (s *GCSObjectClient) List(ctx context.Context, prefix string) (map[string]time.Time, error) {
	objectNamesWithMtime := map[string]time.Time{}
	prefixWithSep := prefix + "/"

	iter := s.bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attr, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			} else {
				return nil, err
			}
		}

		if attr.Name == prefixWithSep {
			continue
		}
		objectNamesWithMtime[strings.TrimPrefix(attr.Name, prefixWithSep)] = attr.Updated
	}

	return objectNamesWithMtime, nil
}
