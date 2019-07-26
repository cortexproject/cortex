package gcs

import (
	"context"
	"io"
	"io/ioutil"
	"time"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
)

// ObjectClient for managing objects in a gcs bucket
type ObjectClient struct {
	cfg    Config
	client *storage.Client
	bucket *storage.BucketHandle
}

// Config is config for the GCS Client.
type Config struct {
	BucketName string `yaml:"bucket_name"`
}

// NewGCSObjectClient makes a new chunk.ObjectClient that writes objects to GCS.
func NewGCSObjectClient(ctx context.Context, cfg Config) (*ObjectClient, error) {
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

func newGCSObjectClient(cfg Config, client *storage.Client) *ObjectClient {
	bucket := client.Bucket(cfg.BucketName)
	return &ObjectClient{
		cfg:    cfg,
		client: client,
		bucket: bucket,
	}
}

// Get object from the store
func (s *ObjectClient) Get(ctx context.Context, objectName string) ([]byte, error) {
	reader, err := s.bucket.Object(objectName).NewReader(ctx)
	if err != nil {
		return nil, nil
	}
	defer reader.Close()

	return ioutil.ReadAll(reader)
}

// Put object into the store
func (s *ObjectClient) Put(ctx context.Context, objectName string, object io.Reader) error {
	writer := s.bucket.Object(objectName).NewWriter(ctx)
	defer writer.Close()

	_, err := io.Copy(writer, object)
	return err
}

// List objects from the store
func (s *ObjectClient) List(ctx context.Context, prefix string) (map[string]time.Time, error) {
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
		objectNamesWithMtime[attr.Name] = attr.Updated
	}

	return objectNamesWithMtime, nil
}
