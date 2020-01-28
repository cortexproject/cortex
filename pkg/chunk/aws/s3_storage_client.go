package aws

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/util"
	awscommon "github.com/weaveworks/common/aws"
	"github.com/weaveworks/common/instrument"
)

var (
	s3RequestDuration = instrument.NewHistogramCollector(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "s3_request_duration_seconds",
		Help:      "Time spent doing S3 requests.",
		Buckets:   []float64{.025, .05, .1, .25, .5, 1, 2},
	}, []string{"operation", "status_code"}))
)

func init() {
	s3RequestDuration.Register()
}

type S3ObjectClient struct {
	bucketNames []string
	S3          s3iface.S3API
}

// NewS3ObjectClient makes a new S3-backed ObjectClient.
func NewS3ObjectClient(cfg StorageConfig) (*S3ObjectClient, error) {
	if cfg.S3.URL == nil {
		return nil, fmt.Errorf("no URL specified for S3")
	}
	s3Config, err := awscommon.ConfigFromURL(cfg.S3.URL)
	if err != nil {
		return nil, err
	}

	s3Config = s3Config.WithS3ForcePathStyle(cfg.S3ForcePathStyle) // support for Path Style S3 url if has the flag

	s3Config = s3Config.WithMaxRetries(0) // We do our own retries, so we can monitor them
	sess, err := session.NewSession(s3Config)
	if err != nil {
		return nil, err
	}
	s3Client := s3.New(sess)
	bucketNames := []string{strings.TrimPrefix(cfg.S3.URL.Path, "/")}
	if cfg.BucketNames != "" {
		bucketNames = strings.Split(cfg.BucketNames, ",") // comma separated list of bucket names
	}
	client := S3ObjectClient{
		S3:          s3Client,
		bucketNames: bucketNames,
	}
	return &client, nil
}

func (a S3ObjectClient) Stop() {
}

func (a S3ObjectClient) GetChunks(ctx context.Context, chunks []chunk.Chunk) ([]chunk.Chunk, error) {
	return util.GetParallelChunks(ctx, chunks, a.getChunk)
}

func (a S3ObjectClient) getChunk(ctx context.Context, decodeContext *chunk.DecodeContext, c chunk.Chunk) (chunk.Chunk, error) {
	readCloser, err := a.GetObject(ctx, c.ExternalKey())
	if err != nil {
		return chunk.Chunk{}, err
	}

	defer readCloser.Close()

	buf, err := ioutil.ReadAll(readCloser)
	if err != nil {
		return chunk.Chunk{}, err
	}

	if err := c.Decode(decodeContext, buf); err != nil {
		return chunk.Chunk{}, err
	}
	return c, nil
}

func (a S3ObjectClient) PutChunks(ctx context.Context, chunks []chunk.Chunk) error {
	var (
		s3ChunkKeys []string
		s3ChunkBufs [][]byte
	)

	for i := range chunks {
		buf, err := chunks[i].Encoded()
		if err != nil {
			return err
		}
		key := chunks[i].ExternalKey()

		s3ChunkKeys = append(s3ChunkKeys, key)
		s3ChunkBufs = append(s3ChunkBufs, buf)
	}

	incomingErrors := make(chan error)
	for i := range s3ChunkBufs {
		go func(i int) {
			incomingErrors <- a.PutObject(ctx, s3ChunkKeys[i], bytes.NewReader(s3ChunkBufs[i]))
		}(i)
	}

	var lastErr error
	for range s3ChunkKeys {
		err := <-incomingErrors
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// bucketFromKey maps a key to a bucket name
func (a S3ObjectClient) bucketFromKey(key string) string {
	if len(a.bucketNames) == 0 {
		return ""
	}

	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	hash := hasher.Sum32()

	return a.bucketNames[hash%uint32(len(a.bucketNames))]
}

// Get object from the store
func (a S3ObjectClient) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	var resp *s3.GetObjectOutput

	// Map the key into a bucket
	bucket := a.bucketFromKey(objectKey)

	err := instrument.CollectedRequest(ctx, "S3.GetObject", s3RequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		resp, err = a.S3.GetObjectWithContext(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(objectKey),
		})
		return err
	})
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

// Put object into the store
func (a S3ObjectClient) PutObject(ctx context.Context, objectKey string, object io.ReadSeeker) error {
	return instrument.CollectedRequest(ctx, "S3.Put", s3RequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		_, err := a.S3.PutObjectWithContext(ctx, &s3.PutObjectInput{
			Body:   object,
			Bucket: aws.String(a.bucketFromKey(objectKey)),
			Key:    aws.String(objectKey),
		})
		return err
	})
}

// List objects from the store
func (a S3ObjectClient) List(ctx context.Context, prefix string) (map[string]time.Time, error) {
	objectKeysWithMtime := map[string]time.Time{}
	prefixWithSep := prefix + "/"

	for i := range a.bucketNames {
		err := instrument.CollectedRequest(ctx, "S3.List", s3RequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
			output, err := a.S3.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{Bucket: &a.bucketNames[i], Prefix: &prefix})
			if err != nil {
				return err
			}

			for i := range output.Contents {
				objectKeysWithMtime[strings.TrimPrefix(*output.Contents[i].Key, prefixWithSep)] = *output.Contents[i].LastModified
			}

			return nil
		})

		if err != nil {
			return nil, err
		}
	}

	return objectKeysWithMtime, nil
}
