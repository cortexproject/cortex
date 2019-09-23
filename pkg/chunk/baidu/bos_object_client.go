package baidu

import (
	"context"
	"io/ioutil"

	"github.com/baidubce/bce-sdk-go/services/bos"
	bosAPI "github.com/baidubce/bce-sdk-go/services/bos/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/instrument"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/util"
)

var (
	bosRequestDuration = instrument.NewHistogramCollector(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "bos_request_duration_seconds",
		Help:      "Time spent doing bos requests.",
		Buckets:   []float64{.025, .05, .1, .25, .5, 1, 2},
	}, []string{"operation", "status_code"}))
)

func init() {
	bosRequestDuration.Register()
}

type ObjectStorageClient struct {
	bos             *bos.Client
	bucket          string
	objectKeyPrefix string
}

type ObjectStorageConfig struct {
	Endpoint        string `yaml:"endpoint"`
	AccessKey       string `yaml:"ak"`
	SecretKey       string `yaml:"sk"`
	Bucket          string `yaml:"bucket"`
	ObjectKeyPrefix string `yaml:"object_key_prefix"`
}

func New(c ObjectStorageConfig) (*ObjectStorageClient, error) {
	bosClient, err := bos.NewClient(c.AccessKey, c.SecretKey, c.Endpoint)
	if err != nil {
		return nil, err
	}
	return &ObjectStorageClient{
		bos:             bosClient,
		bucket:          c.Bucket,
		objectKeyPrefix: c.ObjectKeyPrefix,
	}, nil
}

func (client *ObjectStorageClient) Stop() {
	// nothing to do
}

func (client *ObjectStorageClient) PutChunks(ctx context.Context, chunks []chunk.Chunk) error {
	bosChunkKeys := make([]string, 0, len(chunks))
	bosChunkBufs := make([][]byte, 0, len(chunks))

	for i := range chunks {
		buf, err := chunks[i].Encoded()
		if err != nil {
			return err
		}
		key := client.objectKeyPrefix + chunks[i].ExternalKey()

		bosChunkKeys = append(bosChunkKeys, key)
		bosChunkBufs = append(bosChunkBufs, buf)
	}

	incomingErrors := make(chan error)
	for i := range bosChunkBufs {
		go func(i int) {
			incomingErrors <- client.putChunk(ctx, bosChunkKeys[i], bosChunkBufs[i])
		}(i)
	}

	var lastErr error
	for range bosChunkKeys {
		err := <-incomingErrors
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (client *ObjectStorageClient) putChunk(ctx context.Context, key string, buf []byte) error {
	return instrument.CollectedRequest(ctx, "bos.PutObject", bosRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		_, err := client.bos.PutObjectFromBytes(client.bucket, key, buf, nil)
		return err
	})
}

func (client *ObjectStorageClient) GetChunks(ctx context.Context, chunks []chunk.Chunk) ([]chunk.Chunk, error) {
	return util.GetParallelChunks(ctx, chunks, client.getChunk)
}

func (client ObjectStorageClient) getChunk(ctx context.Context, decodeContext *chunk.DecodeContext, c chunk.Chunk) (chunk.Chunk, error) {
	var result *bosAPI.GetObjectResult
	key := client.objectKeyPrefix + c.ExternalKey()

	err := instrument.CollectedRequest(ctx, "bos.GetObject", bosRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		result, err = client.bos.BasicGetObject(client.bucket, key)
		return err
	})
	if err != nil {
		return chunk.Chunk{}, err
	}
	defer result.Body.Close()

	buf, err := ioutil.ReadAll(result.Body)
	if err != nil {
		return chunk.Chunk{}, err
	}
	if err := c.Decode(decodeContext, buf); err != nil {
		return chunk.Chunk{}, err
	}
	return c, nil
}
