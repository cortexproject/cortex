package aws

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/prometheus/client_golang/prometheus"
	awscommon "github.com/weaveworks/common/aws"
	"github.com/weaveworks/common/instrument"

	"github.com/cortexproject/cortex/pkg/util/flagext"
)

var (
	s3RequestDuration = instrument.NewHistogramCollector(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "s3_archive_request_duration_seconds",
		Help:      "Time spent doing S3 requests by Archiver.",
		Buckets:   []float64{.025, .05, .1, .25, .5, 1, 2},
	}, []string{"operation", "status_code"}))
)

func init() {
	s3RequestDuration.Register()
}

// ObjectClient for managing objects in an s3 bucket
type ObjectClient struct {
	bucketName string
	S3         s3iface.S3API
}

// Config specifies config for storing data on AWS.
type Config struct {
	S3               flagext.URLValue
	BucketName       string
	S3ForcePathStyle bool
}

// NewS3ObjectClient makes a new S3-backed ObjectClient.
func NewS3ObjectClient(cfg Config) (*ObjectClient, error) {
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
	bucketName := strings.TrimPrefix(cfg.S3.URL.Path, "/")
	if cfg.BucketName != "" {
		bucketName = cfg.BucketName
	}
	client := ObjectClient{
		S3:         s3Client,
		bucketName: bucketName,
	}
	return &client, nil
}

// Get object from the store
func (a *ObjectClient) Get(ctx context.Context, objectName string) ([]byte, error) {
	var resp *s3.GetObjectOutput

	if err := instrument.CollectedRequest(ctx, "S3.Get", s3RequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		resp, err = a.S3.GetObjectWithContext(ctx, &s3.GetObjectInput{
			Bucket: aws.String(a.bucketName),
			Key:    aws.String(objectName),
		})
		return err
	}); err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// Put object into the store
func (a *ObjectClient) Put(ctx context.Context, objectName string, object io.ReadSeeker) error {
	return instrument.CollectedRequest(ctx, "S3.Put", s3RequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		_, err := a.S3.PutObjectWithContext(ctx, &s3.PutObjectInput{
			Body:   object,
			Bucket: aws.String(a.bucketName),
			Key:    aws.String(objectName),
		})
		return err
	})
}

// List objects from the store
func (a *ObjectClient) List(ctx context.Context, prefix string) (map[string]time.Time, error) {
	objectNamesWithMtime := map[string]time.Time{}
	prefixWithSep := prefix + "/"

	err := instrument.CollectedRequest(ctx, "S3.List", s3RequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		output, err := a.S3.ListObjectsV2WithContext(nil, &s3.ListObjectsV2Input{Bucket: &a.bucketName, Prefix: &prefix})
		if err != nil {
			return err
		}

		for i := range output.Contents {
			objectNamesWithMtime[strings.TrimPrefix(*output.Contents[i].Key, prefixWithSep)] = *output.Contents[i].LastModified
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return objectNamesWithMtime, nil
}
