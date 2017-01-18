package chunk

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3Client is a client for S3
type S3Client interface {
	PutObject(*s3.PutObjectInput) (*s3.PutObjectOutput, error)
	GetObject(*s3.GetObjectInput) (*s3.GetObjectOutput, error)
}

// NewS3Client makes a new S3Client
func NewS3Client(s3URL string) (S3Client, string, error) {
	url, err := url.Parse(s3URL)
	if err != nil {
		return nil, "", err
	}

	s3Config, err := awsConfigFromURL(url)
	if err != nil {
		return nil, "", err
	}

	s3Client := s3.New(session.New(s3Config))
	bucketName := strings.TrimPrefix(url.Path, "/")

	return s3Client, bucketName, nil
}

func awsConfigFromURL(url *url.URL) (*aws.Config, error) {
	if url.User == nil {
		return nil, fmt.Errorf("must specify username & password in URL")
	}
	password, _ := url.User.Password()
	creds := credentials.NewStaticCredentials(url.User.Username(), password, "")
	config := aws.NewConfig().
		WithCredentials(creds).
		WithMaxRetries(0) // We do our own retries, so we can monitor them
	if strings.Contains(url.Host, ".") {
		config = config.WithEndpoint(fmt.Sprintf("http://%s", url.Host)).WithRegion("dummy")
	} else {
		config = config.WithRegion(url.Host)
	}
	return config, nil
}

// S3ClientValue is a flag.Value that parses a URL and produces a S3Client
type S3ClientValue struct {
	url, BucketName string
	S3Client
}

// String implements flag.Value
func (c *S3ClientValue) String() string {
	return c.url
}

// Set implements flag.Value
func (c *S3ClientValue) Set(v string) error {
	var err error
	c.S3Client, c.BucketName, err = NewS3Client(v)
	return err
}
