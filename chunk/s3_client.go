package chunk

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3Client is a client for a single S3 bucket.
type S3Client interface {
	PutObject(string, []byte) error
	GetObject(string) ([]byte, error)
}

type s3client struct {
	bucketName *string
	s3         *s3.S3
}

func (s *s3client) GetObject(key string) ([]byte, error) {
	resp, err := s.s3.GetObject(&s3.GetObjectInput{
		Bucket: s.bucketName,
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (s *s3client) PutObject(key string, buf []byte) error {
	_, err := s.s3.PutObject(&s3.PutObjectInput{
		Body:   bytes.NewReader(buf),
		Bucket: s.bucketName,
		Key:    aws.String(key),
	})
	return err
}

// NewS3Client makes a new S3Client
func NewS3Client(s3URL *url.URL) (S3Client, error) {
	bucketName := strings.TrimPrefix(s3URL.Path, "/")
	if s3URL.Scheme == "inmemory" {
		return NewMockS3(), nil
	}

	s3Config, err := awsConfigFromURL(s3URL)
	if err != nil {
		return nil, err
	}
	s3Client := s3.New(session.New(s3Config))
	bucket := aws.String(bucketName)
	return &s3client{
		bucketName: bucket,
		s3:         s3Client,
	}, nil
}

// awsConfigFromURL returns AWS config from given URL. It expects escaped AWS Access key ID & Secret Access Key to be
// encoded in the URL. It also expects region specified as a host (letting AWS generate full endpoint) or fully valid
// endpoint with dummy region assumed (e.g for URLs to emulated services).
func awsConfigFromURL(awsURL *url.URL) (*aws.Config, error) {
	if awsURL.User == nil {
		return nil, fmt.Errorf("must specify escaped Access Key & Secret Access in URL")
	}

	password, _ := awsURL.User.Password()
	creds := credentials.NewStaticCredentials(awsURL.User.Username(), password, "")
	config := aws.NewConfig().
		WithCredentials(creds).
		WithMaxRetries(0) // We do our own retries, so we can monitor them
	if strings.Contains(awsURL.Host, ".") {
		return config.WithEndpoint(fmt.Sprintf("http://%s", awsURL.Host)).WithRegion("dummy"), nil
	}

	// Let AWS generate default endpoint based on region passed as a host in URL.
	return config.WithRegion(awsURL.Host), nil
}
