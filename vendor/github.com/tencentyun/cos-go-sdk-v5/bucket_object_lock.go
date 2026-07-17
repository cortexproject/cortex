package cos

import (
	"context"
	"encoding/xml"
	"net/http"
)

type ObjectLockRule struct {
	Days int `xml:"DefaultRetention>Days,omitempty"`
}

type BucketPutObjectLockOptions struct {
	XMLName           xml.Name        `xml:"ObjectLockConfiguration"`
	ObjectLockEnabled string          `xml:"ObjectLockEnabled,omitempty"`
	Rule              *ObjectLockRule `xml:"Rule,omitempty"`
}

type BucketGetObjectLockResult BucketPutObjectLockOptions

func (s *BucketService) PutObjectLockConfiguration(ctx context.Context, opt *BucketPutObjectLockOptions) (*Response, error) {
	sendOpt := &sendOptions{
		baseURL: s.client.BaseURL.BucketURL,
		uri:     "/?object-lock",
		method:  http.MethodPut,
		body:    opt,
	}
	resp, err := s.client.doRetry(ctx, sendOpt)
	return resp, err
}

func (s *BucketService) GetObjectLockConfiguration(ctx context.Context) (*BucketGetObjectLockResult, *Response, error) {
	var res BucketGetObjectLockResult
	sendOpt := &sendOptions{
		baseURL: s.client.BaseURL.BucketURL,
		uri:     "/?object-lock",
		method:  http.MethodGet,
		result:  &res,
	}
	resp, err := s.client.doRetry(ctx, sendOpt)
	return &res, resp, err
}

type ObjectGetRetentionOptions struct {
	XOptionHeader *http.Header `header:"-,omitempty" url:"-" xml:"-"`
}

type ObjectGetRetentionResult struct {
	XMLName         xml.Name `xml:"Retention"`
	RetainUntilDate string   `xml:"RetainUntilDate,omitempty"`
	Mode            string   `xml:"Mode,omitempty"`
}

func (s *ObjectService) GetRetention(ctx context.Context, key string, opt *ObjectGetRetentionOptions) (*ObjectGetRetentionResult, *Response, error) {
	var res ObjectGetRetentionResult
	sendOpt := sendOptions{
		baseURL:   s.client.BaseURL.BucketURL,
		uri:       "/" + encodeURIComponent(key) + "?retention",
		method:    http.MethodGet,
		optHeader: opt,
		result:    &res,
	}
	resp, err := s.client.doRetry(ctx, &sendOpt)
	return &res, resp, err
}

type ObjectPutRetentionOptions struct {
	XMLName         xml.Name `xml:"Retention"`
	RetainUntilDate string   `xml:"RetainUntilDate,omitempty"`
	Mode            string   `xml:"Mode,omitempty"`
}

func (s *ObjectService) PutRetention(ctx context.Context, key string, opt *ObjectPutRetentionOptions) (*Response, error) {
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.BucketURL,
		uri:     "/" + encodeURIComponent(key) + "?retention",
		method:  http.MethodPut,
		body:    opt,
	}
	resp, err := s.client.doRetry(ctx, &sendOpt)
	return resp, err
}
