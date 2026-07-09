package cos

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"net/http"
)

// BucketService 相关 API
type BucketService service

// BucketGetResult is the result of GetBucket
type BucketGetResult struct {
	XMLName        xml.Name `xml:"ListBucketResult"`
	Name           string
	Prefix         string `xml:"Prefix,omitempty"`
	Marker         string `xml:"Marker,omitempty"`
	NextMarker     string `xml:"NextMarker,omitempty"`
	Delimiter      string `xml:"Delimiter,omitempty"`
	MaxKeys        int
	IsTruncated    bool
	Contents       []Object `xml:"Contents,omitempty"`
	CommonPrefixes []string `xml:"CommonPrefixes>Prefix,omitempty"`
	EncodingType   string   `xml:"EncodingType,omitempty"`
}

// BucketGetOptions is the option of GetBucket
type BucketGetOptions struct {
	Prefix        string       `url:"prefix,omitempty" header:"-" xml:"-"`
	Delimiter     string       `url:"delimiter,omitempty" header:"-" xml:"-"`
	EncodingType  string       `url:"encoding-type,omitempty" header:"-" xml:"-"`
	Marker        string       `url:"marker,omitempty" header:"-" xml:"-"`
	MaxKeys       int          `url:"max-keys,omitempty" header:"-" xml:"-"`
	XOptionHeader *http.Header `header:"-,omitempty" url:"-" xml:"-"`
}

// Get Bucket请求等同于 List Object请求，可以列出该Bucket下部分或者所有Object，发起该请求需要拥有Read权限。
//
// https://www.qcloud.com/document/product/436/7734
func (s *BucketService) Get(ctx context.Context, opt *BucketGetOptions) (*BucketGetResult, *Response, error) {
	var res BucketGetResult
	sendOpt := sendOptions{
		baseURL:   s.client.BaseURL.BucketURL,
		uri:       "/",
		method:    http.MethodGet,
		optQuery:  opt,
		optHeader: opt,
		result:    &res,
	}
	resp, err := s.client.doRetry(ctx, &sendOpt)
	return &res, resp, err
}

// BucketPutOptions is same to the ACLHeaderOptions
type BucketPutOptions struct {
	XCosACL                   string                     `header:"x-cos-acl,omitempty" url:"-" xml:"-"`
	XCosGrantRead             string                     `header:"x-cos-grant-read,omitempty" url:"-" xml:"-"`
	XCosGrantWrite            string                     `header:"x-cos-grant-write,omitempty" url:"-" xml:"-"`
	XCosGrantFullControl      string                     `header:"x-cos-grant-full-control,omitempty" url:"-" xml:"-"`
	XCosGrantReadACP          string                     `header:"x-cos-grant-read-acp,omitempty" url:"-" xml:"-"`
	XCosGrantWriteACP         string                     `header:"x-cos-grant-write-acp,omitempty" url:"-" xml:"-"`
	XCosTagging               string                     `header:"x-cos-tagging,omitempty" url:"-" xml:"-"`
	XOptionHeader             *http.Header               `header:"-,omitempty" url:"-" xml:"-"`
	CreateBucketConfiguration *CreateBucketConfiguration `header:"-" url:"-" xml:"-"`
}
type CreateBucketConfiguration struct {
	XMLName          xml.Name `xml:"CreateBucketConfiguration"`
	BucketAZConfig   string   `xml:"BucketAZConfig,omitempty"`
	BucketArchConfig string   `xml:"BucketArchConfig,omitempty"`
}

// Put Bucket请求可以在指定账号下创建一个Bucket。
//
// https://www.qcloud.com/document/product/436/7738
func (s *BucketService) Put(ctx context.Context, opt *BucketPutOptions) (*Response, error) {
	sendOpt := sendOptions{
		baseURL:   s.client.BaseURL.BucketURL,
		uri:       "/",
		method:    http.MethodPut,
		optHeader: opt,
	}
	if opt != nil && opt.CreateBucketConfiguration != nil {
		sendOpt.body = opt.CreateBucketConfiguration
	}
	resp, err := s.client.doRetry(ctx, &sendOpt)
	return resp, err
}

type BucketDeleteOptions struct {
	XOptionHeader *http.Header `header:"-,omitempty" url:"-" xml:"-"`
}

// Delete Bucket请求可以在指定账号下删除Bucket，删除之前要求Bucket为空。
//
// https://www.qcloud.com/document/product/436/7732
func (s *BucketService) Delete(ctx context.Context, opt ...*BucketDeleteOptions) (*Response, error) {
	var dopt *BucketDeleteOptions
	if len(opt) > 0 {
		dopt = opt[0]
	}
	sendOpt := sendOptions{
		baseURL:   s.client.BaseURL.BucketURL,
		uri:       "/",
		method:    http.MethodDelete,
		optHeader: dopt,
	}
	resp, err := s.client.doRetry(ctx, &sendOpt)
	return resp, err
}

type BucketHeadOptions struct {
	XOptionHeader *http.Header `header:"-,omitempty" url:"-" xml:"-"`
}

// Head Bucket请求可以确认是否存在该Bucket，是否有权限访问，Head的权限与Read一致。
//
//	当其存在时，返回 HTTP 状态码200；
//	当无权限时，返回 HTTP 状态码403；
//	当不存在时，返回 HTTP 状态码404。
//
// https://www.qcloud.com/document/product/436/7735
func (s *BucketService) Head(ctx context.Context, opt ...*BucketHeadOptions) (*Response, error) {
	var hopt *BucketHeadOptions
	if len(opt) > 0 {
		hopt = opt[0]
	}
	sendOpt := sendOptions{
		baseURL:   s.client.BaseURL.BucketURL,
		uri:       "/",
		method:    http.MethodHead,
		optHeader: hopt,
	}
	resp, err := s.client.doRetry(ctx, &sendOpt)
	return resp, err
}

func (s *BucketService) IsExist(ctx context.Context) (bool, error) {
	_, err := s.Head(ctx)
	if err == nil {
		return true, nil
	}
	if IsNotFoundError(err) {
		return false, nil
	}
	return false, err
}

// Bucket is the meta info of Bucket
type Bucket struct {
	Name         string
	Region       string `xml:"Location,omitempty"`
	CreationDate string `xml:",omitempty"`
	BucketType   string `xml:",omitempty"`
}

type BucketGetObjectVersionsOptions struct {
	Prefix          string       `url:"prefix,omitempty" header:"-"`
	Delimiter       string       `url:"delimiter,omitempty" header:"-"`
	EncodingType    string       `url:"encoding-type,omitempty" header:"-"`
	KeyMarker       string       `url:"key-marker,omitempty" header:"-"`
	VersionIdMarker string       `url:"version-id-marker,omitempty" header:"-"`
	MaxKeys         int          `url:"max-keys,omitempty" header:"-"`
	XOptionHeader   *http.Header `url:"-" header:"-,omitempty" xml:"-"`
}

type BucketGetObjectVersionsResult struct {
	XMLName             xml.Name                         `xml:"ListVersionsResult"`
	Name                string                           `xml:"Name,omitempty"`
	EncodingType        string                           `xml:"EncodingType,omitempty"`
	Prefix              string                           `xml:"Prefix,omitempty"`
	KeyMarker           string                           `xml:"KeyMarker,omitempty"`
	VersionIdMarker     string                           `xml:"VersionIdMarker,omitempty"`
	MaxKeys             int                              `xml:"MaxKeys,omitempty"`
	Delimiter           string                           `xml:"Delimiter,omitempty"`
	IsTruncated         bool                             `xml:"IsTruncated,omitempty"`
	NextKeyMarker       string                           `xml:"NextKeyMarker,omitempty"`
	NextVersionIdMarker string                           `xml:"NextVersionIdMarker,omitempty"`
	CommonPrefixes      []string                         `xml:"CommonPrefixes>Prefix,omitempty"`
	Version             []ListVersionsResultVersion      `xml:"Version,omitempty"`
	DeleteMarker        []ListVersionsResultDeleteMarker `xml:"DeleteMarker,omitempty"`
}

type ListVersionsResultVersion struct {
	Key          string `xml:"Key,omitempty"`
	VersionId    string `xml:"VersionId,omitempty"`
	IsLatest     bool   `xml:"IsLatest,omitempty"`
	LastModified string `xml:"LastModified,omitempty"`
	ETag         string `xml:"ETag,omitempty"`
	Size         int64  `xml:"Size,omitempty"`
	StorageClass string `xml:"StorageClass,omitempty"`
	Owner        *Owner `xml:"Owner,omitempty"`
}

type ListVersionsResultDeleteMarker struct {
	Key          string `xml:"Key,omitempty"`
	VersionId    string `xml:"VersionId,omitempty"`
	IsLatest     bool   `xml:"IsLatest,omitempty"`
	LastModified string `xml:"LastModified,omitempty"`
	Owner        *Owner `xml:"Owner,omitempty"`
}

func (s *BucketService) GetObjectVersions(ctx context.Context, opt *BucketGetObjectVersionsOptions) (*BucketGetObjectVersionsResult, *Response, error) {
	var res BucketGetObjectVersionsResult
	sendOpt := sendOptions{
		baseURL:   s.client.BaseURL.BucketURL,
		uri:       "/?versions",
		method:    http.MethodGet,
		optQuery:  opt,
		optHeader: opt,
		result:    &res,
	}
	resp, err := s.client.doRetry(ctx, &sendOpt)
	return &res, resp, err
}

type BucketGetMetadataResult struct {
	BucketUrl          string
	BucketName         string
	Location           string
	MAZ                bool
	OFS                bool
	Encryption         *BucketGetEncryptionResult
	ACL                *BucketGetACLResult
	Website            *BucketGetWebsiteResult
	Logging            *BucketGetLoggingResult
	CORS               *BucketGetCORSResult
	Versioning         *BucketGetVersionResult
	Lifecycle          *BucketGetLifecycleResult
	IntelligentTiering *ListIntelligentTieringConfigurations
	Tagging            *BucketGetTaggingResult
	ObjectLock         *BucketGetObjectLockResult
	Replication        *BucketGetReplicationResult
}

func (s *BucketService) GetMeta(ctx context.Context, bucket ...string) (*BucketGetMetadataResult, *Response, error) {
	if s.client.BaseURL.BucketURL == nil {
		return nil, nil, errors.New("BucketURL is empty")
	}
	var customDomain bool
	if !hostPrefix.MatchString(s.client.BaseURL.BucketURL.String()) {
		customDomain = true
		if len(bucket) == 0 || !bucketChecker.MatchString(bucket[0]) {
			return nil, nil, errors.New("you must provide bucket-appid param in using custom domain")
		}
	}
	var err error
	var resp *Response
	var res BucketGetMetadataResult

	resp, err = s.Head(ctx)
	if err != nil {
		return nil, resp, err
	}
	// 非自定义域名
	if !customDomain {
		res.BucketName, res.Location = GetBucketRegionFromUrl(s.client.BaseURL.BucketURL)
	} else {
		res.BucketName, res.Location = bucket[0], resp.Header.Get("X-Cos-Bucket-Region")
	}
	w := bytes.NewBuffer(nil)
	bucketURLTemplate.Execute(w, struct {
		Schema     string
		BucketName string
		Region     string
	}{"https", res.BucketName, res.Location})
	res.BucketUrl = w.String()

	if resp.Header.Get("X-Cos-Bucket-Az-Type") == "MAZ" {
		res.MAZ = true
	}
	if resp.Header.Get("X-Cos-Bucket-Arch") == "OFS" {
		res.OFS = true
	}

	res.Encryption, resp, err = s.GetEncryption(ctx)
	if err != nil {
		if IsNotFoundError(err) {
			res.Encryption = nil
		} else {
			return nil, resp, err
		}
	}
	res.ACL, resp, err = s.GetACL(ctx)
	if err != nil {
		if IsNotFoundError(err) {
			res.ACL = nil
		} else {
			return nil, resp, err
		}
	}
	res.Website, resp, err = s.GetWebsite(ctx)
	if err != nil {
		if IsNotFoundError(err) {
			res.Website = nil
		} else {
			return nil, resp, err
		}
	}
	res.Logging, resp, err = s.GetLogging(ctx)
	if err != nil {
		if IsNotFoundError(err) {
			res.Logging = nil
		} else {
			return nil, resp, err
		}
	}
	res.CORS, resp, err = s.GetCORS(ctx)
	if err != nil {
		if IsNotFoundError(err) {
			res.CORS = nil
		} else {
			return nil, resp, err
		}
	}
	res.Versioning, resp, err = s.GetVersioning(ctx)
	if err != nil {
		if IsNotFoundError(err) {
			res.Versioning = nil
		} else {
			return nil, resp, err
		}
	}
	res.Lifecycle, resp, err = s.GetLifecycle(ctx)
	if err != nil {
		if IsNotFoundError(err) {
			res.Lifecycle = nil
		} else {
			return nil, resp, err
		}
	}
	res.IntelligentTiering, resp, err = s.ListIntelligentTiering(ctx)
	if err != nil {
		if IsNotFoundError(err) {
			res.IntelligentTiering = nil
		} else {
			return nil, resp, err
		}
	}
	res.Tagging, resp, err = s.GetTagging(ctx)
	if err != nil {
		if IsNotFoundError(err) {
			res.Tagging = nil
		} else {
			return nil, resp, err
		}
	}
	res.ObjectLock, resp, err = s.GetObjectLockConfiguration(ctx)
	if err != nil {
		if IsNotFoundError(err) {
			res.ObjectLock = nil
		} else {
			return nil, resp, err
		}
	}
	res.Replication, resp, err = s.GetBucketReplication(ctx)
	if err != nil {
		if IsNotFoundError(err) {
			res.Replication = nil
		} else {
			return nil, resp, err
		}
	}
	return &res, resp, nil
}
