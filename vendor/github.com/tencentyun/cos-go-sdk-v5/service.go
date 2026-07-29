package cos

import (
	"context"
	"encoding/xml"
	"net/http"
)

// Service 相关 API
type ServiceService service

// ServiceGetResult is the result of Get Service
type ServiceGetResult struct {
	XMLName     xml.Name `xml:"ListAllMyBucketsResult"`
	Owner       *Owner   `xml:"Owner"`
	Buckets     []Bucket `xml:"Buckets>Bucket,omitempty"`
	Marker      string   `xml:"Marker"`
	NextMarker  string   `xml:"NextMarker"`
	IsTruncated bool     `xml:"IsTruncated"`
}

type ServiceGetOptions struct {
	TagKey     string `url:"tagkey,omitempty"`
	TagValue   string `url:"tagvalue,omitempty"`
	MaxKeys    int64  `url:"max-keys,omitempty"`
	Marker     string `url:"marker,omitempty"`
	Range      string `url:"range,omitempty"`
	CreateTime int64  `url:"create-time,omitempty"`
	Region     string `url:"region,omitempty"`
}

// Get Service 接口实现获取该用户下所有Bucket列表。
//
// 该API接口需要使用Authorization签名认证，
// 且只能获取签名中AccessID所属账户的Bucket列表。
//
// https://www.qcloud.com/document/product/436/8291
func (s *ServiceService) Get(ctx context.Context, opt ...*ServiceGetOptions) (*ServiceGetResult, *Response, error) {
	var sopt *ServiceGetOptions
	if len(opt) > 0 {
		sopt = opt[0]
	}
	var res ServiceGetResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.ServiceURL,
		uri:      "/",
		method:   http.MethodGet,
		optQuery: sopt,
		result:   &res,
	}
	resp, err := s.client.doRetry(ctx, &sendOpt)
	return &res, resp, err
}
