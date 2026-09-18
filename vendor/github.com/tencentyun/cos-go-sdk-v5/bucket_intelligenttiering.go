package cos

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
)

type BucketIntelligentTieringFilterAnd struct {
	Prefix string              `xml:"Prefix,omitempty" header:"-"`
	Tag    []*BucketTaggingTag `xml:"Tag,omitempty" header:"-"`
}

type BucketIntelligentTieringFilter struct {
	And    *BucketIntelligentTieringFilterAnd `xml:"And,omitempty" header:"-"`
	Prefix string                             `xml:"Prefix,omitempty" header:"-"`
	Tag    []*BucketTaggingTag                `xml:"Tag,omitempty" header:"-"`
}

type BucketIntelligentTieringTransition struct {
	AccessTier      string `xml:"AccessTier,omitempty" header:"-"`
	Days            int    `xml:"Days,omitempty" header:"-"`
	RequestFrequent int    `xml:"RequestFrequent,omitempty" header:"-"`
}

type BucketPutIntelligentTieringOptions struct {
	XMLName    xml.Name                            `xml:"IntelligentTieringConfiguration" header:"-"`
	Status     string                              `xml:"Status,omitempty" header:"-"`
	Transition *BucketIntelligentTieringTransition `xml:"Transition,omitempty" header:"-"`

	// V2
	Id      string                                `xml:"Id,omitempty" header:"-"`
	Tiering []*BucketIntelligentTieringTransition `xml:"Tiering,omitempty" header:"-"`
	Filter  *BucketIntelligentTieringFilter       `xml:"Filter,omitempty" header:"-"`

	XOptionHeader *http.Header `header:"-,omitempty" url:"-" xml:"-"`
}

type BucketGetIntelligentTieringResult BucketPutIntelligentTieringOptions

type BucketGetIntelligentTieringOptions struct {
	XOptionHeader *http.Header `header:"-,omitempty" url:"-" xml:"-"`
}

type IntelligentTieringConfiguration struct {
	Id      string                                `xml:"Id"`
	Status  string                                `xml:"Status"`
	Tiering []*BucketIntelligentTieringTransition `xml:"Tiering"`
	Filter  *BucketIntelligentTieringFilter       `xml:"Filter,omitempty"`
}

type ListIntelligentTieringConfigurations struct {
	XMLName        xml.Name                           `xml:"ListBucketIntelligentTieringConfigurationsOutput"`
	Configurations []*IntelligentTieringConfiguration `xml:"IntelligentTieringConfiguration,omitempty"`
}

func (s *BucketService) PutIntelligentTiering(ctx context.Context, opt *BucketPutIntelligentTieringOptions) (*Response, error) {
	if opt != nil && opt.Transition != nil {
		opt.Transition.RequestFrequent = 1
	}
	sendOpt := sendOptions{
		baseURL:   s.client.BaseURL.BucketURL,
		uri:       "/?intelligenttiering",
		method:    http.MethodPut,
		optHeader: opt,
		body:      opt,
	}
	resp, err := s.client.doRetry(ctx, &sendOpt)
	return resp, err
}

func (s *BucketService) GetIntelligentTiering(ctx context.Context, opt ...*BucketGetIntelligentTieringOptions) (*BucketGetIntelligentTieringResult, *Response, error) {
	var optHeader *BucketGetIntelligentTieringOptions
	if len(opt) > 0 {
		optHeader = opt[0]
	}
	var res BucketGetIntelligentTieringResult
	sendOpt := sendOptions{
		baseURL:   s.client.BaseURL.BucketURL,
		uri:       "/?intelligenttiering",
		method:    http.MethodGet,
		optHeader: optHeader,
		result:    &res,
	}
	resp, err := s.client.doRetry(ctx, &sendOpt)
	return &res, resp, err
}

func (s *BucketService) PutIntelligentTieringV2(ctx context.Context, opt *BucketPutIntelligentTieringOptions) (*Response, error) {
	if opt == nil || opt.Id == "" {
		return nil, errors.New("id is empty")
	}
	sendOpt := sendOptions{
		baseURL:   s.client.BaseURL.BucketURL,
		uri:       "/?intelligent-tiering&id=" + opt.Id,
		method:    http.MethodPut,
		optHeader: opt,
		body:      opt,
	}
	resp, err := s.client.doRetry(ctx, &sendOpt)
	return resp, err
}

func (s *BucketService) GetIntelligentTieringV2(ctx context.Context, id string, opt ...*BucketGetIntelligentTieringOptions) (*BucketGetIntelligentTieringResult, *Response, error) {
	var optHeader *BucketGetIntelligentTieringOptions
	if len(opt) > 0 {
		optHeader = opt[0]
	}
	var res BucketGetIntelligentTieringResult
	sendOpt := sendOptions{
		baseURL:   s.client.BaseURL.BucketURL,
		uri:       "/?intelligent-tiering&id=" + id,
		method:    http.MethodGet,
		optHeader: optHeader,
		result:    &res,
	}
	resp, err := s.client.doRetry(ctx, &sendOpt)
	return &res, resp, err
}

func (s *BucketService) ListIntelligentTiering(ctx context.Context) (*ListIntelligentTieringConfigurations, *Response, error) {
	var res ListIntelligentTieringConfigurations
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.BucketURL,
		uri:     "/?intelligent-tiering",
		method:  http.MethodGet,
		result:  &res,
	}
	resp, err := s.client.doRetry(ctx, &sendOpt)
	return &res, resp, err
}

func (s *BucketService) DeleteIntelligentTiering(ctx context.Context, id string) (*Response, error) {
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.BucketURL,
		uri:     "/?intelligent-tiering&id=" + id,
		method:  http.MethodDelete,
	}
	resp, err := s.client.doRetry(ctx, &sendOpt)
	return resp, err
}
