package cos

import (
	"context"
	"encoding/xml"
	"fmt"
	"github.com/clbanning/mxj"
	"net/http"
	"strconv"
	"strings"
)

type BucketPutOriginOptions struct {
	XMLName xml.Name           `xml:"OriginConfiguration"`
	Rule    []BucketOriginRule `xml:"OriginRule"`
}

type BucketOriginRule struct {
	RulePriority    int                    `xml:"RulePriority,omitempty"`
	OriginType      string                 `xml:"OriginType,omitempty"`
	OriginCondition *BucketOriginCondition `xml:"OriginCondition,omitempty"`
	OriginParameter *BucketOriginParameter `xml:"OriginParameter,omitempty"`
	OriginInfo      *BucketOriginInfo      `xml:"OriginInfo,omitempty"`
}

type BucketOriginCondition struct {
	HTTPStatusCode string `xml:"HTTPStatusCode,omitempty"`
	Prefix         string `xml:"Prefix,omitempty"`
}

type BucketOriginParameter struct {
	Protocol          string                  `xml:"Protocol,omitempty"`
	FollowQueryString *bool                   `xml:"FollowQueryString,omitempty"`
	HttpHeader        *BucketOriginHttpHeader `xml:"HttpHeader,omitempty"`
	FollowRedirection *bool                   `xml:"FollowRedirection,omitempty"`
	HttpRedirectCode  string                  `xml:"HttpRedirectCode,omitempty"`
	CopyOriginData    *bool                   `xml:"CopyOriginData,omitempty"`
}

type BucketOriginHttpHeader struct {
	// 目前还不支持 FollowAllHeaders
	FollowAllHeaders    *bool              `xml:"FollowAllHeaders,omitempty"`
	NewHttpHeaders      []OriginHttpHeader `xml:"NewHttpHeaders>Header,omitempty"`
	FollowHttpHeaders   []OriginHttpHeader `xml:"FollowHttpHeaders>Header,omitempty"`
	ForbidFollowHeaders []OriginHttpHeader `xml:"ForbidFollowHeaders>Header,omitempty"`
}

type OriginHttpHeader struct {
	Key   string `xml:"Key,omitempty"`
	Value string `xml:"Value,omitempty"`
}

type BucketOriginInfo struct {
	HostInfo *BucketOriginHostInfo `xml:"HostInfo,omitempty"`
	FileInfo *BucketOriginFileInfo `xml:"FileInfo,omitempty"`
}

type BucketOriginHostInfo struct {
	HostName          string
	Weight            int64
	StandbyHostName_N []string
}

type BucketOriginFileInfo struct {
	// 兼容旧版本
	PrefixDirective bool   `xml:"PrefixDirective,omitempty"`
	Prefix          string `xml:"Prefix,omitempty"`
	Suffix          string `xml:"Suffix,omitempty"`
	// 新版本
	PrefixConfiguration    *OriginPrefixConfiguration    `xml:"PrefixConfiguration,omitempty"`
	SuffixConfiguration    *OriginSuffixConfiguration    `xml:"SuffixConfiguration,omitempty"`
	FixedFileConfiguration *OriginFixedFileConfiguration `xml:"FixedFileConfiguration,omitempty"`
}

type OriginPrefixConfiguration struct {
	Prefix string `xml:"Prefix,omitempty"`
}

type OriginSuffixConfiguration struct {
	Suffix string `xml:"Suffix,omitempty"`
}

type OriginFixedFileConfiguration struct {
	FixedFilePath string `xml:"FixedFilePath,omitempty"`
}

type BucketGetOriginResult BucketPutOriginOptions

func (this *BucketOriginHostInfo) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if this == nil {
		return nil
	}
	err := e.EncodeToken(start)
	if err != nil {
		return err
	}
	if this.HostName != "" {
		err = e.EncodeElement(this.HostName, xml.StartElement{Name: xml.Name{Local: "HostName"}})
		if err != nil {
			return err
		}
	}
	if this.Weight != 0 {
		err = e.EncodeElement(this.Weight, xml.StartElement{Name: xml.Name{Local: "Weight"}})
		if err != nil {
			return err
		}
	}
	for index, standByHostName := range this.StandbyHostName_N {
		err = e.EncodeElement(standByHostName, xml.StartElement{Name: xml.Name{Local: fmt.Sprintf("StandbyHostName_%v", index+1)}})
		if err != nil {
			return err
		}
	}
	return e.EncodeToken(xml.EndElement{Name: start.Name})
}

func (this *BucketOriginHostInfo) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var val struct {
		XMLName xml.Name
		Inner   []byte `xml:",innerxml"`
	}
	err := d.DecodeElement(&val, &start)
	if err != nil {
		return err
	}
	str := "<HostInfo>" + string(val.Inner) + "</HostInfo>"
	myMxjMap, err := mxj.NewMapXml([]byte(str))
	if err != nil {
		return err
	}
	myMap, ok := myMxjMap["HostInfo"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("XML HostInfo Parse failed")
	}

	var total int
	for key, value := range myMap {
		if key == "HostName" {
			this.HostName = value.(string)
		}
		if key == "Weight" {
			v := value.(string)
			this.Weight, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				return err
			}
		}
		if strings.HasPrefix(key, "StandbyHostName_") {
			total++
		}
	}
	// 按顺序执行
	for i := 1; i <= total; i++ {
		key := fmt.Sprintf("StandbyHostName_%v", i)
		this.StandbyHostName_N = append(this.StandbyHostName_N, myMap[key].(string))
	}

	return nil
}

func (s *BucketService) PutOrigin(ctx context.Context, opt *BucketPutOriginOptions) (*Response, error) {
	sendOpt := &sendOptions{
		baseURL: s.client.BaseURL.BucketURL,
		uri:     "/?origin",
		method:  http.MethodPut,
		body:    opt,
	}
	resp, err := s.client.doRetry(ctx, sendOpt)
	return resp, err
}

func (s *BucketService) GetOrigin(ctx context.Context) (*BucketGetOriginResult, *Response, error) {
	var res BucketGetOriginResult
	sendOpt := &sendOptions{
		baseURL: s.client.BaseURL.BucketURL,
		uri:     "/?origin",
		method:  http.MethodGet,
		result:  &res,
	}
	resp, err := s.client.doRetry(ctx, sendOpt)
	return &res, resp, err
}

func (s *BucketService) DeleteOrigin(ctx context.Context) (*Response, error) {
	sendOpt := &sendOptions{
		baseURL: s.client.BaseURL.BucketURL,
		uri:     "/?origin",
		method:  http.MethodDelete,
	}
	resp, err := s.client.doRetry(ctx, sendOpt)
	return resp, err
}
