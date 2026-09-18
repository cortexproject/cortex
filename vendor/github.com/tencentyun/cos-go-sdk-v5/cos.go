package cos

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"text/template"
	"time"

	"regexp"
	"strconv"

	"github.com/google/go-querystring/query"
	"github.com/mozillazg/go-httpheader"
)

const (
	// Version current go sdk version
	Version               = "0.7.66"
	UserAgent             = "cos-go-sdk-v5/" + Version
	contentTypeXML        = "application/xml"
	defaultServiceBaseURL = "http://service.cos.myqcloud.com"
	XOptionalKey          = "cos-go-sdk-v5-XOptionalKey"
)

var (
	bucketURLTemplate = template.Must(
		template.New("bucketURLFormat").Parse(
			"{{.Schema}}://{{.BucketName}}.cos.{{.Region}}.myqcloud.com",
		),
	)

	// {<http://>|<https://>}{bucketname-appid}.{cos|cos-internal|cos-website|ci}.{region}.{myqcloud.com/tencentcos.cn}{/}
	hostSuffix            = regexp.MustCompile(`^.*((cos|cos-internal|cos-website|ci)\.[a-z-1]+|file)\.(myqcloud\.com|tencentcos\.cn).*$`)
	hostPrefix            = regexp.MustCompile(`^(http://|https://){0,1}([a-z0-9-]+-[0-9]+\.){0,1}((cos|cos-internal|cos-website|ci)\.[a-z-1]+|file)\.(myqcloud\.com|tencentcos\.cn).*$`)
	metaInsightHostPrefix = regexp.MustCompile(`^(http://|https://){0,1}([0-9]+\.){1}((cos|cos-internal|cos-website|ci)\.[a-z-1]+|file)\.(myqcloud\.com|tencentcos\.cn).*$`)
	bucketChecker         = regexp.MustCompile(`^[a-z0-9-]+-[0-9]+$`)
	regionChecker         = regexp.MustCompile(`^[a-z-1]+$`)

	// 校验传入的url
	domainSuffix        = regexp.MustCompile(`^.*\.(myqcloud\.com(:[0-9]+){0,1}|tencentcos\.cn(:[0-9]+){0,1})$`)
	bucketDomainChecker = regexp.MustCompile(`^(http://|https://){0,1}([a-z0-9-]+\.)+(myqcloud\.com|tencentcos\.cn)(:[0-9]+){0,1}$`)
	invalidBucketErr    = fmt.Errorf("invalid bucket format, please check your cos.BaseURL")

	switchHost             = regexp.MustCompile(`([a-z0-9-]+-[0-9]+\.)(cos\.[a-z-1]+)\.(myqcloud\.com)(:[0-9]+){0,1}$`)
	accelerateDomainSuffix = "accelerate.myqcloud.com"
	oldDomainSuffix        = ".myqcloud.com"
	newDomainSuffix        = ".tencentcos.cn"

	ObjectKeySimplifyCheckErr = fmt.Errorf("The Getobject Key is illegal")
)

// BaseURL 访问各 API 所需的基础 URL
type BaseURL struct {
	// 访问 bucket, object 相关 API 的基础 URL（不包含 path 部分）: http://example.com
	BucketURL *url.URL
	// 访问 service API 的基础 URL（不包含 path 部分）: http://example.com
	ServiceURL *url.URL
	// 访问 job API 的基础 URL （不包含 path 部分）: http://example.com
	BatchURL *url.URL
	// 访问 CI 的基础 URL
	CIURL *url.URL
	// 访问 Fetch Task 的基础 URL
	FetchURL *url.URL
	// 访问 MetaInsight 的基础 URL
	MetaInsightURL *url.URL
}

func (*BaseURL) innerCheck(u *url.URL, reg *regexp.Regexp) bool {
	if u == nil {
		return true
	}
	urlStr := strings.TrimRight(u.String(), "/")
	if !strings.HasPrefix(urlStr, "https://") && !strings.HasPrefix(urlStr, "http://") {
		return false
	}
	if domainSuffix.MatchString(urlStr) && !reg.MatchString(urlStr) {
		return false
	}
	host := u.Hostname()
	if domainSuffix.MatchString(host) && !reg.MatchString(u.Scheme+"://"+host) {
		return false
	}
	return true
}

func (u *BaseURL) Check() bool {
	return u.innerCheck(u.BucketURL, bucketDomainChecker) && u.innerCheck(u.ServiceURL, bucketDomainChecker) && u.innerCheck(u.BatchURL, bucketDomainChecker)
}

// NewBucketURL 生成 BaseURL 所需的 BucketURL
//
//	bucketName: bucket名称, bucket的命名规则为{name}-{appid} ，此处填写的存储桶名称必须为此格式
//	Region: 区域代码: ap-beijing-1,ap-beijing,ap-shanghai,ap-guangzhou...
//	secure: 是否使用 https
func NewBucketURL(bucketName, region string, secure bool) (*url.URL, error) {
	schema := "https"
	if !secure {
		schema = "http"
	}

	if region == "" || !regionChecker.MatchString(region) {
		return nil, fmt.Errorf("region[%v] is invalid", region)
	}
	if bucketName == "" || !strings.ContainsAny(bucketName, "-") {
		return nil, fmt.Errorf("bucketName[%v] is invalid", bucketName)
	}
	w := bytes.NewBuffer(nil)
	bucketURLTemplate.Execute(w, struct {
		Schema     string
		BucketName string
		Region     string
	}{
		schema, bucketName, region,
	})

	u, _ := url.Parse(w.String())
	return u, nil
}

type RetryOptions struct {
	Count          int
	Interval       time.Duration
	AutoSwitchHost bool
}
type Config struct {
	EnableCRC              bool
	RequestBodyClose       bool
	RetryOpt               RetryOptions
	ObjectKeySimplifyCheck bool
}

// Client is a client manages communication with the COS API.
type Client struct {
	client *http.Client

	Host      string
	UserAgent string
	BaseURL   *BaseURL

	common service

	Service     *ServiceService
	Bucket      *BucketService
	Object      *ObjectService
	Batch       *BatchService
	CI          *CIService
	MetaInsight *MetaInsightService

	Conf *Config

	invalidURL bool
}

type service struct {
	client *Client
}

// go http default CheckRedirect
func HttpDefaultCheckRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	return nil
}

// NewClient returns a new COS API client.
func NewClient(uri *BaseURL, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	// avoid SSRF, default don't follow 3xx
	if httpClient.CheckRedirect == nil {
		httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}

	baseURL := &BaseURL{}
	if uri != nil {
		baseURL.BucketURL = uri.BucketURL
		baseURL.ServiceURL = uri.ServiceURL
		baseURL.BatchURL = uri.BatchURL
		baseURL.CIURL = uri.CIURL
		baseURL.FetchURL = uri.FetchURL
		baseURL.MetaInsightURL = uri.MetaInsightURL
	}
	if baseURL.ServiceURL == nil {
		baseURL.ServiceURL, _ = url.Parse(defaultServiceBaseURL)
	}
	var invalidURL bool
	if !baseURL.Check() {
		invalidURL = true
	}

	c := &Client{
		client:    httpClient,
		UserAgent: UserAgent,
		BaseURL:   baseURL,
		Conf: &Config{
			EnableCRC:        true,
			RequestBodyClose: false,
			RetryOpt: RetryOptions{
				Count:          3,
				Interval:       time.Duration(0),
				AutoSwitchHost: false,
			},
			ObjectKeySimplifyCheck: true,
		},
		invalidURL: invalidURL,
	}
	c.common.client = c
	c.Service = (*ServiceService)(&c.common)
	c.Bucket = (*BucketService)(&c.common)
	c.Object = (*ObjectService)(&c.common)
	c.Batch = (*BatchService)(&c.common)
	c.CI = (*CIService)(&c.common)
	c.MetaInsight = (*MetaInsightService)(&c.common)
	return c
}

func (c *Client) DisableURLCheck() {
	c.invalidURL = false
}

type Credential struct {
	SecretID     string
	SecretKey    string
	SessionToken string
}

func (c *Client) GetCredential() *Credential {
	if auth, ok := c.client.Transport.(TransportIface); ok {
		ak, sk, token, err := auth.GetCredential()
		if err != nil {
			return nil
		}
		return &Credential{
			SecretID:     ak,
			SecretKey:    sk,
			SessionToken: token,
		}
	}
	return nil
}

type commonHeader struct {
	ContentLength int64 `header:"Content-Length,omitempty"`
}

func (c *Client) newPresignedRequest(ctx context.Context, sendOpt *sendOptions, enablePathMerge bool) (req *http.Request, err error) {
	sendOpt.uri, err = addURLOptions(sendOpt.uri, sendOpt.optQuery)
	if err != nil {
		return
	}
	urlStr := fmt.Sprintf("%s://%s%s", sendOpt.baseURL.Scheme, sendOpt.baseURL.Host, sendOpt.uri)
	if enablePathMerge {
		u, _ := url.Parse(sendOpt.uri)
		urlStr = sendOpt.baseURL.ResolveReference(u).String()
	}
	req, err = http.NewRequest(sendOpt.method, urlStr, nil)
	if err != nil {
		return
	}

	req.Header, err = addHeaderOptions(ctx, req.Header, sendOpt.optHeader)
	if err != nil {
		return
	}
	return req, err
}

func (c *Client) newRequest(ctx context.Context, baseURL *url.URL, uri, method string, body interface{}, optQuery interface{}, optHeader interface{}, isRetry bool) (req *http.Request, err error) {
	if c.invalidURL {
		return nil, invalidBucketErr
	}
	if baseURL == nil {
		return nil, invalidBucketErr
	}
	if !checkURL(baseURL) {
		host := baseURL.String()
		if c.BaseURL.MetaInsightURL != baseURL || !metaInsightHostPrefix.MatchString(host) {
			return nil, invalidBucketErr
		}
	}
	uri, err = addURLOptions(uri, optQuery)
	if err != nil {
		return
	}
	u, _ := url.Parse(uri)
	urlStr := baseURL.ResolveReference(u).String()

	var reader io.Reader
	contentType := ""
	contentMD5 := ""
	contentLength := int64(-1)
	if body != nil {
		// 上传文件
		if r, ok := body.(io.Reader); ok {
			reader = r
		} else {
			b, err := xml.Marshal(body)
			if err != nil {
				return nil, err
			}
			contentType = contentTypeXML
			reader = bytes.NewReader(b)
			contentMD5 = base64.StdEncoding.EncodeToString(calMD5Digest(b))
			contentLength = int64(len(b))
		}
	} else if method == http.MethodPut || method == http.MethodPost {
		contentLength = 0
	}

	req, err = http.NewRequest(method, urlStr, reader)
	if err != nil {
		return
	}

	req.Header, err = addHeaderOptions(ctx, req.Header, optHeader)
	if err != nil {
		return
	}
	if v := req.Header.Get("Content-Length"); v == "" && contentLength >= 0 {
		req.Header.Set("Content-Length", strconv.FormatInt(contentLength, 10))
	}
	if v := req.Header.Get("Content-Length"); req.ContentLength == 0 && v != "" && v != "0" {
		req.ContentLength, _ = strconv.ParseInt(v, 10, 64)
	}

	if contentMD5 != "" {
		req.Header["Content-MD5"] = []string{contentMD5}
	}
	if v := req.Header.Get("User-Agent"); v == "" || !strings.HasPrefix(v, UserAgent) {
		if c.UserAgent != "" {
			req.Header.Set("User-Agent", c.UserAgent)
		}
	}
	if req.Header.Get("Content-Type") == "" && contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if isRetry {
		req.Header.Set("X-Cos-Sdk-Retry", "true")
	}
	if c.Host != "" {
		req.Host = c.Host
	}
	if c.Conf.RequestBodyClose {
		req.Close = true
	}
	return
}

func (c *Client) doAPI(ctx context.Context, req *http.Request, result interface{}, closeBody bool) (*Response, error) {
	var cancel context.CancelFunc
	if closeBody {
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
	}
	req = req.WithContext(ctx)

	resp, err := c.client.Do(req)
	if err != nil {
		// If we got an error, and the context has been canceled,
		// the context's error is probably more useful.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		return nil, err
	}

	defer func() {
		if closeBody {
			// Close the body to let the Transport reuse the connection
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}
	}()

	response := newResponse(resp)

	err = checkResponse(resp)
	if err != nil {
		// StatusCode != 2xx when Get Object
		if !closeBody {
			resp.Body.Close()
		}
		// even though there was an error, we still return the response
		// in case the caller wants to inspect it further
		return response, err
	}

	// need CRC64 verification
	if reader, ok := req.Body.(*teeReader); ok {
		if c.Conf.EnableCRC && reader.writer != nil && !reader.disableCheckSum {
			localcrc := reader.Crc64()
			scoscrc := response.Header.Get("x-cos-hash-crc64ecma")
			icoscrc, err := strconv.ParseUint(scoscrc, 10, 64)
			if icoscrc != localcrc {
				return response, fmt.Errorf("verification failed, want:%v, return:%v, x-cos-hash-crc64ecma:%v, err:%v, header:%+v", localcrc, icoscrc, scoscrc, err, response.Header)
			}
		}
	}

	if result != nil {
		if w, ok := result.(io.Writer); ok {
			_, err = io.Copy(w, resp.Body)
			if err != nil { // read body failed
				return response, err
			}
		} else {
			err = xml.NewDecoder(resp.Body).Decode(result)
			if err == io.EOF {
				err = nil // ignore EOF errors caused by empty response body
			}
		}
	}

	return response, err
}

type sendOptions struct {
	// 基础 URL
	baseURL *url.URL
	// URL 中除基础 URL 外的剩余部分
	uri string
	// 请求方法
	method string

	body interface{}
	// url 查询参数
	optQuery interface{}
	// http header 参数
	optHeader interface{}
	// 用 result 反序列化 resp.Body
	result interface{}
	// 是否禁用自动调用 resp.Body.Close()
	// 自动调用 Close() 是为了能够重用连接
	disableCloseBody bool
	// 是否重试
	isRetry bool
}

func toSwitchHost(oldURL *url.URL) *url.URL {
	// 判断域名是否能够切换
	if !switchHost.MatchString(oldURL.Host) {
		return oldURL
	}
	newURL, _ := url.Parse(oldURL.String())
	hostAndPort := strings.SplitN(newURL.Host, ":", 2)
	newHost := hostAndPort[0]
	// 加速域名不切换
	if strings.HasSuffix(newHost, accelerateDomainSuffix) {
		return oldURL
	}
	newHost = newHost[:len(newHost)-len(oldDomainSuffix)] + newDomainSuffix
	if len(hostAndPort) > 1 {
		newHost += ":" + hostAndPort[1]
	}
	newURL.Host = newHost
	return newURL
}

func (c *Client) CheckRetrieable(u *url.URL, resp *Response, err error, secondLast bool) (*url.URL, bool) {
	res := u
	if err != nil && err != invalidBucketErr {
		// 不重试
		if resp != nil && resp.StatusCode < 500 {
			if c.Conf.RetryOpt.AutoSwitchHost {
				if resp.StatusCode == 301 || resp.StatusCode == 302 || resp.StatusCode == 307 {
					if resp.Header.Get("X-Cos-Request-Id") == "" {
						res = toSwitchHost(u)
						if res != u {
							return res, true
						}
					}
				}
			}
			return res, false
		}
		if c.Conf.RetryOpt.AutoSwitchHost && secondLast {
			// 收不到报文 或者 不存在RequestId
			if resp == nil || resp.Header.Get("X-Cos-Request-Id") == "" {
				res = toSwitchHost(u)
			}
		}
		return res, true
	}
	return res, false
}

func (c *Client) doRetry(ctx context.Context, opt *sendOptions) (resp *Response, err error) {
	if opt.body != nil {
		if _, ok := opt.body.(io.Reader); ok {
			resp, err = c.send(ctx, opt)
			return
		}
	}
	count := 1
	if c.Conf.RetryOpt.Count > 0 {
		count = c.Conf.RetryOpt.Count
	}
	retryErr := &RetryError{}
	var retrieable bool
	for nr := 0; nr < count; nr++ {
		// 把上一次错误记录下来
		if err != nil {
			retryErr.Add(err)
		}
		opt.isRetry = nr > 0
		resp, err = c.send(ctx, opt)
		opt.baseURL, retrieable = c.CheckRetrieable(opt.baseURL, resp, err, nr >= count-2)
		if retrieable {
			if c.Conf.RetryOpt.Interval > 0 && nr+1 < count {
				time.Sleep(c.Conf.RetryOpt.Interval)
			}
			continue
		}
		break
	}
	// 最后一次非COS错误，输出三次结果
	if err != nil {
		if _, ok := err.(*ErrorResponse); !ok {
			retryErr.Add(err)
			err = retryErr
		}
	}
	return
}

func (c *Client) send(ctx context.Context, opt *sendOptions) (resp *Response, err error) {
	req, err := c.newRequest(ctx, opt.baseURL, opt.uri, opt.method, opt.body, opt.optQuery, opt.optHeader, opt.isRetry)
	if err != nil {
		return
	}

	resp, err = c.doAPI(ctx, req, opt.result, !opt.disableCloseBody)
	return
}

// addURLOptions adds the parameters in opt as URL query parameters to s. opt
// must be a struct whose fields may contain "url" tags.
func addURLOptions(s string, opt interface{}) (string, error) {
	v := reflect.ValueOf(opt)
	if v.Kind() == reflect.Ptr && v.IsNil() {
		return s, nil
	}

	u, err := url.Parse(s)
	if err != nil {
		return s, err
	}

	qs, err := query.Values(opt)
	if err != nil {
		return s, err
	}

	// 保留原有的参数，并且放在前面。因为 cos 的 url 路由是以第一个参数作为路由的
	// e.g. /?uploads
	q := u.RawQuery
	rq := qs.Encode()
	if q != "" {
		if rq != "" {
			u.RawQuery = fmt.Sprintf("%s&%s", q, qs.Encode())
		}
	} else {
		u.RawQuery = rq
	}
	return u.String(), nil
}

type XOptionalValue struct {
	Header *http.Header
}

// addHeaderOptions adds the parameters in opt as Header fields to req. opt
// must be a struct whose fields may contain "header" tags.
func addHeaderOptions(ctx context.Context, header http.Header, opt interface{}) (http.Header, error) {
	defer func() {
		// 通过context传递
		if val := ctx.Value(XOptionalKey); val != nil {
			if optVal, ok := val.(*XOptionalValue); ok {
				if optVal.Header != nil {
					for key, values := range *optVal.Header {
						for _, value := range values {
							header.Add(key, value)
						}
					}
				}
			}
		}
	}()
	v := reflect.ValueOf(opt)
	if v.Kind() == reflect.Ptr && v.IsNil() {
		return header, nil
	}

	h, err := httpheader.Header(opt)
	if err != nil {
		return nil, err
	}

	for key, values := range h {
		for _, value := range values {
			header.Add(key, value)
		}
	}

	return header, nil
}

func checkURL(baseURL *url.URL) bool {
	if baseURL == nil {
		return false
	}
	if baseURL.Scheme == "" || baseURL.Hostname() == "" {
		return false
	}
	host := baseURL.String()
	if hostSuffix.MatchString(host) && !hostPrefix.MatchString(host) {
		return false
	}
	return true
}

func CheckObjectKeySimplify(key string) bool {
	res, err := filepath.Abs(key)
	if res == "/" || err != nil {
		return false
	}
	return true
}

// Owner defines Bucket/Object's owner
type Owner struct {
	UIN         string `xml:"uin,omitempty"`
	ID          string `xml:",omitempty"`
	DisplayName string `xml:",omitempty"`
}

// Initiator same to the Owner struct
type Initiator Owner

// Response API 响应
type Response struct {
	*http.Response
}

func newResponse(resp *http.Response) *Response {
	return &Response{
		Response: resp,
	}
}

// ACLHeaderOptions is the option of ACLHeader
type ACLHeaderOptions struct {
	XCosACL              string `header:"x-cos-acl,omitempty" url:"-" xml:"-"`
	XCosGrantRead        string `header:"x-cos-grant-read,omitempty" url:"-" xml:"-"`
	XCosGrantWrite       string `header:"x-cos-grant-write,omitempty" url:"-" xml:"-"`
	XCosGrantFullControl string `header:"x-cos-grant-full-control,omitempty" url:"-" xml:"-"`
	XCosGrantReadACP     string `header:"x-cos-grant-read-acp,omitempty" url:"-" xml:"-"`
	XCosGrantWriteACP    string `header:"x-cos-grant-write-acp,omitempty" url:"-" xml:"-"`
}

// ACLGrantee is the param of ACLGrant
type ACLGrantee struct {
	TypeAttr    xml.Attr `xml:",attr,omitempty"`
	Type        string   `xml:"type,attr,omitempty"`
	UIN         string   `xml:"uin,omitempty"`
	URI         string   `xml:"URI,omitempty"`
	ID          string   `xml:",omitempty"`
	DisplayName string   `xml:",omitempty"`
	SubAccount  string   `xml:"Subaccount,omitempty"`
}

// ACLGrant is the param of ACLXml
type ACLGrant struct {
	Grantee    *ACLGrantee
	Permission string
}

// ACLXml is the ACL body struct
type ACLXml struct {
	XMLName           xml.Name `xml:"AccessControlPolicy"`
	Owner             *Owner
	AccessControlList []ACLGrant `xml:"AccessControlList>Grant,omitempty"`
}

type aclEnum struct {
	Private                string
	PublicRead             string
	PublicReadWrite        string
	AuthenticatedRead      string
	Default                string
	BucketOwnerRead        string
	BucketOwnerFullControl string
}

var ACL = &aclEnum{
	Private:                "private",
	PublicRead:             "public-read",
	PublicReadWrite:        "public-read-write",
	AuthenticatedRead:      "authenticated-read",
	Default:                "default",
	BucketOwnerRead:        "bucket-owner-read",
	BucketOwnerFullControl: "bucket-owner-full-control",
}

func decodeACL(resp *Response, res *ACLXml) {
	ItemMap := map[string]string{
		"ACL":          "x-cos-acl",
		"READ":         "x-cos-grant-read",
		"WRITE":        "x-cos-grant-write",
		"READ_ACP":     "x-cos-grant-read-acp",
		"WRITE_ACP":    "x-cos-grant-write-acp",
		"FULL_CONTROL": "x-cos-grant-full-control",
	}
	publicACL := make(map[string]int)
	resACL := make(map[string][]string)
	for _, item := range res.AccessControlList {
		if item.Grantee == nil {
			continue
		}
		if item.Grantee.ID == "qcs::cam::anyone:anyone" || item.Grantee.URI == "http://cam.qcloud.com/groups/global/AllUsers" {
			publicACL[item.Permission] = 1
		} else if item.Grantee.ID != res.Owner.ID {
			resACL[item.Permission] = append(resACL[item.Permission], "id=\""+item.Grantee.ID+"\"")
		}
	}
	if publicACL["FULL_CONTROL"] == 1 || (publicACL["READ"] == 1 && publicACL["WRITE"] == 1) {
		resACL["ACL"] = []string{"public-read-write"}
	} else if publicACL["READ"] == 1 {
		resACL["ACL"] = []string{"public-read"}
	} else {
		resACL["ACL"] = []string{"private"}
	}

	for item, header := range ItemMap {
		if len(resp.Header.Get(header)) > 0 || len(resACL[item]) == 0 {
			continue
		}
		resp.Header.Set(header, uniqueGrantID(resACL[item]))
	}
}

func uniqueGrantID(grantIDs []string) string {
	res := []string{}
	filter := make(map[string]int)
	for _, id := range grantIDs {
		if filter[id] != 0 {
			continue
		}
		filter[id] = 1
		res = append(res, id)
	}
	return strings.Join(res, ",")
}
