package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type MetaInsightService service

// OptHeaders 请求头
type OptHeaders struct {
	XOptionHeader *http.Header `header:"-,omitempty" url:"-" json:"-" xml:"-"`
}

func (s *MetaInsightService) baseSend(ctx context.Context, opt interface{}, optionHeader *OptHeaders, uri string, method string) (*bytes.Buffer, *Response, error) {
	var buf bytes.Buffer
	var f *strings.Reader
	var sendOpt *sendOptions
	if optionHeader == nil {
		optionHeader = &OptHeaders{
			XOptionHeader: &http.Header{},
		}
	}
	optionHeader.XOptionHeader.Add("Content-Type", "application/json")
	optionHeader.XOptionHeader.Add("Accept", "application/json")
	if method == http.MethodGet {
		sendOpt = &sendOptions{
			baseURL:   s.client.BaseURL.MetaInsightURL,
			uri:       uri,
			method:    method,
			optHeader: optionHeader,
			optQuery:  opt,
			result:    &buf,
		}
	} else {
		if opt != nil {
			bs, err := json.Marshal(opt)
			if err != nil {
				return nil, nil, err
			}
			f = strings.NewReader(string(bs))
		}
		sendOpt = &sendOptions{
			baseURL:   s.client.BaseURL.MetaInsightURL,
			uri:       uri,
			method:    method,
			body:      f,
			optHeader: optionHeader,
			result:    &buf,
		}
	}
	resp, err := s.client.send(ctx, sendOpt)
	return &buf, resp, err
}

type CreateDatasetOptions struct {
	DatasetName     string      `json:"DatasetName, omitempty" url:"-" `     // 数据集名称，同一个账户下唯一。命名规则如下： 长度为1~32字符。 只能包含小写英文字母，数字，短划线（-）。 必须以英文字母和数字开头。
	Description     string      `json:"Description, omitempty" url:"-" `     // 数据集描述信息。长度为1~256个英文或中文字符，默认值为空。
	TemplateId      string      `json:"TemplateId, omitempty" url:"-" `      // 指模板，在建立元数据索引时，后端将根据模板来决定收集哪些元数据。每个模板都包含一个或多个算子，不同的算子表示不同的元数据。目前支持的模板： Official:DefaultEmptyId：默认为空的模板，表示不进行元数据的采集。 Official:COSBasicMeta：基础信息模板，包含 COS 文件基础元信息算子，表示采集 COS 文件的名称、类型、ACL等基础元信息数据。 Official:FaceSearch：人脸检索模板，包含人脸检索、COS 文件基础元信息算子。Official:ImageSearch：图像检索模板，包含图像检索、COS 文件基础元信息算子。
	Version         string      `json:"Version, omitempty" url:"-" `         // 数据集版本。basic、standard，默认为basic。
	Volume          int         `json:"Volume, omitempty" url:"-" `          // Version为basic时为50w。Version为standard时，默认为500w，可设置1-10000，单位w。传0采用默认值。
	TrainingMode    int         `json:"TrainingMode, omitempty" url:"-" `    // 训练数据的来源模式。默认为0，表示训练数据来源于指定数据集，值为1时表示训练数据来源于cos某个bucket目录下文件。仅在Version为standard时生效。
	TrainingDataset string      `json:"TrainingDataset, omitempty" url:"-" ` // 训练数据的数据集名称。仅在TrainingMode为0时生效。
	TrainingURI     string      `json:"TrainingURI, omitempty" url:"-" `     // 训练数据的资源路径。仅在TrainingMode为1时生效。
	SceneType       string      `json:"SceneType, omitempty" url:"-" `       // 场景类型。支持general、E-commercial、iDrive，默认为general。
	OptHeaders      *OptHeaders `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type CreateDatasetResult struct {
	RequestId string   `json:"RequestId"` // 请求ID
	Dataset   *Dataset `json:"Dataset"`   // 数据集信息
}

type Dataset struct {
	TemplateId    string `json:"TemplateId"`    //    模板ID。
	Description   string `json:"Description"`   //    数据集描述信息
	CreateTime    string `json:"CreateTime"`    //  数据集创建时间的时间戳，格式为RFC3339Nano
	UpdateTime    string `json:"UpdateTime"`    //  数据集修改时间的时间戳，格式为RFC3339Nano创建数据集后，如果未更新过数据集，则数据集修改时间的时间戳和数据集创建时间的时间戳相同
	BindCount     int    `json:"BindCount"`     //  数据集当前绑定的COS Bucket数量
	FileCount     int    `json:"FileCount"`     //  数据集当前文件数量
	TotalFileSize int    `json:"TotalFileSize"` //  数据集中当前文件总大小，单位为字节
	DatasetName   string `json:"DatasetName"`   // 数据集名称
}

// CreateDataset 创建数据集
// https://cloud.tencent.com/document/product/460/106020
func (s *MetaInsightService) CreateDataset(ctx context.Context, opt *CreateDatasetOptions) (*CreateDatasetResult, *Response, error) {
	var res CreateDatasetResult
	if opt == nil {
		return nil, nil, fmt.Errorf("opt param nil")
	}
	buf, resp, err := s.baseSend(ctx, opt, opt.OptHeaders, "/"+"dataset", http.MethodPost)
	if buf.Len() > 0 {
		err = json.Unmarshal(buf.Bytes(), &res)
	}
	return &res, resp, err
}

type DescribeDatasetsOptions struct {
	Maxresults int         `url:"maxresults, omitempty" json:"-"` // 本次返回数据集的最大个数，取值范围为0~200。不设置此参数或者设置为0时，则默认值为100。
	Nexttoken  string      `url:"nexttoken, omitempty" json:"-"`  // 翻页标记。当文件总数大于设置的MaxResults时，用于翻页的Token。从NextToken开始按字典序返回文件信息列表。填写上次查询返回的值，首次使用时填写为空。
	Prefix     string      `url:"prefix, omitempty" json:"-"`     // 数据集名称前缀。
	OptHeaders *OptHeaders `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type DescribeDatasetsResult struct {
	RequestId string     `json:"RequestId"` // 请求ID
	Datasets  []*Dataset `json:"Datasets"`  // 数据集信息
	NextToken string     `json:"NextToken"` // 翻页标记。当任务列表总数大于设置的MaxResults时，用于翻页的Token。符合条件的任务列表未全部返回时，此参数才有值。下一次列出任务列表时将此值作为NextToken传入，将后续的任务列表返回。
}

// DescribeDatasets 列出数据集
// https://cloud.tencent.com/document/product/460/106158
func (s *MetaInsightService) DescribeDatasets(ctx context.Context, opt *DescribeDatasetsOptions) (*DescribeDatasetsResult, *Response, error) {
	var res DescribeDatasetsResult

	buf, resp, err := s.baseSend(ctx, opt, opt.OptHeaders, "/"+"datasets", http.MethodGet)
	if buf.Len() > 0 {
		err = json.Unmarshal(buf.Bytes(), &res)
	}
	return &res, resp, err
}

type UpdateDatasetOptions struct {
	DatasetName string      `json:"DatasetName, omitempty" url:"-" ` // 数据集名称，同一个账户下唯一。
	Description string      `json:"Description, omitempty" url:"-" ` // 数据集描述信息。长度为1~256个英文或中文字符，默认值为空。
	TemplateId  string      `json:"TemplateId, omitempty" url:"-" `  // 该参数表示模板，在建立元数据索引时，后端将根据模板来决定收集哪些元数据。每个模板都包含一个或多个算子，不同的算子表示不同的元数据。目前支持的模板： Official:Empty：默认为空的模板，表示不进行元数据的采集。 Official:COSBasicMeta：基础信息模板，包含COS文件基础元信息算子，表示采集cos文件的名称、类型、acl等基础元信息数据。
	OptHeaders  *OptHeaders `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type UpdateDatasetResult struct {
	RequestId string   `json:"RequestId"` // 请求ID
	Dataset   *Dataset `json:"Dataset"`   // 数据集信息
}

// UpdateDataset 更新数据集
// https://cloud.tencent.com/document/product/460/106156
func (s *MetaInsightService) UpdateDataset(ctx context.Context, opt *UpdateDatasetOptions) (*UpdateDatasetResult, *Response, error) {
	var res UpdateDatasetResult
	if opt == nil {
		return nil, nil, fmt.Errorf("opt param nil")
	}
	buf, resp, err := s.baseSend(ctx, opt, opt.OptHeaders, "/"+"dataset", http.MethodPut)
	if buf.Len() > 0 {
		err = json.Unmarshal(buf.Bytes(), &res)
	}
	return &res, resp, err
}

type DeleteDatasetOptions struct {
	DatasetName string      `json:"DatasetName, omitempty" url:"-" ` // 数据集名称，同一个账户下唯一。
	OptHeaders  *OptHeaders `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type DeleteDatasetResult struct {
	RequestId string   `json:"RequestId"` // 请求ID
	Dataset   *Dataset `json:"Dataset"`   // 数据集信息
}

// DeleteDataset 删除数据集
// https://cloud.tencent.com/document/product/460/106157
func (s *MetaInsightService) DeleteDataset(ctx context.Context, opt *DeleteDatasetOptions) (*DeleteDatasetResult, *Response, error) {
	var res DeleteDatasetResult
	if opt == nil {
		return nil, nil, fmt.Errorf("opt param nil")
	}
	buf, resp, err := s.baseSend(ctx, opt, opt.OptHeaders, "/"+"dataset", http.MethodDelete)
	if buf.Len() > 0 {
		err = json.Unmarshal(buf.Bytes(), &res)
	}
	return &res, resp, err
}

type DescribeDatasetOptions struct {
	Datasetname string      `url:"datasetname, omitempty" json:"-"` // 数据集名称，同一个账户下唯一。
	Statistics  bool        `url:"statistics, omitempty" json:"-"`  // 是否需要实时统计数据集中文件相关信息。有效值： false：不统计，返回的文件的总大小、数量信息可能不正确也可能都为0。 true：需要统计，返回数据集中当前的文件的总大小、数量信息。 默认值为false。
	OptHeaders  *OptHeaders `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type DescribeDatasetResult struct {
	RequestId string   `json:"RequestId"` // 请求ID
	Dataset   *Dataset `json:"Dataset"`   // 数据集信息
}

// DescribeDataset 查询数据集
// https://cloud.tencent.com/document/product/460/106155
func (s *MetaInsightService) DescribeDataset(ctx context.Context, opt *DescribeDatasetOptions) (*DescribeDatasetResult, *Response, error) {
	var res DescribeDatasetResult

	buf, resp, err := s.baseSend(ctx, opt, opt.OptHeaders, "/"+"dataset", http.MethodGet)
	if buf.Len() > 0 {
		err = json.Unmarshal(buf.Bytes(), &res)
	}
	return &res, resp, err
}

type CreateFileMetaIndexOptions struct {
	DatasetName string      `json:"DatasetName, omitempty" url:"-" ` // 数据集名称，同一个账户下唯一。
	File        *File       `json:"File, omitempty" url:"-" `        // 用于建立索引的文件信息。
	OptHeaders  *OptHeaders `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type File struct {
	CustomId     string             `json:"CustomId, omitempty" url:"-" `     // 自定义ID。该文件索引到数据集后，作为该行元数据的属性存储，用于和您的业务系统进行关联、对应。您可以根据业务需求传入该值，例如将某个URI关联到您系统内的某个ID。推荐传入全局唯一的值。在查询时，该字段支持前缀查询和排序，详情请见字段和操作符的支持列表。
	CustomLabels *map[string]string `json:"CustomLabels, omitempty" url:"-" ` // 自定义标签。您可以根据业务需要自定义添加标签键值对信息，用于在查询时可以据此为筛选项进行检索，详情请见字段和操作符的支持列表。
	Key          string             `json:"Key, omitempty" url:"-" `          // 自定义标签键
	Value        string             `json:"Value, omitempty" url:"-" `        // 自定义标签值
	MediaType    string             `json:"MediaType, omitempty" url:"-" `    // 可选项，文件媒体类型，枚举值： image：图片。  other：其他。 document：文档。 archive：压缩包。 video：视频。  audio：音频。
	ContentType  string             `json:"ContentType, omitempty" url:"-" `  // 可选项，文件内容类型（MIME Type），如image/jpeg。
	URI          string             `json:"URI, omitempty" url:"-" `          // 资源标识字段，表示需要建立索引的文件地址，当前仅支持COS上的文件，字段规则：cos:///，其中BucketName表示COS存储桶名称，ObjectKey表示文件完整路径，例如：cos://examplebucket-1250000000/test1/img.jpg。 注意： 1、仅支持本账号内的COS文件 2、不支持HTTP开头的地址
	MaxFaceNum   int                `json:"MaxFaceNum, omitempty" url:"-" `   // 输入图片中检索的人脸数量，默认值为20，最大值为20。(仅当数据集模板 ID 为 Official:FaceSearch 有效)。
	Persons      []*Persons         `json:"Persons, omitempty" url:"-" `      // 自定义人物属性(仅当数据集模板 ID 为 Official:FaceSearch 有效)。
	OptHeaders   *OptHeaders        `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type Persons struct {
	PersonId   string      `json:"PersonId, omitempty" url:"-" ` // 自定义人物 ID。
	OptHeaders *OptHeaders `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type CreateFileMetaIndexResult struct {
	RequestId string `json:"RequestId"` // 请求ID
	EventId   string `json:"EventId"`   // 创建元数据索引的任务ID
}

// CreateFileMetaIndex 创建元数据索引
// https://cloud.tencent.com/document/product/460/106022
func (s *MetaInsightService) CreateFileMetaIndex(ctx context.Context, opt *CreateFileMetaIndexOptions) (*CreateFileMetaIndexResult, *Response, error) {
	var res CreateFileMetaIndexResult
	if opt == nil {
		return nil, nil, fmt.Errorf("opt param nil")
	}
	buf, resp, err := s.baseSend(ctx, opt, opt.OptHeaders, "/"+"filemeta", http.MethodPost)
	if buf.Len() > 0 {
		err = json.Unmarshal(buf.Bytes(), &res)
	}
	return &res, resp, err
}

type UpdateFileMetaIndexOptions struct {
	DatasetName string      `json:"DatasetName, omitempty" url:"-" ` // 数据集名称，同一个账户下唯一。
	Callback    string      `json:"Callback, omitempty" url:"-" `    // 元数据索引结果（以回调形式发送至您的回调地址，支持以 http:// 或者 https:// 开头的地址，例如： http://www.callback.com
	File        *File       `json:"File, omitempty" url:"-" `        // 用于建立索引的文件信息。
	OptHeaders  *OptHeaders `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type UpdateFileMetaIndexResult struct {
	RequestId string `json:"RequestId"` // 请求ID
	EventId   string `json:"EventId"`   // 创建元数据索引的任务ID
}

// UpdateFileMetaIndex 更新元数据索引
// https://cloud.tencent.com/document/product/460/106162
func (s *MetaInsightService) UpdateFileMetaIndex(ctx context.Context, opt *UpdateFileMetaIndexOptions) (*UpdateFileMetaIndexResult, *Response, error) {
	var res UpdateFileMetaIndexResult
	if opt == nil {
		return nil, nil, fmt.Errorf("opt param nil")
	}
	buf, resp, err := s.baseSend(ctx, opt, opt.OptHeaders, "/"+"filemeta", http.MethodPut)
	if buf.Len() > 0 {
		err = json.Unmarshal(buf.Bytes(), &res)
	}
	return &res, resp, err
}

type DescribeFileMetaIndexOptions struct {
	Datasetname string      `url:"datasetname, omitempty" json:"-"` // 数据集名称，同一个账户下唯一。
	Uri         string      `url:"uri, omitempty" json:"-"`         // 资源标识字段，表示需要建立索引的文件地址，当前仅支持COS上的文件，字段规则：cos:///，其中BucketName表示COS存储桶名称，ObjectKey表示文件完整路径，例如：cos://examplebucket-1250000000/test1/img.jpg。 注意： 1、仅支持本账号内的COS文件 2、不支持HTTP开头的地址 3、需UrlEncode
	OptHeaders  *OptHeaders `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type DescribeFileMetaIndexResult struct {
	RequestId string         `json:"RequestId"` // 请求ID。
	Files     []*FilesDetail `json:"Files"`     // 文件元数据的结构体。实际返回的数据可能并不包含该结构体的所有属性，这和您索引该文件时选用的工作流模板配置以及文件本身的内容有关。
}

type FilesDetail struct {
	CreateTime       string             `json:"CreateTime"`       //  元数据创建时间的时间戳，格式为RFC3339Nano
	UpdateTime       string             `json:"UpdateTime"`       //  元数据修改时间的时间戳，格式为RFC3339Nano创建元数据后，如果未更新过元数据，则元数据修改时间的时间戳和元数据创建时间的时间戳相同
	URI              string             `json:"URI"`              //   资源标识字段，表示需要建立索引的文件地址
	Filename         string             `json:"Filename"`         //   文件路径
	MediaType        string             `json:"MediaType"`        //    文件媒体类型。 枚举值：image：图片。other：其他。document：文档。archive：压缩包。audio：音频。video：视频。
	ContentType      string             `json:"ContentType"`      //   文件内容类型（MIME Type）。
	COSStorageClass  string             `json:"COSStorageClass"`  //   文件存储空间类型。
	COSCRC64         string             `json:"COSCRC64"`         //    文件CRC64值。
	ObjectACL        string             `json:"ObjectACL"`        //    对象ACL。
	Size             int                `json:"Size"`             //    文件大小，单位为字节。
	CacheControl     string             `json:"CacheControl"`     //    指定Object被下载时网页的缓存行为。
	ETag             string             `json:"ETag"`             //    Object生成时会创建相应的ETag ，ETag用于标识一个Object的内容。
	FileModifiedTime string             `json:"FileModifiedTime"` //   文件最近一次修改时间的时间戳， 格式为RFC3339Nano。
	CustomId         string             `json:" CustomId"`        //   该文件的自定义ID。该文件索引到数据集后，作为该行元数据的属性存储，用于和您的业务系统进行关联、对应。您可以根据业务需求传入该值，例如将某个URI关联到您系统内的某个ID。推荐传入全局唯一的值。
	CustomLabels     *map[string]string `json:"CustomLabels"`     //   文件自定义标签列表。储存您业务自定义的键名、键值对信息，用于在查询时可以据此为筛选项进行检索。
}

// DescribeFileMetaIndex 查询元数据索引
// https://cloud.tencent.com/document/product/460/106164
func (s *MetaInsightService) DescribeFileMetaIndex(ctx context.Context, opt *DescribeFileMetaIndexOptions) (*DescribeFileMetaIndexResult, *Response, error) {
	var res DescribeFileMetaIndexResult

	buf, resp, err := s.baseSend(ctx, opt, opt.OptHeaders, "/"+"filemeta", http.MethodGet)
	if buf.Len() > 0 {
		err = json.Unmarshal(buf.Bytes(), &res)
	}
	return &res, resp, err
}

type DeleteFileMetaIndexOptions struct {
	DatasetName string      `json:"DatasetName, omitempty" url:"-" ` // 数据集名称，同一个账户下唯一。
	URI         string      `json:"URI, omitempty" url:"-" `         // 资源标识字段，表示需要建立索引的文件地址。
	OptHeaders  *OptHeaders `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type DeleteFileMetaIndexResult struct {
	RequestId string `json:"RequestId"` // 请求ID
}

// DeleteFileMetaIndex 删除元数据索引
// https://cloud.tencent.com/document/product/460/106163
func (s *MetaInsightService) DeleteFileMetaIndex(ctx context.Context, opt *DeleteFileMetaIndexOptions) (*DeleteFileMetaIndexResult, *Response, error) {
	var res DeleteFileMetaIndexResult
	if opt == nil {
		return nil, nil, fmt.Errorf("opt param nil")
	}
	buf, resp, err := s.baseSend(ctx, opt, opt.OptHeaders, "/"+"filemeta", http.MethodDelete)
	if buf.Len() > 0 {
		err = json.Unmarshal(buf.Bytes(), &res)
	}
	return &res, resp, err
}

type CreateDatasetBindingOptions struct {
	DatasetName string      `json:"DatasetName, omitempty" url:"-" ` // 数据集名称，同一个账户下唯一。
	URI         string      `json:"URI, omitempty" url:"-" `         // 资源标识字段，表示需要与数据集绑定的资源，当前仅支持COS存储桶的资源，字段规则：cos://<BucketName>/<Path>，其中BucketName表示COS存储桶名称，Path表示资源路径，例如：cos://examplebucket-1250000000/test/。
	Mode        int         `json:"Mode", omitempty" url:"-" `       // 建立绑定后，以何种方式进行文件索引特征的提取，有效值：0，表示仅对增量文件建立索引；1，表示在对增量文件建立索引的同时，也会对存储桶中存量文件建立索引，存量文件索引建立的时长与存量文件数有关；默认值为0。
	OptHeaders  *OptHeaders `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type CreateDatasetBindingResult struct {
	RequestId string   `json:"RequestId"` // 请求ID
	Binding   *Binding `json:"Binding"`   // 绑定信息
}

type Binding struct {
	URI         string `json:"URI"`         //  资源标识字段，表示需要与数据集绑定的资源，当前仅支持COS存储桶，字段规则：cos://，其中BucketName表示COS存储桶名称，例如：cos://examplebucket-1250000000
	State       string `json:"State"`       //  数据集和 COS Bucket绑定关系的状态。取值范围如下：Running：绑定关系运行中。
	StockState  string `json:"StockState"`  //  当前绑定的存储桶对应的存量索引的状态：有效值：NoIndexing（未进行存量建立索引）、Indexing（存量索引建立中）、Success（存量索引已建立完成）。
	CreateTime  string `json:"CreateTime"`  //  数据集和 COS Bucket绑定关系创建时间的时间戳，格式为RFC3339Nano。
	UpdateTime  string `json:"UpdateTime"`  //  数据集和 COS Bucket的绑定关系修改时间的时间戳，格式为RFC3339Nano。创建绑定关系后，如果未暂停或者未重启过绑定关系，则绑定关系修改时间的时间戳和绑定关系创建时间的时间戳相同。
	DatasetName string `json:"DatasetName"` // 数据集名称。
	Detail      string `json:"Detail"`      // 详情
}

// CreateDatasetBinding 绑定存储桶与数据集
// https://cloud.tencent.com/document/product/460/106159
func (s *MetaInsightService) CreateDatasetBinding(ctx context.Context, opt *CreateDatasetBindingOptions) (*CreateDatasetBindingResult, *Response, error) {
	var res CreateDatasetBindingResult
	if opt == nil {
		return nil, nil, fmt.Errorf("opt param nil")
	}
	buf, resp, err := s.baseSend(ctx, opt, opt.OptHeaders, "/"+"datasetbinding", http.MethodPost)
	if buf.Len() > 0 {
		err = json.Unmarshal(buf.Bytes(), &res)
	}
	return &res, resp, err
}

type DescribeDatasetBindingOptions struct {
	Datasetname string      `url:"datasetname, omitempty" json:"-"` // 数据集名称，同一个账户下唯一。
	Uri         string      `url:"uri, omitempty" json:"-"`         // 资源标识字段，表示需要与数据集绑定的资源，当前仅支持COS存储桶，字段规则：cos://，其中BucketName表示COS存储桶名称，例如（需要进行urlencode）：cos%3A%2F%2Fexample-125000
	OptHeaders  *OptHeaders `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type DescribeDatasetBindingResult struct {
	RequestId string   `json:"RequestId"` // 请求ID
	Binding   *Binding `json:"Binding"`   // 数据集和 COS Bucket 绑定关系信息的列表。
}

// DescribeDatasetBinding 查询数据集与存储桶的绑定关系
// https://cloud.tencent.com/document/product/460/106485
func (s *MetaInsightService) DescribeDatasetBinding(ctx context.Context, opt *DescribeDatasetBindingOptions) (*DescribeDatasetBindingResult, *Response, error) {
	var res DescribeDatasetBindingResult

	buf, resp, err := s.baseSend(ctx, opt, opt.OptHeaders, "/"+"datasetbinding", http.MethodGet)
	if buf.Len() > 0 {
		err = json.Unmarshal(buf.Bytes(), &res)
	}
	return &res, resp, err
}

type DescribeDatasetBindingsOptions struct {
	Datasetname string      `url:"datasetname, omitempty" json:"-"` // 数据集名称，同一个账户下唯一。
	Maxresults  int         `url:"maxresults, omitempty" json:"-"`  // 返回绑定关系的最大个数，取值范围为0~200。不设置此参数或者设置为0时，则默认值为100。
	Nexttoken   string      `url:"nexttoken, omitempty" json:"-"`   // 当绑定关系总数大于设置的MaxResults时，用于翻页的token。从NextToken开始按字典序返回绑定关系信息列表。第一次调用此接口时，设置为空。
	OptHeaders  *OptHeaders `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type DescribeDatasetBindingsResult struct {
	RequestId string     `json:"RequestId"` // 请求ID
	NextToken string     `json:"NextToken"` // 当绑定关系总数大于设置的MaxResults时，用于翻页的token。下一次列出绑定关系信息时以此值为NextToken，将未返回的结果返回。当绑定关系未全部返回时，此参数才有值。
	Bindings  []*Binding `json:"Bindings"`  // 数据集和 COS Bucket 绑定关系信息的列表。
}

// DescribeDatasetBindings 查询绑定关系列表
// https://cloud.tencent.com/document/product/460/106161
func (s *MetaInsightService) DescribeDatasetBindings(ctx context.Context, opt *DescribeDatasetBindingsOptions) (*DescribeDatasetBindingsResult, *Response, error) {
	var res DescribeDatasetBindingsResult

	buf, resp, err := s.baseSend(ctx, opt, opt.OptHeaders, "/"+"datasetbindings", http.MethodGet)
	if buf.Len() > 0 {
		err = json.Unmarshal(buf.Bytes(), &res)
	}
	return &res, resp, err
}

type DeleteDatasetBindingOptions struct {
	DatasetName string      `json:"DatasetName, omitempty" url:"-" ` // 数据集名称，同一个账户下唯一。
	URI         string      `json:"URI, omitempty" url:"-" `         // 资源标识字段，表示需要与数据集绑定的资源，当前仅支持COS存储桶，字段规则：cos://<BucketName>，其中BucketName表示COS存储桶名称，例如：cos://examplebucket-1250000000
	OptHeaders  *OptHeaders `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type DeleteDatasetBindingResult struct {
	RequestId string `json:"RequestId"` // 请求ID
}

// DeleteDatasetBinding 解绑存储桶与数据集
// https://cloud.tencent.com/document/product/460/106160
func (s *MetaInsightService) DeleteDatasetBinding(ctx context.Context, opt *DeleteDatasetBindingOptions) (*DeleteDatasetBindingResult, *Response, error) {
	var res DeleteDatasetBindingResult
	if opt == nil {
		return nil, nil, fmt.Errorf("opt param nil")
	}
	buf, resp, err := s.baseSend(ctx, opt, opt.OptHeaders, "/"+"datasetbinding", http.MethodDelete)
	if buf.Len() > 0 {
		err = json.Unmarshal(buf.Bytes(), &res)
	}
	return &res, resp, err
}

type DatasetSimpleQueryOptions struct {
	DatasetName  string          `json:"DatasetName, omitempty" url:"-" `  // 数据集名称，同一个账户下唯一。
	Query        *Query          `json:"Query, omitempty" url:"-" `        // 简单查询参数条件，可自嵌套。
	MaxResults   int             `json:"MaxResults, omitempty" url:"-" `   // 返回文件元数据的最大个数，取值范围为0200。 使用聚合参数时，该值表示返回分组的最大个数，取值范围为02000。 不设置此参数或者设置为0时，则取默认值100。
	NextToken    string          `json:"NextToken, omitempty" url:"-" `    // 当绑定关系总数大于设置的MaxResults时，用于翻页的token。从NextToken开始按字典序返回绑定关系信息列表。第一次调用此接口时，设置为空。
	Sort         string          `json:"Sort, omitempty" url:"-" `         // 排序字段列表。请参考字段和操作符的支持列表。 多个排序字段可使用半角逗号（,）分隔，例如：Size,Filename。 最多可设置5个排序字段。 排序字段顺序即为排序优先级顺序。
	Order        string          `json:"Order, omitempty" url:"-" `        // 排序字段的排序方式。取值如下： asc：升序； desc（默认）：降序。 多个排序方式可使用半角逗号（,）分隔，例如：asc,desc。 排序方式不可多于排序字段，即参数Order的元素数量需小于等于参数Sort的元素数量。例如Sort取值为Size,Filename时，Order可取值为asc,desc或asc。 排序方式少于排序字段时，未排序的字段默认取值asc。例如Sort取值为Size,Filename，Order取值为asc时，Filename默认排序方式为asc，即升序排列
	Aggregations []*Aggregations `json:"Aggregations, omitempty" url:"-" ` // 聚合字段信息列表。 当您使用聚合查询时，仅返回聚合结果，不再返回匹配到的元信息列表。
	WithFields   []string        `json:"WithFields, omitempty" url:"-" `   // 仅返回特定字段的值，而不是全部已有的元信息字段。可用于降低返回的结构体大小。不填或留空则返回所有字段。
	OptHeaders   *OptHeaders     `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type Aggregations struct {
	Operation  string      `json:"Operation, omitempty" url:"-" ` //  聚合字段的操作符。枚举值：min：最小值。max：最大值。average：平均数sum：求和。count：计数。distinct：去重计数。group：分组计数，按照分组计数结果从高到低排序。
	Field      string      `json:"Field, omitempty" url:"-" `     // 字段名称。关于支持的字段，请参考字段和操作符的支持列表。
	OptHeaders *OptHeaders `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type Query struct {
	Operation  string        `json:"Operation, omitempty" url:"-" `  // 操作运算符。枚举值： not：逻辑非。 or：逻辑或。 and：逻辑与。 lt：小于。 lte：小于等于。 gt：大于。 gte：大于等于。 eq：等于。 exist：存在性查询。 prefix：前缀查询。 match-phrase：字符串匹配查询。 nested：字段为数组时，其中同一对象内逻辑条件查询。
	SubQueries []*SubQueries `json:"SubQueries, omitempty" url:"-" ` // 子查询的结构体。 只有当Operations为逻辑运算符（and、or、not或nested）时，才能设置子查询条件。 在逻辑运算符为and/or/not时，其SubQueries内描述的所有条件需符合父级设置的and/or/not逻辑关系。 在逻辑运算符为nested时，其父级的Field必须为一个数组类的字段（如：Labels）。 子查询条件SubQueries组的Operation必须为and/or/not中的一个或多个，其Field必须为父级Field的子属性。
	Field      string        `json:"Field, omitempty" url:"-" `      // 字段名称。关于支持的字段，请参考字段和操作符的支持列表。
	Value      string        `json:"Value, omitempty" url:"-" `      // 查询的字段值。当Operations为逻辑运算符（and、or、not或nested）时，该字段无效。
	OptHeaders *OptHeaders   `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type SubQueries struct {
	Value      string      `json:"Value, omitempty" url:"-" `     // 查询的字段值。当Operations为逻辑运算符（and、or、not或nested）时，该字段无效。
	Operation  string      `json:"Operation, omitempty" url:"-" ` //  操作运算符。枚举值：not：逻辑非。or：逻辑或。and：逻辑与。lt：小于。lte：小于等于。gt：大于。gte：大于等于。eq：等于。exist：存在性查询。prefix：前缀查询。match-phrase：字符串匹配查询。nested：字段为数组时，其中同一对象内逻辑条件查询。
	Field      string      `json:"Field, omitempty" url:"-" `     // 字段名称。关于支持的字段，请参考字段和操作符的支持列表。
	OptHeaders *OptHeaders `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type DatasetSimpleQueryResult struct {
	RequestId    string                `json:"RequestId"`    // 请求ID
	Files        []*FileResult         `json:"Files"`        // 文件信息列表。仅在请求的Aggregations为空时返回。
	Aggregations []*AggregationsResult `json:"Aggregations"` // 聚合字段信息列表。仅在请求的Aggregations不为空时返回。
	NextToken    string                `json:"NextToken"`    // 翻页标记。当文件总数大于设置的MaxResults时，用于翻页的Token。符合条件的文件信息未全部返回时，此参数才有值。下一次列出文件信息时将此值作为NextToken传入，将后续的文件信息返回。
}

type AggregationsResult struct {
	Operation string    `json:"Operation"` //  聚合字段的聚合操作符。
	Value     float64   `json:"Value"`     //  聚合的统计结果。
	Groups    []*Groups `json:"Groups"`    //  分组聚合的结果列表。仅在请求的Aggregations中存在group类型的Operation时才会返回。
	Field     string    `json:"Field"`     // 聚合字段名称。
}

type Groups struct {
	Count int    `json:"Count"` //  分组聚合的总个数。
	Value string `json:"Value"` // 分组聚合的值。
}

type FileResult struct {
	ObjectId             string             `json:"ObjectId"`             // 对象唯一ID。
	CreateTime           string             `json:"CreateTime"`           //  元数据创建时间的时间戳，格式为RFC3339Nano
	UpdateTime           string             `json:"UpdateTime"`           //  元数据修改时间的时间戳，格式为RFC3339Nano创建元数据后，如果未更新过元数据，则元数据修改时间的时间戳和元数据创建时间的时间戳相同
	URI                  string             `json:"URI"`                  //   资源标识字段，表示需要建立索引的文件地址
	Filename             string             `json:"Filename"`             //   文件路径
	MediaType            string             `json:"MediaType"`            //    文件媒体类型。 枚举值：image：图片。other：其他。document：文档。archive：压缩包。audio：音频。video：视频。
	ContentType          string             `json:"ContentType"`          //   文件内容类型（MIME Type）。
	COSStorageClass      string             `json:"COSStorageClass"`      //   文件存储空间类型。
	COSCRC64             string             `json:"COSCRC64"`             //    文件CRC64值。
	Size                 int                `json:"Size"`                 //    文件大小，单位为字节。
	CacheControl         string             `json:"CacheControl"`         //    指定Object被下载时网页的缓存行为。该字段需要设置COS Object HTTP属性Cache-Control。
	ContentDisposition   string             `json:"ContentDisposition"`   //    指定Object被下载时的名称。需要设置COS Object HTTP属性Content-Disposition。
	ContentEncoding      string             `json:"ContentEncoding"`      //    指定该Object被下载时的内容编码格式。需要设置COS Object HTTP属性Content-Encoding。
	ContentLanguage      string             `json:"ContentLanguage"`      //    Object内容使用的语言。需要设置COS Object HTTP属性Content-Language。
	ServerSideEncryption string             `json:"ServerSideEncryption"` //    加密算法,需要设置x-cos-server-side-encryption。
	ETag                 string             `json:"ETag"`                 //    Object生成时会创建相应的ETag ，ETag用于标识一个Object的内容。
	FileModifiedTime     string             `json:"FileModifiedTime"`     //   文件最近一次修改时间的时间戳， 格式为RFC3339Nano。
	CustomId             string             `json:"CustomId"`             //   该文件的自定义ID。该文件索引到数据集后，作为该行元数据的属性存储，用于和您的业务系统进行关联、对应。您可以根据业务需求传入该值，例如将某个URI关联到您系统内的某个ID。推荐传入全局唯一的值。
	CustomLabels         *map[string]string `json:"CustomLabels"`         //   文件自定义标签列表。储存您业务自定义的键名、键值对信息，用于在查询时可以据此为筛选项进行检索。
	COSUserMeta          *map[string]string `json:"COSUserMeta"`          //   cos自定义头部。储存您业务在cos object上的键名、键值对信息，用于在查询时可以据此为筛选项进行检索。
	ObjectACL            string             `json:"ObjectACL"`            //    文件访问权限属性。
	COSTagging           *map[string]string `json:"COSTagging"`           //   cos自定义标签。储存您业务在cos object上的自定义标签的键名、键值对信息，用于在查询时可以据此为筛选项进行检索。
	COSTaggingCount      int                `json:"COSTaggingCount"`      //    cos自定义标签的数量。
	DatasetName          string             `json:"DatasetName"`          // 数据集名称。
}

// DatasetSimpleQuery 简单查询
// https://cloud.tencent.com/document/product/460/106375
func (s *MetaInsightService) DatasetSimpleQuery(ctx context.Context, opt *DatasetSimpleQueryOptions) (*DatasetSimpleQueryResult, *Response, error) {
	var res DatasetSimpleQueryResult
	if opt == nil {
		return nil, nil, fmt.Errorf("opt param nil")
	}
	buf, resp, err := s.baseSend(ctx, opt, opt.OptHeaders, "/"+"datasetquery"+"/"+"simple", http.MethodPost)
	if buf.Len() > 0 {
		err = json.Unmarshal(buf.Bytes(), &res)
	}
	return &res, resp, err
}

type DatasetFaceSearchOptions struct {
	DatasetName    string      `json:"DatasetName, omitempty" url:"-" `    // 数据集名称，同一个账户下唯一。
	URI            string      `json:"URI, omitempty" url:"-" `            // 资源标识字段，表示需要建立索引的文件地址。
	MaxFaceNum     int         `json:"MaxFaceNum, omitempty" url:"-" `     // 输入图片中检索的人脸数量，默认值为1(传0或不传采用默认值)，最大值为10。
	Limit          int         `json:"Limit, omitempty" url:"-" `          // 检索的每张人脸返回相关人脸数量，默认值为10，最大值为100。
	MatchThreshold int         `json:"MatchThreshold, omitempty" url:"-" ` // 出参 Score 中，只有超过 MatchThreshold 值的结果才会返回。范围：1-100，默认值为0，推荐值为80。
	OptHeaders     *OptHeaders `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type DatasetFaceSearchResult struct {
	FaceResult []*FaceResult `json:"FaceResult"` // 人脸检索识别结果信息列表。
	RequestId  string        `json:"RequestId"`  // 请求 ID。
}

type FaceResult struct {
	FaceInfos         []*FaceInfosInMeta `json:"FaceInfos"`         // 相关人脸信息列表。
	InputFaceBoundary *FaceBoundary      `json:"InputFaceBoundary"` // 输入图片的人脸框位置。
}

type FaceBoundary struct {
	Height int `json:"Height"` // 人脸高度。
	Width  int `json:"Width"`  // 人脸宽度。
	Left   int `json:"Left"`   // 人脸框左上角横坐标。
	Top    int `json:"Top"`    // 人脸框左上角纵坐标。
}

type FaceInfosInMeta struct {
	PersonId     string        `json:"PersonId"`     // 自定义人物ID。
	FaceBoundary *FaceBoundary `json:"FaceBoundary"` // 相关人脸框位置。
	FaceId       string        `json:"FaceId"`       // 人脸ID。
	Score        int           `json:"Score"`        // 相关人脸匹配得分。
	URI          string        `json:"URI"`          // 资源标识字段，表示需要建立索引的文件地址。
}

// DatasetFaceSearch 人脸搜索
// https://cloud.tencent.com/document/product/460/106166
func (s *MetaInsightService) DatasetFaceSearch(ctx context.Context, opt *DatasetFaceSearchOptions) (*DatasetFaceSearchResult, *Response, error) {
	var res DatasetFaceSearchResult
	if opt == nil {
		return nil, nil, fmt.Errorf("opt param nil")
	}
	buf, resp, err := s.baseSend(ctx, opt, opt.OptHeaders, "/"+"datasetquery"+"/"+"facesearch", http.MethodPost)
	if buf.Len() > 0 {
		err = json.Unmarshal(buf.Bytes(), &res)
	}
	return &res, resp, err
}

type SearchImageOptions struct {
	DatasetName    string      `json:"DatasetName, omitempty" url:"-" `    // 数据集名称，同一个账户下唯一。
	Mode           string      `json:"Mode, omitempty" url:"-" `           // 指定检索方式为图片或文本，pic 为图片检索，text 为文本检索，默认为 pic。
	URI            string      `json:"URI, omitempty" url:"-" `            // 资源标识字段，表示需要建立索引的文件地址(Mode 为 pic 时必选)。
	Limit          int         `json:"Limit, omitempty" url:"-" `          // 返回相关图片的数量，默认值为10，最大值为100。
	Text           string      `json:"Text, omitempty" url:"-" `           // 检索语句，检索方式为 text 时必填，最多支持60个字符 (Mode 为 text 时必选)。
	MatchThreshold int         `json:"MatchThreshold, omitempty" url:"-" ` // 出参 Score（相关图片匹配得分） 中，只有超过 MatchThreshold 值的结果才会返回。默认值为0，推荐值为80。
	OptHeaders     *OptHeaders `header:"-, omitempty" url:"-" json:"-" xml:"-"`
}

type SearchImageResult struct {
	ImageResult []*ImageResult `json:"ImageResult"` // 图像检索识别结果信息列表。
	RequestId   string         `json:"RequestId"`   // 请求ID。
}

type ImageResult struct {
	URI   string `json:"URI"`   // 资源标识字段，表示需要建立索引的文件地址。
	Score int    `json:"Score"` // 相关图片匹配得分。
}

// SearchImage 图像检索
// https://cloud.tencent.com/document/product/460/106376
func (s *MetaInsightService) SearchImage(ctx context.Context, opt *SearchImageOptions) (*SearchImageResult, *Response, error) {
	var res SearchImageResult
	if opt == nil {
		return nil, nil, fmt.Errorf("opt param nil")
	}
	buf, resp, err := s.baseSend(ctx, opt, opt.OptHeaders, "/"+"datasetquery"+"/"+"imagesearch", http.MethodPost)
	if buf.Len() > 0 {
		err = json.Unmarshal(buf.Bytes(), &res)
	}
	return &res, resp, err
}
