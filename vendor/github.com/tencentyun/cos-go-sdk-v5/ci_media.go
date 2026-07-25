package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/clbanning/mxj"
	"github.com/mitchellh/mapstructure"
)

type VodInfo struct {
	FileId   string `xml:"FileId,omitempty"`
	SubAppId string `xml:"SubAppId,omitempty"`
}

// JobInput TODO
type JobInput struct {
	Object     string `xml:"Object,omitempty"`
	Lang       string `xml:"Lang,omitempty"`
	Type       string `xml:"Type,omitempty"`
	BasicType  string `xml:"BasicType,omitempty"`
	CosHeaders []struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	} `xml:"CosHeaders"`
	Url string   `xml:"Url,omitempty"`
	Vod *VodInfo `xml:"Vod,omitempty"`
}

// StreamExtract TODO
type StreamExtract struct {
	Index  string `xml:"Index,omitempty"`
	Object string `xml:"Object,omitempty"`
}

// JobOutput TODO
type JobOutput struct {
	Region        string          `xml:"Region,omitempty"`
	Bucket        string          `xml:"Bucket,omitempty"`
	Object        string          `xml:"Object,omitempty"`
	SpriteObject  string          `xml:"SpriteObject,omitempty"`
	AuObject      string          `xml:"AuObject,omitempty"`
	BassObject    string          `xml:"BassObject,omitempty"`
	DrumObject    string          `xml:"DrumObject,omitempty"`
	StreamExtract []StreamExtract `xml:"StreamExtract,omitempty"`
}

// ClipConfig TODO
type ClipConfig struct {
	Duration string `xml:"Duration"`
}

// Container TODO
type Container struct {
	Format     string      `xml:"Format,omitempty"`
	ClipConfig *ClipConfig `xml:"ClipConfig,omitempty"`
}

// Video TODO
type Video struct {
	Codec                      string           `xml:"Codec"`
	Width                      string           `xml:"Width,omitempty"`
	Height                     string           `xml:"Height,omitempty"`
	Fps                        string           `xml:"Fps,omitempty"`
	Remove                     string           `xml:"Remove,omitempty"`
	Profile                    string           `xml:"Profile,omitempty"`
	Bitrate                    string           `xml:"Bitrate,omitempty"`
	Crf                        string           `xml:"Crf,omitempty"`
	Gop                        string           `xml:"Gop,omitempty"`
	Preset                     string           `xml:"Preset,omitempty"`
	Bufsize                    string           `xml:"Bufsize,omitempty"`
	Maxrate                    string           `xml:"Maxrate,omitempty"`
	HlsTsTime                  string           `xml:"HlsTsTime,omitempty"`
	DashSegment                string           `xml:"DashSegment,omitempty"`
	Pixfmt                     string           `xml:"Pixfmt,omitempty"`
	LongShortMode              string           `xml:"LongShortMode,omitempty"`
	Rotate                     string           `xml:"Rotate,omitempty"`
	AnimateOnlyKeepKeyFrame    string           `xml:"AnimateOnlyKeepKeyFrame,omitempty"`
	AnimateTimeIntervalOfFrame string           `xml:"AnimateTimeIntervalOfFrame,omitempty"`
	AnimateFramesPerSecond     string           `xml:"AnimateFramesPerSecond,omitempty"`
	Quality                    string           `xml:"Quality,omitempty"`
	Roi                        string           `xml:"Roi,omitempty"`
	Crop                       string           `xml:"Crop,omitempty"`
	Interlaced                 string           `xml:"Interlaced,omitempty"`
	ColorParam                 *VideoColorParam `xml:"ColorParam,omitempty"`
}

type VideoColorParam struct {
	ColorRange     string `xml:"ColorRange,omitempty"`
	ColorSpace     string `xml:"ColorSpace,omitempty"`
	ColorTrc       string `xml:"ColorTrc,omitempty"`
	ColorPrimaries string `xml:"ColorPrimaries,omitempty"`
}

// TranscodeProVideo TODO
type TranscodeProVideo struct {
	Codec      string `xml:"Codec,omitempty"`
	Profile    string `xml:"Profile,omitempty"`
	Width      string `xml:"Width,omitempty"`
	Height     string `xml:"Height,omitempty"`
	Interlaced string `xml:"Interlaced,omitempty"`
	Fps        string `xml:"Fps,omitempty"`
	Bitrate    string `xml:"Bitrate,omitempty"`
	Rotate     string `xml:"Rotate,omitempty"`
}

// TimeInterval TODO
type TimeInterval struct {
	Start    string `xml:"Start,omitempty"`
	Duration string `xml:"Duration,omitempty"`
}

// Audio TODO
type Audio struct {
	Codec         string `xml:"Codec,omitempty"`
	Samplerate    string `xml:"Samplerate,omitempty"`
	Bitrate       string `xml:"Bitrate,omitempty"`
	Channels      string `xml:"Channels,omitempty"`
	Remove        string `xml:"Remove,omitempty"`
	KeepTwoTracks string `xml:"KeepTwoTracks,omitempty"`
	SwitchTrack   string `xml:"SwitchTrack,omitempty"`
	SampleFormat  string `xml:"SampleFormat,omitempty"`
}

// TranscodeProAudio TODO
type TranscodeProAudio struct {
	Codec  string `xml:"Codec,omitempty"`
	Remove string `xml:"Remove,omitempty"`
}

// TransConfig TODO
type TransConfig struct {
	AdjDarMethod          string      `xml:"AdjDarMethod,omitempty"`
	IsCheckReso           string      `xml:"IsCheckReso,omitempty"`
	ResoAdjMethod         string      `xml:"ResoAdjMethod,omitempty"`
	IsCheckVideoBitrate   string      `xml:"IsCheckVideoBitrate,omitempty"`
	VideoBitrateAdjMethod string      `xml:"VideoBitrateAdjMethod,omitempty"`
	IsCheckAudioBitrate   string      `xml:"IsCheckAudioBitrate,omitempty"`
	AudioBitrateAdjMethod string      `xml:"AudioBitrateAdjMethod,omitempty"`
	DeleteMetadata        string      `xml:"DeleteMetadata,omitempty"`
	IsHdr2Sdr             string      `xml:"IsHdr2Sdr,omitempty"`
	HlsEncrypt            *HlsEncrypt `xml:"HlsEncrypt,omitempty"`
}

// Transcode TODO
type Transcode struct {
	Container     *Container    `xml:"Container,omitempty"`
	Video         *Video        `xml:"Video,omitempty"`
	TimeInterval  *TimeInterval `xml:"TimeInterval,omitempty"`
	Audio         *Audio        `xml:"Audio,omitempty"`
	TransConfig   *TransConfig  `xml:"TransConfig,omitempty"`
	AudioMix      *AudioMix     `xml:"AudioMix,omitempty"`
	AudioMixArray []AudioMix    `xml:"AudioMixArray,omitempty"`
}

// TranscodePro TODO
type TranscodePro struct {
	Container    *Container         `xml:"Container,omitempty"`
	Video        *TranscodeProVideo `xml:"Video,omitempty"`
	Audio        *TranscodeProAudio `xml:"Audio,omitempty"`
	TimeInterval *TimeInterval      `xml:"TimeInterval,omitempty"`
	TransConfig  *TransConfig       `xml:"TransConfig,omitempty"`
}

// WatermarkSlideConfig TODO
type WatermarkSlideConfig struct {
	SlideMode   string `xml:"SlideMode,omitempty"`
	XSlideSpeed string `xml:"XSlideSpeed,omitempty"`
	YSlideSpeed string `xml:"YSlideSpeed,omitempty"`
}

// Image TODO
type Image struct {
	Url          string `xml:"Url,omitempty"`
	Mode         string `xml:"Mode,omitempty"`
	Width        string `xml:"Width,omitempty"`
	Height       string `xml:"Height,omitempty"`
	Transparency string `xml:"Transparency,omitempty"`
	Background   string `xml:"Background,omitempty"`
}

// Text TODO
type Text struct {
	FontSize     string `xml:"FontSize,omitempty"`
	FontType     string `xml:"FontType,omitempty"`
	FontColor    string `xml:"FontColor,omitempty"`
	Transparency string `xml:"Transparency,omitempty"`
	Text         string `xml:"Text,omitempty"`
}

// TtsTpl TODO
type TtsTpl struct {
	Mode      string `xml:"Mode,omitempty"`
	Codec     string `xml:"Codec,omitempty"`
	VoiceType string `xml:"VoiceType,omitempty"`
	Volume    string `xml:"Volume,omitempty"`
	Speed     string `xml:"Speed,omitempty"`
}

// TtsConfig TODO
type TtsConfig struct {
	Input     string `xml:"Input,omitempty"`
	InputType string `xml:"InputType,omitempty"`
}

// Translation TODO
type Translation struct {
	Lang string `xml:"Lang,omitempty"`
	Type string `xml:"Type,omitempty"`
}

// WordsGeneralize TODO
type WordsGeneralize struct {
	NerMethod string `xml:"NerMethod,omitempty"`
	SegMethod string `xml:"SegMethod,omitempty"`
}

// WordsGeneralizeResult TODO
type WordsGeneralizeResult struct {
	WordsGeneralizeLable []WordsGeneralizeResulteLable `xml:"WordsGeneralizeLable,omitempty"`
	WordsGeneralizeToken []WordsGeneralizeResulteToken `xml:"WordsGeneralizeToken,omitempty"`
}

// WordsGeneralizeResulteLable TODO
type WordsGeneralizeResulteLable struct {
	Category string `xml:"Category,omitempty"`
	Word     string `xml:"Word,omitempty"`
}

// WordsGeneralizeResulteToken TODO
type WordsGeneralizeResulteToken struct {
	Length string `xml:"Length,omitempty"`
	Offset string `xml:"Offset,omitempty"`
	Pos    string `xml:"Pos,omitempty"`
	Word   string `xml:"Word,omitempty"`
}

// Watermark TODO
type Watermark struct {
	Type        string                `xml:"Type,omitempty"`
	Pos         string                `xml:"Pos,omitempty"` // TopLeft：左上; Top：上居中; TopRight：右上; Left：左居中; Center：正中心; Right：右居中; BottomLeft：左下; Bottom：下居中; BottomRight：右下
	LocMode     string                `xml:"LocMode,omitempty"`
	Dx          string                `xml:"Dx,omitempty"`
	Dy          string                `xml:"Dy,omitempty"`
	StartTime   string                `xml:"StartTime,omitempty"`
	EndTime     string                `xml:"EndTime,omitempty"`
	SlideConfig *WatermarkSlideConfig `xml:"SlideConfig,omitempty"`
	Image       *Image                `xml:"Image,omitempty"`
	Text        *Text                 `xml:"Text,omitempty"`
}

// EffectConfig TODO
type EffectConfig struct {
	EnableStartFadein string `xml:"EnableStartFadein,omitempty"`
	StartFadeinTime   string `xml:"StartFadeinTime,omitempty"`
	EnableEndFadeout  string `xml:"EnableEndFadeout,omitempty"`
	EndFadeoutTime    string `xml:"EndFadeoutTime,omitempty"`
	EnableBgmFade     string `xml:"EnableBgmFade,omitempty"`
	BgmFadeTime       string `xml:"BgmFadeTime,omitempty"`
}

// AudioMix TODO
type AudioMix struct {
	AudioSource  string        `xml:"AudioSource,omitempty"`
	MixMode      string        `xml:"MixMode,omitempty"`
	Replace      string        `xml:"Replace,omitempty"`
	EffectConfig *EffectConfig `xml:"EffectConfig,omitempty"`
}

// ConcatFragment TODO
type ConcatFragment struct {
	Url           string `xml:"Url,omitempty"`
	Mode          string `xml:"Mode,omitempty"`
	StartTime     string `xml:"StartTime,omitempty"`
	EndTime       string `xml:"EndTime,omitempty"`
	FragmentIndex string `xml:"FragmentIndex,omitempty"`
	Duration      string `xml:"Duration,omitempty"`
}

// ConcatTemplate TODO
type ConcatTemplate struct {
	ConcatFragment  []ConcatFragment `xml:"ConcatFragment,omitempty"`
	Audio           *Audio           `xml:"Audio,omitempty"`
	Video           *Video           `xml:"Video,omitempty"`
	Container       *Container       `xml:"Container,omitempty"`
	Index           string           `xml:"Index,omitempty"`
	AudioMix        *AudioMix        `xml:"AudioMix,omitempty"`
	AudioMixArray   []AudioMix       `xml:"AudioMixArray,omitempty"`
	SceneChangeInfo *SceneChangeInfo `xml:"SceneChangeInfo,omitempty"`
	DirectConcat    string           `xml:"DirectConcat,omitempty"`
}

// SceneChangeInfo 转场参数
type SceneChangeInfo struct {
	Mode           string `xml:"Mode,omitempty"`
	Time           string `xml:"Time,omitempty"`
	TransitionType string `xml:"TransitionType,omitempty"`
}

// SpriteSnapshotConfig TODO
type SpriteSnapshotConfig struct {
	CellHeight  string `xml:"CellHeight,omitempty"`
	CellWidth   string `xml:"CellWidth,omitempty"`
	Color       string `xml:"Color,omitempty"`
	Columns     string `xml:"Columns,omitempty"`
	Lines       string `xml:"Lines,omitempty"`
	Margin      string `xml:"Margin,omitempty"`
	Padding     string `xml:"Padding,omitempty"`
	ScaleMethod string `xml:"ScaleMethod,omitempty"`
}

// Snapshot TODO
type Snapshot struct {
	Mode                 string                `xml:"Mode,omitempty"`
	Start                string                `xml:"Start,omitempty"`
	TimeInterval         string                `xml:"TimeInterval,omitempty"`
	Count                string                `xml:"Count,omitempty"`
	Width                string                `xml:"Width,omitempty"`
	Height               string                `xml:"Height,omitempty"`
	CIParam              string                `xml:"CIParam,omitempty"`
	IsCheckCount         bool                  `xml:"IsCheckCount,omitempty"`
	IsCheckBlack         bool                  `xml:"IsCheckBlack,omitempty"`
	BlackLevel           string                `xml:"BlackLevel,omitempty"`
	PixelBlackThreshold  string                `xml:"PixelBlackThreshold,omitempty"`
	SnapshotOutMode      string                `xml:"SnapshotOutMode,omitempty"`
	SpriteSnapshotConfig *SpriteSnapshotConfig `xml:"SpriteSnapshotConfig,omitempty"`
}

// AnimationVideo TODO
// 有意和转码区分，两种任务关注的参数不一样避免干扰
type AnimationVideo struct {
	Codec                      string `xml:"Codec,omitempty"`
	Width                      string `xml:"Width,omitempty"`
	Height                     string `xml:"Height,omitempty"`
	Fps                        string `xml:"Fps,omitempty"`
	AnimateOnlyKeepKeyFrame    string `xml:"AnimateOnlyKeepKeyFrame,omitempty"`
	AnimateTimeIntervalOfFrame string `xml:"AnimateTimeIntervalOfFrame,omitempty"`
	AnimateFramesPerSecond     string `xml:"AnimateFramesPerSecond,omitempty"`
	Quality                    string `xml:"Quality,omitempty"`
}

// Animation TODO
type Animation struct {
	Container    *Container      `xml:"Container,omitempty"`
	Video        *AnimationVideo `xml:"Video,omitempty"`
	TimeInterval *TimeInterval   `xml:"TimeInterval,omitempty"`
}

// HlsEncrypt TODO
type HlsEncrypt struct {
	IsHlsEncrypt bool   `xml:"IsHlsEncrypt,omitempty"`
	UriKey       string `xml:"UriKey,omitempty"`
}

// Segment TODO
type Segment struct {
	Format         string      `xml:"Format,omitempty"`
	Duration       string      `xml:"Duration,omitempty"`
	TranscodeIndex string      `xml:"TranscodeIndex,omitempty"`
	HlsEncrypt     *HlsEncrypt `xml:"HlsEncrypt,omitempty"`
	StartTime      string      `xml:"StartTime,omitempty"`
	EndTime        string      `xml:"EndTime,omitempty"`
}

// VideoMontageVideo TODO
type VideoMontageVideo struct {
	Codec   string `xml:"Codec"`
	Width   string `xml:"Width"`
	Height  string `xml:"Height"`
	Fps     string `xml:"Fps"`
	Remove  string `xml:"Remove,omitempty"`
	Bitrate string `xml:"Bitrate"`
	Crf     string `xml:"Crf"`
}

// VideoMontage TODO
type VideoMontage struct {
	Container     *Container         `xml:"Container,omitempty"`
	Video         *VideoMontageVideo `xml:"Video,omitempty"`
	Audio         *Audio             `xml:"Audio,omitempty"`
	Duration      string             `xml:"Duration,omitempty"`
	Scene         string             `xml:"Scene,omitempty"`
	AudioMix      *AudioMix          `xml:"AudioMix,omitempty"`
	AudioMixArray []AudioMix         `xml:"AudioMixArray,omitempty"`
}

// AudioConfig TODO
type AudioConfig struct {
	Codec      string `xml:"Codec"`
	Samplerate string `xml:"Samplerate"`
	Bitrate    string `xml:"Bitrate"`
	Channels   string `xml:"Channels"`
}

// VoiceSeparate TODO
type VoiceSeparate struct {
	AudioMode   string       `xml:"AudioMode,omitempty"` // IsAudio 人声, IsBackground 背景声, AudioAndBackground 人声和背景声
	AudioConfig *AudioConfig `xml:"AudioConfig,omitempty"`
}

// ColorEnhance TODO
type ColorEnhance struct {
	Enable     string `xml:"Enable,omitempty"`
	Contrast   string `xml:"Contrast,omitempty"`
	Correction string `xml:"Correction,omitempty"`
	Saturation string `xml:"Saturation,omitempty"`
}

// MsSharpen TODO
type MsSharpen struct {
	Enable       string `xml:"Enable,omitempty"`
	SharpenLevel string `xml:"SharpenLevel,omitempty"`
}

// VideoProcess TODO
type VideoProcess struct {
	ColorEnhance *ColorEnhance `xml:"ColorEnhance,omitempty"`
	MsSharpen    *MsSharpen    `xml:"MsSharpen,omitempty"`
}

// SDRtoHDR TODO
type SDRtoHDR struct {
	HdrMode string `xml:"HdrMode,omitempty"` // HLG、HDR10
}

// SuperResolution TODO
type SuperResolution struct {
	Resolution    string `xml:"Resolution,omitempty"` // sdtohd、hdto4k
	EnableScaleUp string `xml:"EnableScaleUp,omitempty"`
	Version       string `xml:"Version,omitempty"`
}

// DigitalWatermark TODO
type DigitalWatermark struct {
	Message string `xml:"Message"`
	Type    string `xml:"Type"`
	Version string `xml:"Version"`
}

// ExtractDigitalWatermark TODO
type ExtractDigitalWatermark struct {
	Type    string `xml:"Type"`
	Version string `xml:"Version"`
}

// Subtitles TODO
type Subtitles struct {
	Subtitle []Subtitle `xml:"Subtitle,omitempty"`
}

// Subtitle TODO
type Subtitle struct {
	Url          string `xml:"Url,omitempty"`
	Embed        string `xml:"Embed,omitempty"`
	FontType     string `xml:"FontType,omitempty"`
	FontSize     string `xml:"FontSize,omitempty"`
	FontColor    string `xml:"FontColor,omitempty"`
	OutlineColor string `xml:"OutlineColor,omitempty"`
	VMargin      string `xml:"VMargin,omitempty"`
}

// VideoTag TODO
type VideoTag struct {
	Scenario string `xml:"Scenario,omitempty"` // 1. Stream 2. ShortVideo
	Text     string `xml:"Text,omitempty"`
}

// VideoTagResult TODO
type VideoTagResult struct {
	StreamData *VideoTagResultStreamData `xml:"StreamData,omitempty"`
}

// VideoTagResultStreamData TODO
type VideoTagResultStreamData struct {
	SubErrCode string                        `xml:"SubErrCode,omitempty"`
	SubErrMsg  string                        `xml:"SubErrMsg,omitempty"`
	Data       *VideoTagResultStreamDataData `xml:"Data,omitempty"`
}

// VideoTagResultStreamDataData TODO
type VideoTagResultStreamDataData struct {
	Tags       []VideoTagResultStreamDataDataTags       `xml:"Tags,omitempty"`
	PersonTags []VideoTagResultStreamDataDataPersonTags `xml:"PersonTags,omitempty"`
	PlaceTags  []VideoTagResultStreamDataDataPlaceTags  `xml:"PlaceTags,omitempty"`
	ActionTags []VideoTagResultStreamDataDataActionTags `xml:"ActionTags,omitempty"`
	ObjectTags []VideoTagResultStreamDataDataObjectTags `xml:"ObjectTags,omitempty"`
	Labels     []VideoTagResultStreamDataDataLabels     `xml:"Labels,omitempty"`
}

// VideoTagResultStreamDataDataTags TODO
type VideoTagResultStreamDataDataTags struct {
	Tag        string  `xml:"Tag,omitempty"`
	TagCls     string  `xml:"TagCls,omitempty"`
	Confidence float64 `xml:"Confidence,omitempty"`
}

// VideoTagResultStreamDataDataPersonTags TODO
type VideoTagResultStreamDataDataPersonTags struct {
	Name            string                                                  `xml:"Name,omitempty"`
	Confidence      float64                                                 `xml:"Confidence,omitempty"`
	Count           string                                                  `xml:"Count,omitempty"`
	DetailPerSecond []VideoTagResultStreamDataDataPersonTagsDetailPerSecond `xml:"DetailPerSecond,omitempty"`
}

// VideoTagResultStreamDataDataPersonTagsDetailPerSecond TODO
type VideoTagResultStreamDataDataPersonTagsDetailPerSecond struct {
	TimeStamp  string                                                      `xml:"TimeStamp,omitempty"`
	Name       string                                                      `xml:"Name,omitempty"`
	Confidence float64                                                     `xml:"Confidence,omitempty"`
	BBox       []VideoTagResultStreamDataDataPersonTagsDetailPerSecondBBox `xml:"BBox,omitempty"`
}

// VideoTagResultStreamDataDataPersonTags TODO
type VideoTagResultStreamDataDataPersonTagsDetailPerSecondBBox struct {
	X1 string `xml:"X1,omitempty"`
	X2 string `xml:"X2,omitempty"`
	Y1 string `xml:"Y1,omitempty"`
	Y2 string `xml:"Y2,omitempty"`
}

// VideoTagResultStreamDataDataPlaceTags TODO
type VideoTagResultStreamDataDataPlaceTags struct {
	Tags            []VideoTagResultStreamDataDataTags `xml:"Tags,omitempty"`
	ClipFrameResult []string                           `xml:"ClipFrameResult,omitempty"`
	StartTime       string                             `xml:"StartTime,omitempty"`
	EndTime         string                             `xml:"EndTime,omitempty"`
	StartIndex      string                             `xml:"StartIndex,omitempty"`
	EndIndex        string                             `xml:"EndIndex,omitempty"`
}

// VideoTagResultStreamDataDataActionTags TODO
type VideoTagResultStreamDataDataActionTags struct {
	Tags      []VideoTagResultStreamDataDataTags `xml:"Tags,omitempty"`
	StartTime string                             `xml:"StartTime,omitempty"`
	EndTime   string                             `xml:"EndTime,omitempty"`
}

// VideoTagResultStreamDataDataObjectTags TODO
type VideoTagResultStreamDataDataObjectTags struct {
	Objects   []VideoTagResultStreamDataDataPersonTagsDetailPerSecond `xml:"Objects,omitempty"`
	TimeStamp string                                                  `xml:"TimeStamp,omitempty"`
}

// VideoTagResultStreamDataDataLabels TODO
type VideoTagResultStreamDataDataLabels struct {
	First      string  `xml:"First,omitempty"`
	Second     string  `xml:"Second,omitempty"`
	Confidence float64 `xml:"Confidence,omitempty"`
}

// QualityEstimate TODO
type QualityEstimate struct {
	Score         string `xml:"Score,omitempty"`
	VqaPlusResult struct {
		NoAudio        bool `xml:"NoAudio,omitempty"`
		NoVideo        bool `xml:"NoVideo,omitempty"`
		DetailedResult []struct {
			Type  string `xml:"Type,omitempty"`
			Items []struct {
				Confidence      int     `xml:"Confidence,omitempty"`
				StartTimeOffset float32 `xml:"StartTimeOffset,omitempty"`
				EndTimeOffset   float32 `xml:"EndTimeOffset,omitempty"`
				AreaCoordSet    []int   `xml:"AreaCoordSet,omitempty"`
			} `xml:"Items,omitempty"`
		} `xml:"DetailedResult,omitempty"`
	} `xml:"VqaPlusResult,omitempty"`
}

// QualityEstimateConfig 视频质量检查的参数
type QualityEstimateConfig struct {
	Rotate string `xml:"Rotate,omitempty"`
	Mode   string `xml:"Mode,omitempty"`
}

// MediaResult TODO
type MediaResult struct {
	OutputFile struct {
		Bucket  string `xml:"Bucket,omitempty"`
		Md5Info []struct {
			Md5        string `xml:"Md5,omitempty"`
			ObjectName string `xml:"ObjectName,omitempty"`
		} `xml:"Md5Info,omitempty"`
		ObjectName       []string `xml:"ObjectName,omitempty"`
		ObjectPrefix     string   `xml:"ObjectPrefix,omitempty"`
		Region           string   `xml:"Region,omitempty"`
		SpriteOutputFile struct {
			Bucket  string `xml:"Bucket,omitempty"`
			Md5Info []struct {
				Md5        string `xml:"Md5,omitempty"`
				ObjectName string `xml:"ObjectName,omitempty"`
			} `xml:"Md5Info,omitempty"`
			ObjectName   []string `xml:"ObjectName,omitempty"`
			ObjectPrefix string   `xml:"ObjectPrefix,omitempty"`
			Region       string   `xml:"Region,omitempty"`
		} `xml:"SpriteOutputFile,omitempty"`
	} `xml:"OutputFile,omitempty"`
}

// MediaInfo TODO
type MediaInfo struct {
	Format struct {
		Bitrate        string `xml:"Bitrate"`
		Duration       string `xml:"Duration"`
		FormatLongName string `xml:"FormatLongName"`
		FormatName     string `xml:"FormatName"`
		NumProgram     string `xml:"NumProgram"`
		NumStream      string `xml:"NumStream"`
		Size           string `xml:"Size"`
		StartTime      string `xml:"StartTime"`
	} `xml:"Format"`
	Stream struct {
		Audio []struct {
			Bitrate        string `xml:"Bitrate"`
			Channel        string `xml:"Channel"`
			ChannelLayout  string `xml:"ChannelLayout"`
			CodecLongName  string `xml:"CodecLongName"`
			CodecName      string `xml:"CodecName"`
			CodecTag       string `xml:"CodecTag"`
			CodecTagString string `xml:"CodecTagString"`
			CodecTimeBase  string `xml:"CodecTimeBase"`
			Duration       string `xml:"Duration"`
			Index          string `xml:"Index"`
			Language       string `xml:"Language"`
			SampleFmt      string `xml:"SampleFmt"`
			SampleRate     string `xml:"SampleRate"`
			StartTime      string `xml:"StartTime"`
			Timebase       string `xml:"Timebase"`
		} `xml:"Audio"`
		Subtitle string `xml:"Subtitle"`
		Video    []struct {
			AvgFps         string `xml:"AvgFps"`
			Bitrate        string `xml:"Bitrate"`
			CodecLongName  string `xml:"CodecLongName"`
			CodecName      string `xml:"CodecName"`
			CodecTag       string `xml:"CodecTag"`
			CodecTagString string `xml:"CodecTagString"`
			CodecTimeBase  string `xml:"CodecTimeBase"`
			Dar            string `xml:"Dar"`
			Duration       string `xml:"Duration"`
			Fps            string `xml:"Fps"`
			HasBFrame      string `xml:"HasBFrame"`
			Height         string `xml:"Height"`
			Index          string `xml:"Index"`
			Language       string `xml:"Language"`
			Level          string `xml:"Level"`
			NumFrames      string `xml:"NumFrames"`
			PixFormat      string `xml:"PixFormat"`
			Profile        string `xml:"Profile"`
			RefFrames      string `xml:"RefFrames"`
			Rotation       string `xml:"Rotation"`
			Sar            string `xml:"Sar"`
			StartTime      string `xml:"StartTime"`
			Timebase       string `xml:"Timebase"`
			Width          string `xml:"Width"`
			ColorRange     string `xml:"ColorRange"`
			ColorTransfer  string `xml:"ColorTransfer"`
			ColorPrimaries string `xml:"ColorPrimaries"`
		} `xml:"Video"`
	} `xml:"Stream"`
}

// PicProcess TODO
type PicProcess struct {
	IsPicInfo   string `xml:"IsPicInfo,omitempty"`
	ProcessRule string `xml:"ProcessRule,omitempty"`
}

// PicProcessResult TODO
type PicProcessResult struct {
	UploadResult struct {
		OriginalInfo struct {
			Key       string `xml:"Key"`
			Location  string `xml:"Location"`
			ETag      string `xml:"ETag"`
			ImageInfo struct {
				Format      string `xml:"Format"`
				Width       int32  `xml:"Width"`
				Height      int32  `xml:"Height"`
				Quality     int32  `xml:"Quality"`
				Ave         string `xml:"Ave"`
				Orientation int32  `xml:"Orientation"`
			} `xml:"ImageInfo"`
		} `xml:"OriginalInfo"`
		ProcessResults struct {
			Object struct {
				Key      string `xml:"Key"`
				Location string `xml:"Location"`
				Format   string `xml:"Format"`
				Width    int32  `xml:"Width"`
				Height   int32  `xml:"Height"`
				Size     int32  `xml:"Size"`
				Quality  int32  `xml:"Quality"`
				Etag     string `xml:"Etag"`
			} `xml:"Object"`
		} `xml:"ProcessResults"`
	} `xml:"UploadResult"`
}

// PosterProduction TODO
type PosterProduction struct {
	TemplateId string      `xml:"TemplateId,omitempty"`
	Info       interface{} `xml:"Info,omitempty"`
}

// PicProcessJobOperation TODO
type PicProcessJobOperation struct {
	TemplateId       string            `xml:"TemplateId,omitempty"`
	PicProcess       *PicProcess       `xml:"PicProcess,omitempty"`
	Output           *JobOutput        `xml:"Output,omitempty"`
	UserData         string            `xml:"UserData,omitempty"`
	JobLevel         int               `xml:"JobLevel,omitempty"`
	PicProcessResult *PicProcessResult `xml:"PicProcessResult,omitempty"`
	PosterProduction *PosterProduction `xml:"PosterProduction,omitempty"`
}

// MediaProcessJobOperation TODO
type MediaProcessJobOperation struct {
	Tag                     string                   `xml:"Tag,omitempty"`
	Output                  *JobOutput               `xml:"Output,omitempty"`
	MediaResult             *MediaResult             `xml:"MediaResult,omitempty"`
	MediaInfo               *MediaInfo               `xml:"MediaInfo,omitempty"`
	Transcode               *Transcode               `xml:"Transcode,omitempty"`
	Watermark               []Watermark              `xml:"Watermark,omitempty"`
	TemplateId              string                   `xml:"TemplateId,omitempty"`
	WatermarkTemplateId     []string                 `xml:"WatermarkTemplateId,omitempty"`
	ConcatTemplate          *ConcatTemplate          `xml:"ConcatTemplate,omitempty"`
	Snapshot                *Snapshot                `xml:"Snapshot,omitempty"`
	Animation               *Animation               `xml:"Animation,omitempty"`
	Segment                 *Segment                 `xml:"Segment,omitempty"`
	VideoMontage            *VideoMontage            `xml:"VideoMontage,omitempty"`
	VoiceSeparate           *VoiceSeparate           `xml:"VoiceSeparate,omitempty"`
	VideoProcess            *VideoProcess            `xml:"VideoProcess,omitempty"`
	TranscodeTemplateId     string                   `xml:"TranscodeTemplateId,omitempty"` // 视频增强、超分、SDRtoHDR任务类型，可以选择转码模板相关参数
	SDRtoHDR                *SDRtoHDR                `xml:"SDRtoHDR,omitempty"`
	SuperResolution         *SuperResolution         `xml:"SuperResolution,omitempty"`
	DigitalWatermark        *DigitalWatermark        `xml:"DigitalWatermark,omitempty"`
	ExtractDigitalWatermark *ExtractDigitalWatermark `xml:"ExtractDigitalWatermark,omitempty"`
	VideoTag                *VideoTag                `xml:"VideoTag,omitempty"`
	VideoTagResult          *VideoTagResult          `xml:"VideoTagResult,omitempty"`
	SmartCover              *NodeSmartCover          `xml:"SmartCover,omitempty"`
	UserData                string                   `xml:"UserData,omitempty"`
	JobLevel                int                      `xml:"JobLevel,omitempty"`
	QualityEstimate         *QualityEstimate         `xml:"QualityEstimate,omitempty"`
	TtsTpl                  *TtsTpl                  `xml:"TtsTpl,omitempty"`
	TtsConfig               *TtsConfig               `xml:"TtsConfig,omitempty"`
	Translation             *Translation             `xml:"Translation,omitempty"`
	WordsGeneralize         *WordsGeneralize         `xml:"WordsGeneralize,omitempty"`
	WordsGeneralizeResult   *WordsGeneralizeResult   `xml:"WordsGeneralizeResult,omitempty"`
	QualityEstimateConfig   *QualityEstimateConfig   `xml:"QualityEstimateConfig,omitempty"`
	NoiseReduction          *NoiseReduction          `xml:"NoiseReduction,omitempty"`
	SplitVideoParts         *SplitVideoParts         `xml:"SplitVideoParts,omitempty"`
	SplitVideoInfoResult    *SplitVideoInfoResult    `xml:"SplitVideoInfoResult,omitempty"`
	Subtitles               *Subtitles               `xml:"Subtitles,omitempty"`
	VideoEnhance            *VideoEnhance            `xml:"VideoEnhance,omitempty"`
	VideoTargetRec          *VideoTargetRec          `xml:"VideoTargetRec,omitempty"`
	VideoTargetRecResult    *VideoTargetRecResult    `xml:"VideoTargetRecResult,omitempty"`
	SegmentVideoBody        *SegmentVideoBody        `xml:"SegmentVideoBody,omitempty"`
	FreeTranscode           string                   `xml:"FreeTranscode,omitempty"`
	PicProcess              *PicProcess              `xml:"PicProcess,omitempty"`
	PicProcessResult        *PicProcessResult        `xml:"PicProcessResult,omitempty"`
	PosterProduction        *PosterProduction        `xml:"PosterProduction,omitempty"`
	SpeechRecognition       *SpeechRecognition       `xml:"SpeechRecognition,omitempty"`
	SpeechRecognitionResult *SpeechRecognitionResult `xml:"SpeechRecognitionResult,omitempty"`
	SoundHoundResult        *SoundHoundResult        `xml:"SoundHoundResult,omitempty"`
	FillConcat              *FillConcat              `xml:"FillConcat,omitempty"`
	VideoSynthesis          *VideoSynthesis          `xml:"VideoSynthesis,omitempty"`
	DnaConfig               *DnaConfig               `xml:"DnaConfig,omitempty"`
	DnaResult               *DnaResult               `xml:"DnaResult,omitempty"`
	VocalScore              *VocalScore              `xml:"VocalScore,omitempty"`
	VocalScoreResult        *VocalScoreResult        `xml:"VocalScoreResult,omitempty"`
	ImageInspect            *ImageInspect            `xml:"ImageInspect,omitempty"`
	ImageInspectResult      *ImageInspectResult      `xml:"ImageInspectResult,omitempty"`
	SnapshotPrefix          string                   `xml:"SnapshotPrefix,omitempty"`
	ImageOCR                *ImageOCRTemplate        `xml:"ImageOCR,omitempty"`
	Detection               *ImageOCRDetection       `xml:"Detection,omitempty"`
	CustomId                string                   `xml:"CustomId,omitempty"`
}

// CreatePicJobsOptions TODO
type CreatePicJobsOptions struct {
	XMLName             xml.Name                      `xml:"Request"`
	Tag                 string                        `xml:"Tag,omitempty"`
	Input               *JobInput                     `xml:"Input,omitempty"`
	Operation           *PicProcessJobOperation       `xml:"Operation,omitempty"`
	QueueId             string                        `xml:"QueueId,omitempty"`
	CallBack            string                        `xml:"CallBack,omitempty"`
	QueueType           string                        `xml:"QueueType,omitempty"`
	CallBackFormat      string                        `xml:"CallBackFormat,omitempty"`
	CallBackType        string                        `xml:"CallBackType,omitempty"`
	CallBackMqConfig    *NotifyConfigCallBackMqConfig `xml:"CallBackMqConfig,omitempty"`
	CallBackKafkaConfig *KafkaConfig                  `xml:"CallBackKafkaConfig,omitempty"`
}

// CreateAIJobsOptions TODO
type CreateAIJobsOptions CreateMediaJobsOptions

// CreateMediaJobsOptions TODO
type CreateMediaJobsOptions struct {
	XMLName             xml.Name                      `xml:"Request"`
	Tag                 string                        `xml:"Tag,omitempty"`
	Input               *JobInput                     `xml:"Input,omitempty"`
	Operation           *MediaProcessJobOperation     `xml:"Operation,omitempty"`
	QueueId             string                        `xml:"QueueId,omitempty"`
	QueueType           string                        `xml:"QueueType,omitempty"`
	CallBackFormat      string                        `xml:"CallBackFormat,omitempty"`
	CallBackType        string                        `xml:"CallBackType,omitempty"`
	CallBack            string                        `xml:"CallBack,omitempty"`
	CallBackMqConfig    *NotifyConfigCallBackMqConfig `xml:"CallBackMqConfig,omitempty"`
	CallBackKafkaConfig *KafkaConfig                  `xml:"CallBackKafkaConfig,omitempty"`
}

// NotifyConfigCallBackMqConfig TODO
type NotifyConfigCallBackMqConfig struct {
	MqMode   string `xml:"MqMode,omitempty"`
	MqRegion string `xml:"MqRegion,omitempty"`
	MqName   string `xml:"MqName,omitempty"`
}

// MediaProcessJobDetail TODO
type MediaProcessJobDetail struct {
	Code         string                    `xml:"Code,omitempty"`
	Message      string                    `xml:"Message,omitempty"`
	JobId        string                    `xml:"JobId,omitempty"`
	Tag          string                    `xml:"Tag,omitempty"`
	Progress     string                    `xml:"Progress,omitempty"`
	State        string                    `xml:"State,omitempty"`
	CreationTime string                    `xml:"CreationTime,omitempty"`
	StartTime    string                    `xml:"StartTime,omitempty"`
	EndTime      string                    `xml:"EndTime,omitempty"`
	QueueId      string                    `xml:"QueueId,omitempty"`
	Input        *JobInput                 `xml:"Input,omitempty"`
	Operation    *MediaProcessJobOperation `xml:"Operation,omitempty"`
	SubTag       string                    `xml:"SubTag,omitempty"`
	Workflow     []struct {
		Name         string `xml:"Name,omitempty"`
		RunId        string `xml:"RunId,omitempty"`
		WorkflowId   string `xml:"WorkflowId,omitempty"`
		WorkflowName string `xml:"WorkflowName,omitempty"`
	} `xml:"Workflow"`
}

// CreatePicJobsResult TODO
type CreatePicJobsResult CreateMediaJobsResult

// CreateAIJobsResult TODO
type CreateAIJobsResult CreateMediaJobsResult

// CreateMediaJobsResult TODO
type CreateMediaJobsResult struct {
	XMLName    xml.Name               `xml:"Response"`
	JobsDetail *MediaProcessJobDetail `xml:"JobsDetail,omitempty"`
}

// CreateMultiMediaJobsOptions TODO
type CreateMultiMediaJobsOptions struct {
	XMLName             xml.Name                      `xml:"Request"`
	Tag                 string                        `xml:"Tag,omitempty"`
	Input               *JobInput                     `xml:"Input,omitempty"`
	Operation           []MediaProcessJobOperation    `xml:"Operation,omitempty"`
	QueueId             string                        `xml:"QueueId,omitempty"`
	QueueType           string                        `xml:"QueueType,omitempty"`
	CallBackFormat      string                        `xml:"CallBackFormat,omitempty"`
	CallBackType        string                        `xml:"CallBackType,omitempty"`
	CallBack            string                        `xml:"CallBack,omitempty"`
	CallBackMqConfig    *NotifyConfigCallBackMqConfig `xml:"CallBackMqConfig,omitempty"`
	CallBackKafkaConfig *KafkaConfig                  `xml:"CallBackKafkaConfig,omitempty"`
}

// CreateMultiMediaJobsResult TODO
type CreateMultiMediaJobsResult struct {
	XMLName    xml.Name                `xml:"Response"`
	JobsDetail []MediaProcessJobDetail `xml:"JobsDetail,omitempty"`
}

// MediaProcessJobsNotifyBody TODO
type MediaProcessJobsNotifyBody struct {
	XMLName    xml.Name               `xml:"Response"`
	EventName  string                 `xml:"EventName"`
	JobsDetail *MediaProcessJobDetail `xml:"JobsDetail,omitempty"`
}

// WorkflowExecutionNotifyBody TODO
type WorkflowExecutionNotifyBody struct {
	XMLName           xml.Name `xml:"Response"`
	EventName         string   `xml:"EventName"`
	WorkflowExecution struct {
		RunId      string `xml:"RunId"`
		BucketId   string `xml:"BucketId"`
		Object     string `xml:"Object"`
		CosHeaders []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"CosHeaders"`
		WorkflowId   string `xml:"WorkflowId"`
		WorkflowName string `xml:"WorkflowName"`
		CreateTime   string `xml:"CreateTime"`
		State        string `xml:"State"`
		Tasks        []struct {
			Type                  string `xml:"Type"`
			CreateTime            string `xml:"CreateTime"`
			EndTime               string `xml:"EndTime"`
			State                 string `xml:"State"`
			JobId                 string `xml:"JobId"`
			Name                  string `xml:"Name"`
			TemplateId            string `xml:"TemplateId"`
			TemplateName          string `xml:"TemplateName"`
			TranscodeTemplateId   string `xml:"TranscodeTemplateId,omitempty"`
			TranscodeTemplateName string `xml:"TranscodeTemplateName,omitempty"`
			HdrMode               string `xml:"HdrMode,omitempty"`
			ResultInfo            struct {
				ObjectCount       int `xml:"ObjectCount"`
				SpriteObjectCount int `xml:"SpriteObjectCount"`
				ObjectInfo        []struct {
					ObjectName      string             `xml:"ObjectName"`
					ObjectUrl       string             `xml:"ObjectUrl"`
					InputObjectName string             `xml:"InputObjectName"`
					Code            string             `xml:"Code"`
					Message         string             `xml:"Message"`
					ImageOcrResult  *ImageOCRDetection `xml:"ImageOcrResult,omitempty"`
				} `xml:"ObjectInfo,omitempty"`
				SpriteObjectInfo []struct {
					ObjectName      string `xml:"ObjectName"`
					ObjectUrl       string `xml:"ObjectUrl"`
					InputObjectName string `xml:"InputObjectName"`
					Code            string `xml:"Code"`
					Message         string `xml:"Message"`
				} `xml:"SpriteObjectInfo,omitempty"`
			} `xml:"ResultInfo,omitempty"`
		} `xml:"Tasks"`
	} `xml:"WorkflowExecution"`
}

// CreateMultiMediaJobs TODO
func (s *CIService) CreateMultiMediaJobs(ctx context.Context, opt *CreateMultiMediaJobsOptions) (*CreateMultiMediaJobsResult, *Response, error) {
	var res CreateMultiMediaJobsResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/jobs",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateMediaJobs TODO
func (s *CIService) CreateMediaJobs(ctx context.Context, opt *CreateMediaJobsOptions) (*CreateMediaJobsResult, *Response, error) {
	var res CreateMediaJobsResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/jobs",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreatePicProcessJobs TODO
func (s *CIService) CreatePicProcessJobs(ctx context.Context, opt *CreatePicJobsOptions) (*CreatePicJobsResult, *Response, error) {
	var res CreatePicJobsResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/pic_jobs",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateAIJobs TODO
func (s *CIService) CreateAIJobs(ctx context.Context, opt *CreateAIJobsOptions) (*CreateAIJobsResult, *Response, error) {
	var res CreateAIJobsResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/ai_jobs",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribePicProcessJobResult TODO
type DescribePicProcessJobResult DescribeMediaProcessJobResult

// DescribeAIJobResult TODO
type DescribeAIJobResult DescribeMediaProcessJobResult

// DescribeMediaProcessJobResult TODO
type DescribeMediaProcessJobResult struct {
	XMLName        xml.Name               `xml:"Response"`
	JobsDetail     *MediaProcessJobDetail `xml:"JobsDetail,omitempty"`
	NonExistJobIds string                 `xml:"NonExistJobIds,omitempty"`
}

// DescribeMediaJob TODO
func (s *CIService) DescribeMediaJob(ctx context.Context, jobid string) (*DescribeMediaProcessJobResult, *Response, error) {
	var res DescribeMediaProcessJobResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/jobs/" + jobid,
		method:  http.MethodGet,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribePicProcessJob TODO
func (s *CIService) DescribePicProcessJob(ctx context.Context, jobid string) (*DescribePicProcessJobResult, *Response, error) {
	var res DescribePicProcessJobResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/pic_jobs/" + jobid,
		method:  http.MethodGet,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribeAIJob TODO
func (s *CIService) DescribeAIJob(ctx context.Context, jobid string) (*DescribeAIJobResult, *Response, error) {
	var res DescribeAIJobResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/ai_jobs/" + jobid,
		method:  http.MethodGet,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribeMutilMediaProcessJobResult TODO
type DescribeMutilMediaProcessJobResult struct {
	XMLName        xml.Name                `xml:"Response"`
	JobsDetail     []MediaProcessJobDetail `xml:"JobsDetail,omitempty"`
	NonExistJobIds []string                `xml:"NonExistJobIds,omitempty"`
}

// DescribeMultiMediaJob TODO
func (s *CIService) DescribeMultiMediaJob(ctx context.Context, jobids []string) (*DescribeMutilMediaProcessJobResult, *Response, error) {
	jobidsStr := ""
	if len(jobids) < 1 {
		return nil, nil, errors.New("empty param jobids")
	} else {
		jobidsStr = strings.Join(jobids, ",")
	}

	var res DescribeMutilMediaProcessJobResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/jobs/" + jobidsStr,
		method:  http.MethodGet,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribeMediaJobsOptions TODO
type DescribeMediaJobsOptions struct {
	QueueId               string `url:"queueId,omitempty"`
	Tag                   string `url:"tag,omitempty"`
	OrderByTime           string `url:"orderByTime,omitempty"`
	NextToken             string `url:"nextToken,omitempty"`
	Size                  int    `url:"size,omitempty"`
	States                string `url:"states,omitempty"`
	StartCreationTime     string `url:"startCreationTime,omitempty"`
	EndCreationTime       string `url:"endCreationTime,omitempty"`
	WorkflowId            string `url:"workflowId,omitempty"`
	InventoryTriggerJobId string `url:"inventoryTriggerJobId,omitempty"`
	InputObject           string `url:"inputObject,omitempty"`
}

// DescribeMediaJobsResult TODO
type DescribeMediaJobsResult struct {
	XMLName    xml.Name                `xml:"Response"`
	JobsDetail []MediaProcessJobDetail `xml:"JobsDetail,omitempty"`
	NextToken  string                  `xml:"NextToken,omitempty"`
}

// DescribeMediaJobs TODO
// https://cloud.tencent.com/document/product/460/48235
func (s *CIService) DescribeMediaJobs(ctx context.Context, opt *DescribeMediaJobsOptions) (*DescribeMediaJobsResult, *Response, error) {
	var res DescribeMediaJobsResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/jobs",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribePicProcessQueuesOptions TODO
type DescribePicProcessQueuesOptions DescribeMediaProcessQueuesOptions

// DescribeMediaProcessQueuesOptions TODO
type DescribeMediaProcessQueuesOptions struct {
	QueueIds   string `url:"queueIds,omitempty"`
	State      string `url:"state,omitempty"`
	PageNumber int    `url:"pageNumber,omitempty"`
	PageSize   int    `url:"pageSize,omitempty"`
	Category   string `url:"category,omitempty"`
}

// DescribePicProcessQueuesResult TODO
type DescribePicProcessQueuesResult DescribeMediaProcessQueuesResult

// DescribeMediaProcessQueuesResult TODO
type DescribeMediaProcessQueuesResult struct {
	XMLName      xml.Name            `xml:"Response"`
	RequestId    string              `xml:"RequestId,omitempty"`
	TotalCount   int                 `xml:"TotalCount,omitempty"`
	PageNumber   int                 `xml:"PageNumber,omitempty"`
	PageSize     int                 `xml:"PageSize,omitempty"`
	QueueList    []MediaProcessQueue `xml:"QueueList,omitempty"`
	NonExistPIDs []string            `xml:"NonExistPIDs,omitempty"`
}

// MediaProcessQueue TODO
type MediaProcessQueue struct {
	QueueId       string                         `xml:"QueueId,omitempty"`
	Name          string                         `xml:"Name,omitempty"`
	State         string                         `xml:"State,omitempty"`
	MaxSize       int                            `xml:"MaxSize,omitempty"`
	MaxConcurrent int                            `xml:"MaxConcurrent,omitempty"`
	UpdateTime    string                         `xml:"UpdateTime,omitempty"`
	CreateTime    string                         `xml:"CreateTime,omitempty"`
	NotifyConfig  *MediaProcessQueueNotifyConfig `xml:"NotifyConfig,omitempty"`
}

// MediaProcessQueueNotifyConfig TODO
type MediaProcessQueueNotifyConfig struct {
	Url          string       `xml:"Url,omitempty"`
	State        string       `xml:"State,omitempty"`
	Type         string       `xml:"Type,omitempty"`
	Event        string       `xml:"Event,omitempty"`
	ResultFormat string       `xml:"ResultFormat,omitempty"`
	MqMode       string       `xml:"MqMode,omitempty"`
	MqRegion     string       `xml:"MqRegion,omitempty"`
	MqName       string       `xml:"MqName,omitempty"`
	KafkaConfig  *KafkaConfig `xml:"KafkaConfig,omitempty"`
}

type KafkaConfig struct {
	Region     string `xml:"Region,omitempty"`
	InstanceId string `xml:"InstanceId,omitempty"`
	Topic      string `xml:"Topic,omitempty"`
}

// DescribeMediaProcessQueues TODO
func (s *CIService) DescribeMediaProcessQueues(ctx context.Context, opt *DescribeMediaProcessQueuesOptions) (*DescribeMediaProcessQueuesResult, *Response, error) {
	var res DescribeMediaProcessQueuesResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/queue",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribePicProcessQueues TODO
func (s *CIService) DescribePicProcessQueues(ctx context.Context, opt *DescribePicProcessQueuesOptions) (*DescribePicProcessQueuesResult, *Response, error) {
	var res DescribePicProcessQueuesResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/picqueue",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribeAIProcessQueues TODO
func (s *CIService) DescribeAIProcessQueues(ctx context.Context, opt *DescribeMediaProcessQueuesOptions) (*DescribeMediaProcessQueuesResult, *Response, error) {
	var res DescribeMediaProcessQueuesResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/ai_queue",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribeASRQueues TODO
func (s *CIService) DescribeASRProcessQueues(ctx context.Context, opt *DescribeMediaProcessQueuesOptions) (*DescribeMediaProcessQueuesResult, *Response, error) {
	var res DescribeMediaProcessQueuesResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/asrqueue",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

type DescribeFielProcessQueuesOptions DescribeMediaProcessQueuesOptions
type DescribeFileProcessQueuesResult DescribeMediaProcessQueuesResult

// DescribeFileProcessQueues TODO
func (s *CIService) DescribeFileProcessQueues(ctx context.Context, opt *DescribeFielProcessQueuesOptions) (*DescribeFileProcessQueuesResult, *Response, error) {
	var res DescribeFileProcessQueuesResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/file_queue",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateMediaProcessQueueOptions TODO
type UpdateMediaProcessQueueOptions struct {
	XMLName      xml.Name                       `xml:"Request"`
	Name         string                         `xml:"Name,omitempty"`
	QueueID      string                         `xml:"QueueID,omitempty"`
	State        string                         `xml:"State,omitempty"`
	NotifyConfig *MediaProcessQueueNotifyConfig `xml:"NotifyConfig,omitempty"`
}

// UpdateMediaProcessQueueResult TODO
type UpdateMediaProcessQueueResult struct {
	XMLName   xml.Name           `xml:"Response"`
	RequestId string             `xml:"RequestId"`
	Queue     *MediaProcessQueue `xml:"Queue"`
}

// UpdateMediaProcessQueue TODO
func (s *CIService) UpdateMediaProcessQueue(ctx context.Context, opt *UpdateMediaProcessQueueOptions) (*UpdateMediaProcessQueueResult, *Response, error) {
	var res UpdateMediaProcessQueueResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/queue/" + opt.QueueID,
		body:    opt,
		method:  http.MethodPut,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribePicProcessBucketsOptions 描述图片处理桶的参数
type DescribePicProcessBucketsOptions DescribeMediaProcessBucketsOptions

// DescribeAIProcessBucketsOptions 描述AI处理桶的参数
type DescribeAIProcessBucketsOptions DescribeMediaProcessBucketsOptions

// DescribeASRProcessBucketsOptions 描述ASR处理桶的参数
type DescribeASRProcessBucketsOptions DescribeMediaProcessBucketsOptions

// DescribeFileProcessBucketsOptions 描述文件处理桶的参数
type DescribeFileProcessBucketsOptions DescribeMediaProcessBucketsOptions

// DescribeMediaProcessBucketsOptions 描述媒体处理桶的参数
type DescribeMediaProcessBucketsOptions struct {
	Regions     string `url:"regions,omitempty"`
	BucketNames string `url:"bucketNames,omitempty"`
	BucketName  string `url:"bucketName,omitempty"`
	PageNumber  int    `url:"pageNumber,omitempty"`
	PageSize    int    `url:"pageSize,omitempty"`
}

// DescribeMediaProcessBucketsResult 描述媒体处理桶的结果
type DescribeMediaProcessBucketsResult struct {
	XMLName         xml.Name             `xml:"Response"`
	RequestId       string               `xml:"RequestId,omitempty"`
	TotalCount      int                  `xml:"TotalCount,omitempty"`
	PageNumber      int                  `xml:"PageNumber,omitempty"`
	PageSize        int                  `xml:"PageSize,omitempty"`
	MediaBucketList []MediaProcessBucket `xml:"MediaBucketList,omitempty"`
}

// DescribeMediaProcessBucketsResult 描述图片处理桶的结果
type DescribePicProcessBucketsResult struct {
	XMLName         xml.Name             `xml:"Response"`
	RequestId       string               `xml:"RequestId,omitempty"`
	TotalCount      int                  `xml:"TotalCount,omitempty"`
	PageNumber      int                  `xml:"PageNumber,omitempty"`
	PageSize        int                  `xml:"PageSize,omitempty"`
	MediaBucketList []MediaProcessBucket `xml:"PicBucketList,omitempty"`
}

// DescribeMediaProcessBucketsResult 描述AI处理桶的结果
type DescribeAIProcessBucketsResult struct {
	XMLName         xml.Name             `xml:"Response"`
	RequestId       string               `xml:"RequestId,omitempty"`
	TotalCount      int                  `xml:"TotalCount,omitempty"`
	PageNumber      int                  `xml:"PageNumber,omitempty"`
	PageSize        int                  `xml:"PageSize,omitempty"`
	MediaBucketList []MediaProcessBucket `xml:"AiBucketList,omitempty"`
}

// DescribeMediaProcessBucketsResult 描述ASR处理桶的结果
type DescribeASRProcessBucketsResult struct {
	XMLName         xml.Name             `xml:"Response"`
	RequestId       string               `xml:"RequestId,omitempty"`
	TotalCount      int                  `xml:"TotalCount,omitempty"`
	PageNumber      int                  `xml:"PageNumber,omitempty"`
	PageSize        int                  `xml:"PageSize,omitempty"`
	MediaBucketList []MediaProcessBucket `xml:"AsrBucketList,omitempty"`
}

// DescribeFileProcessBucketsResult TODO
type DescribeFileProcessBucketsResult struct {
	XMLName        xml.Name             `xml:"Response"`
	RequestId      string               `xml:"RequestId,omitempty"`
	TotalCount     int                  `xml:"TotalCount,omitempty"`
	PageNumber     int                  `xml:"PageNumber,omitempty"`
	PageSize       int                  `xml:"PageSize,omitempty"`
	FileBucketList []MediaProcessBucket `xml:"FileBucketList,omitempty"`
}

// MediaProcessBucket 媒体处理bucket
type MediaProcessBucket struct {
	Name       string `xml:"Name,omitempty"`
	BucketId   string `xml:"BucketId,omitempty"`
	Region     string `xml:"Region,omitempty"`
	CreateTime string `xml:"CreateTime,omitempty"`
}

// CreateMediaProcessBucketOptions 开通媒体处理服务参数
type CreateMediaProcessBucketOptions struct {
}

// CreateMediaProcessBucketResult 开通媒体处理服务返回
type CreateMediaProcessBucketResult struct {
	XMLName     xml.Name           `xml:"Response"`
	RequestId   string             `xml:"RequestId,omitempty"`
	MediaBucket MediaProcessBucket `xml:"MediaBucket,omitempty"`
}

// CreateMediaProcessBucket 开通媒体处理服务 https://cloud.tencent.com/document/product/436/115298
func (s *CIService) CreateMediaProcessBucket(ctx context.Context, opt *CreateMediaProcessBucketOptions) (*CreateMediaProcessBucketResult, *Response, error) {
	var res CreateMediaProcessBucketResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/mediabucket",
		method:  http.MethodPost,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribeMediaProcessBuckets TODO
// 媒体bucket接口 https://cloud.tencent.com/document/product/436/48988
func (s *CIService) DescribeMediaProcessBuckets(ctx context.Context, opt *DescribeMediaProcessBucketsOptions) (*DescribeMediaProcessBucketsResult, *Response, error) {
	var res DescribeMediaProcessBucketsResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/mediabucket",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreatePicProcessBucketOptions 开通图片处理异步能力参数
type CreatePicProcessBucketOptions struct {
}

// PicProcessBucket 图片处理桶信息别名
type PicProcessBucket MediaProcessBucket

// CreatePicProcessBucketResult 开通图片处理异步能力返回
type CreatePicProcessBucketResult struct {
	XMLName   xml.Name         `xml:"Response"`
	RequestId string           `xml:"RequestId,omitempty"`
	PicBucket PicProcessBucket `xml:"PicBucket,omitempty"`
}

// CreatePicProcessBucket 开通图片处理异步能力 https://cloud.tencent.com/document/product/460/95749
func (s *CIService) CreatePicProcessBucket(ctx context.Context, opt *CreatePicProcessBucketOptions) (*CreatePicProcessBucketResult, *Response, error) {
	var res CreatePicProcessBucketResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/picbucket",
		method:  http.MethodPost,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribePicProcessBuckets TODO
func (s *CIService) DescribePicProcessBuckets(ctx context.Context, opt *DescribePicProcessBucketsOptions) (*DescribePicProcessBucketsResult, *Response, error) {
	var res DescribePicProcessBucketsResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/picbucket",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateAIProcessBucketOptions 开通AI处理能力参数
type CreateAIProcessBucketOptions struct {
}

// AIProcessBucket AI处理桶信息别名
type AIProcessBucket MediaProcessBucket

// CreateAIProcessBucketResult 开通AI处理能力返回
type CreateAIProcessBucketResult struct {
	XMLName   xml.Name        `xml:"Response"`
	RequestId string          `xml:"RequestId,omitempty"`
	AiBucket  AIProcessBucket `xml:"AiBucket,omitempty"`
}

// CreateAIProcessBucket 开通AI处理能力 https://cloud.tencent.com/document/product/460/79593
func (s *CIService) CreateAIProcessBucket(ctx context.Context, opt *CreateAIProcessBucketOptions) (*CreateAIProcessBucketResult, *Response, error) {
	var res CreateAIProcessBucketResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/ai_bucket",
		method:  http.MethodPost,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribeAIProcessBuckets TODO
func (s *CIService) DescribeAIProcessBuckets(ctx context.Context, opt *DescribeAIProcessBucketsOptions) (*DescribeAIProcessBucketsResult, *Response, error) {
	var res DescribeAIProcessBucketsResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/ai_bucket",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateASRProcessBucketOptions 开通ASR处理服务参数
type CreateASRProcessBucketOptions struct {
}

// ASRBucketBucket ASR处理桶信息别名
type AsrBucketBucket MediaProcessBucket

// CreateASRProcessBucketResult 开通ASR处理服务返回
type CreateASRProcessBucketResult struct {
	XMLName         xml.Name        `xml:"Response"`
	RequestId       string          `xml:"RequestId,omitempty"`
	AsrBucketBucket AsrBucketBucket `xml:"AsrBucket,omitempty"`
}

// CreateASRProcessBucket 开通ASR处理服务 https://cloud.tencent.com/document/product/460/86377
func (s *CIService) CreateASRProcessBucket(ctx context.Context, opt *CreateASRProcessBucketOptions) (*CreateASRProcessBucketResult, *Response, error) {
	var res CreateASRProcessBucketResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/asrbucket",
		method:  http.MethodPost,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribeASRProcessBuckets TODO
func (s *CIService) DescribeASRProcessBuckets(ctx context.Context, opt *DescribeASRProcessBucketsOptions) (*DescribeASRProcessBucketsResult, *Response, error) {
	var res DescribeASRProcessBucketsResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/asrbucket",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateFileProcessBucketOptions 开通文件处理服务参数
type CreateFileProcessBucketOptions struct {
}

// FileProcessBucket 文件处理桶信息别名
type FileProcessBucket MediaProcessBucket

// CreateFileProcessBucketResult 开通文件处理服务返回
type CreateFileProcessBucketResult struct {
	XMLName           xml.Name          `xml:"Response"`
	RequestId         string            `xml:"RequestId,omitempty"`
	FileProcessBucket FileProcessBucket `xml:"FileBucket,omitempty"`
}

// CreateFileProcessBucket 开通文件处理服务 https://cloud.tencent.com/document/product/460/86377
func (s *CIService) CreateFileProcessBucket(ctx context.Context, opt *CreateFileProcessBucketOptions) (*CreateFileProcessBucketResult, *Response, error) {
	var res CreateFileProcessBucketResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/file_bucket",
		method:  http.MethodPost,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribeFileProcessBuckets TODO
func (s *CIService) DescribeFileProcessBuckets(ctx context.Context, opt *DescribeFileProcessBucketsOptions) (*DescribeFileProcessBucketsResult, *Response, error) {
	var res DescribeFileProcessBucketsResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/file_bucket",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// GetMediaInfoResult TODO
type GetMediaInfoResult struct {
	XMLName   xml.Name   `xml:"Response"`
	MediaInfo *MediaInfo `xml:"MediaInfo"`
}

// GetMediaInfo TODO
// 媒体信息接口 https://cloud.tencent.com/document/product/436/55672
func (s *CIService) GetMediaInfo(ctx context.Context, name string, opt *ObjectGetOptions, id ...string) (*GetMediaInfoResult, *Response, error) {
	var u string

	// 兼容 name 以 / 开头的情况
	if strings.HasPrefix(name, "/") {
		name = encodeURIComponent("/") + encodeURIComponent(name[1:], []byte{'/'})
	} else {
		name = encodeURIComponent(name, []byte{'/'})
	}

	if len(id) == 1 {
		u = fmt.Sprintf("/%s?versionId=%s&ci-process=videoinfo", name, id[0])
	} else if len(id) == 0 {
		u = fmt.Sprintf("/%s?ci-process=videoinfo", name)
	} else {
		return nil, nil, fmt.Errorf("wrong params")
	}

	var res GetMediaInfoResult
	sendOpt := sendOptions{
		baseURL:   s.client.BaseURL.BucketURL,
		uri:       u,
		method:    http.MethodGet,
		optQuery:  opt,
		optHeader: opt,
		result:    &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

type PlayKey struct {
	MasterPlayKey string `xml:"MasterPlayKey"`
	BackupPlayKey string `xml:"BackupPlayKey"`
}

type PlayKeyResult struct {
	XMLName     xml.Name `xml:"Response"`
	PlayKeyList *PlayKey `xml:"PlayKeyList"`
}

func (s *CIService) CreatePlayKey(ctx context.Context) (*PlayKeyResult, *Response, error) {
	var res PlayKeyResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/playKey",
		method:  http.MethodPost,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

func (s *CIService) CreateMediaPlayKey(ctx context.Context) (*PlayKeyResult, *Response, error) {
	return s.CreatePlayKey(ctx)
}

func (s *CIService) GetPlayKey(ctx context.Context) (*PlayKeyResult, *Response, error) {
	var res PlayKeyResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/playKey",
		method:  http.MethodGet,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

func (s *CIService) DescribeMediaPlayKey(ctx context.Context) (*PlayKeyResult, *Response, error) {
	var res PlayKeyResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/playKey",
		method:  http.MethodGet,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

type UpdateMediaPlayKeyOptions struct {
	MasterPlayKey string `url:"masterPlayKey,omitempty"`
	BackupPlayKey string `url:"backupPlayKey,omitempty"`
}

func (s *CIService) UpdateMediaPlayKey(ctx context.Context, opt *UpdateMediaPlayKeyOptions) (*PlayKeyResult, *Response, error) {
	var res PlayKeyResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/playKey",
		method:   http.MethodPut,
		optQuery: opt,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// GenerateMediaInfoOptions TODO
type GenerateMediaInfoOptions struct {
	XMLName xml.Name  `xml:"Request"`
	Input   *JobInput `xml:"Input,omitempty"`
}

// GenerateMediaInfo TODO
// 生成媒体信息接口，支持大文件，耗时较大请求
func (s *CIService) GenerateMediaInfo(ctx context.Context, opt *GenerateMediaInfoOptions) (*GetMediaInfoResult, *Response, error) {
	var res GetMediaInfoResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/mediainfo",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// GenerateAVInfoOptions 兼容avinfo接口
type GenerateAVInfoOptions struct {
	XMLName    xml.Name    `xml:"Request"`
	Input      *JobInput   `xml:"Input,omitempty"`
	OptHeaders *OptHeaders `header:"-,omitempty" url:"-" json:"-" xml:"-"`
}

// GetAVInfoResult 兼容avinfo格式
type GetAVInfoResult struct {
	Format struct {
		BitRate        string `json:"bit_rate"`
		Duration       string `json:"duration"`
		FormatLongName string `json:"format_long_name"`
		FormatName     string `json:"format_name"`
		NbPrograms     int    `json:"nb_programs"`
		NbStreams      int    `json:"nb_streams"`
		Size           string `json:"size"`
		StartTime      string `json:"start_time"`
	} `json:"format"`
	Streams []struct {
		AvgFrameRate       string    `json:"avg_frame_rate,omitempty"`
		BitRate            string    `json:"bit_rate"`
		CodecLongName      string    `json:"codec_long_name"`
		CodecName          string    `json:"codec_name"`
		CodecTag           string    `json:"codec_tag"`
		CodecTagString     string    `json:"codec_tag_string"`
		CodecTimeBase      string    `json:"codec_time_base"`
		CodecType          string    `json:"codec_type"`
		ColorPrimaries     string    `json:"color_primaries,omitempty"`
		ColorRange         string    `json:"color_range,omitempty"`
		ColorTransfer      string    `json:"color_transfer,omitempty"`
		CreationTime       time.Time `json:"creation_time"`
		DisplayAspectRatio string    `json:"display_aspect_ratio,omitempty"`
		Duration           string    `json:"duration"`
		FiledOrder         string    `json:"filed_order,omitempty"`
		HasBFrames         string    `json:"has_b_frames,omitempty"`
		Height             int       `json:"height,omitempty"`
		Index              int       `json:"index"`
		Language           string    `json:"language"`
		Level              int       `json:"level,omitempty"`
		NbFrames           string    `json:"nb_frames,omitempty"`
		PixFmt             string    `json:"pix_fmt,omitempty"`
		Profile            string    `json:"profile,omitempty"`
		RFrameRate         string    `json:"r_frame_rate,omitempty"`
		Refs               int       `json:"refs,omitempty"`
		Rotation           string    `json:"rotation,omitempty"`
		SampleAspectRatio  string    `json:"sample_aspect_ratio,omitempty"`
		StartTime          string    `json:"start_time"`
		Timebase           string    `json:"timebase"`
		Width              int       `json:"width,omitempty"`
		ChannelLayout      string    `json:"channel_layout,omitempty"`
		Channels           int       `json:"channels,omitempty"`
		SampleFmt          string    `json:"sample_fmt,omitempty"`
		SampleRate         string    `json:"sample_rate,omitempty"`
	} `json:"streams"`
}

// GenerateAVInfo TODO
// 生成媒体信息接口，支持大文件，耗时较大请求
func (s *CIService) GenerateAVInfo(ctx context.Context, opt *GenerateAVInfoOptions) (*GetAVInfoResult, *Response, error) {
	var buf bytes.Buffer
	var res GetAVInfoResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/avinfo",
		method:  http.MethodPost,
		body:    opt,
		result:  &buf,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	if buf.Len() > 0 {
		err = json.Unmarshal(buf.Bytes(), &res)
	}
	return &res, resp, err
}

// GetSnapshotOptions TODO
type GetSnapshotOptions struct {
	Time   float32 `url:"time,omitempty"`
	Height int     `url:"height,omitempty"`
	Width  int     `url:"width,omitempty"`
	Format string  `url:"format,omitempty"`
	Rotate string  `url:"rotate,omitempty"`
	Mode   string  `url:"mode,omitempty"`
}

// GetSnapshot TODO
// 媒体截图接口 https://cloud.tencent.com/document/product/436/55671
func (s *CIService) GetSnapshot(ctx context.Context, name string, opt *GetSnapshotOptions, id ...string) (*Response, error) {
	var u string
	if len(id) == 1 {
		u = fmt.Sprintf("/%s?versionId=%s&ci-process=snapshot", encodeURIComponent(name), id[0])
	} else if len(id) == 0 {
		u = fmt.Sprintf("/%s?ci-process=snapshot", encodeURIComponent(name))
	} else {
		return nil, fmt.Errorf("wrong params")
	}

	sendOpt := sendOptions{
		baseURL:          s.client.BaseURL.BucketURL,
		uri:              u,
		method:           http.MethodGet,
		optQuery:         opt,
		disableCloseBody: true,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return resp, err
}

type PostSnapshotOptions struct {
	XMLName xml.Name   `xml:"Request"`
	Input   *JobInput  `xml:"Input,omitempty"`
	Time    string     `xml:"Time,omitempty"`
	Width   int        `xml:"Width,omitempty"`
	Height  int        `xml:"Height,omitempty"`
	Mode    string     `xml:"Mode,omitempty"`
	Rotate  string     `xml:"Rotate,omitempty"`
	Format  string     `xml:"Format,omitempty"`
	Output  *JobOutput `xml:"Output,omitempty"`
}

type PostSnapshotResult struct {
	XMLName xml.Name   `xml:"Response"`
	Output  *JobOutput `xml:"Output,omitempty"`
}

// PostSnapshot
// https://cloud.tencent.com/document/product/460/73407
// upload snapshot image to cos
func (s *CIService) PostSnapshot(ctx context.Context, opt *PostSnapshotOptions) (*PostSnapshotResult, *Response, error) {
	var res PostSnapshotResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/snapshot",
		body:    opt,
		method:  http.MethodPost,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// PostCISnapshot 发送截图请求到万象ci服务
func (s *CIService) PostCISnapshot(ctx context.Context, opt *PostSnapshotOptions) (*Response, error) {
	sendOpt := sendOptions{
		baseURL:          s.client.BaseURL.CIURL,
		uri:              "/cisnapshot",
		body:             opt,
		method:           http.MethodPost,
		disableCloseBody: true,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return resp, err
}

// GetPrivateM3U8Options TODO
type GetPrivateM3U8Options struct {
	Expires int `url:"expires"`
}

// GetPrivateM3U8 TODO
// 获取私有m3u8资源接口 https://cloud.tencent.com/document/product/460/63738
func (s *CIService) GetPrivateM3U8(ctx context.Context, name string, opt *GetPrivateM3U8Options, id ...string) (*Response, error) {
	var u string
	if len(id) == 1 {
		u = fmt.Sprintf("/%s?versionId=%s&ci-process=pm3u8", encodeURIComponent(name), id[0])
	} else if len(id) == 0 {
		u = fmt.Sprintf("/%s?ci-process=pm3u8", encodeURIComponent(name))
	} else {
		return nil, fmt.Errorf("wrong params")
	}

	sendOpt := sendOptions{
		baseURL:          s.client.BaseURL.BucketURL,
		uri:              u,
		method:           http.MethodGet,
		optQuery:         opt,
		disableCloseBody: true,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return resp, err
}

// TriggerWorkflowOptions TODO
type TriggerWorkflowOptions struct {
	WorkflowId string `url:"workflowId"`
	Object     string `url:"object"`
	Name       string `url:"name"`
}

// TriggerWorkflowResult TODO
type TriggerWorkflowResult struct {
	XMLName    xml.Name `xml:"Response"`
	InstanceId string   `xml:"InstanceId"`
	RequestId  string   `xml:"RequestId"`
}

// TriggerWorkflow TODO
// 单文件触发工作流 https://cloud.tencent.com/document/product/460/54640
func (s *CIService) TriggerWorkflow(ctx context.Context, opt *TriggerWorkflowOptions) (*TriggerWorkflowResult, *Response, error) {
	var res TriggerWorkflowResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/triggerworkflow",
		optQuery: opt,
		method:   http.MethodPost,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribeWorkflowExecutionsOptions TODO
type DescribeWorkflowExecutionsOptions struct {
	WorkflowId        string `url:"workflowId,omitempty"`
	Name              string `url:"name,omitempty"`
	OrderByTime       string `url:"orderByTime,omitempty"`
	NextToken         string `url:"nextToken,omitempty"`
	Size              int    `url:"size,omitempty"`
	States            string `url:"states,omitempty"`
	StartCreationTime string `url:"startCreationTime,omitempty"`
	EndCreationTime   string `url:"endCreationTime,omitempty"`
	JobId             string `url:"jobId,omitempty"`
}

// WorkflowExecutionList TODO
type WorkflowExecutionList struct {
	RunId        string `xml:"RunId,omitempty"`
	WorkflowId   string `xml:"WorkflowId,omitempty"`
	State        string `xml:"State,omitempty"`
	CreationTime string `xml:"CreationTime,omitempty"`
	Object       string `xml:"Object,omitempty"`
}

// DescribeWorkflowExecutionsResult TODO
type DescribeWorkflowExecutionsResult struct {
	XMLName               xml.Name                `xml:"Response"`
	WorkflowExecutionList []WorkflowExecutionList `xml:"WorkflowExecutionList,omitempty"`
	NextToken             string                  `xml:"NextToken,omitempty"`
}

// DescribeWorkflowExecutions TODO
// 获取工作流实例列表 https://cloud.tencent.com/document/product/460/80050
func (s *CIService) DescribeWorkflowExecutions(ctx context.Context, opt *DescribeWorkflowExecutionsOptions) (*DescribeWorkflowExecutionsResult, *Response, error) {
	var res DescribeWorkflowExecutionsResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/workflowexecution",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// NotifyConfig TODO
type NotifyConfig struct {
	URL          string       `xml:"Url,omitempty"`
	Event        string       `xml:"Event,omitempty"`
	Type         string       `xml:"Type,omitempty"`
	ResultFormat string       `xml:"ResultFormat,omitempty"`
	State        string       `xml:"State,omitempty"`
	KafkaConfig  *KafkaConfig `xml:"KafkaConfig,omitempty" json:"KafkaConfig,omitempty"`
}

// ExtFilter TODO
type ExtFilter struct {
	State           string   `xml:"State,omitempty"`
	Video           string   `xml:"Video,omitempty"`
	Audio           string   `xml:"Audio,omitempty"`
	Image           string   `xml:"Image,omitempty"`
	ContentType     string   `xml:"ContentType,omitempty"`
	Custom          string   `xml:"Custom,omitempty"`
	CustomExts      string   `xml:"CustomExts,omitempty"`
	AllFile         string   `xml:"AllFile,omitempty"`
	AutoContentType []string `xml:"AutoContentType,omitempty"`
}

// NodeInput TODO
type NodeInput struct {
	QueueId      string        `xml:"QueueId,omitempty"`
	ObjectPrefix string        `xml:"ObjectPrefix,omitempty"`
	NotifyConfig *NotifyConfig `xml:"NotifyConfig,omitempty" json:"NotifyConfig,omitempty"`
	ExtFilter    *ExtFilter    `xml:"ExtFilter,omitempty" json:"ExtFilter,omitempty"`
}

// NodeOutput TODO
type NodeOutput struct {
	Region        string          `xml:"Region,omitempty"`
	Bucket        string          `xml:"Bucket,omitempty"`
	Object        string          `xml:"Object,omitempty"`
	AuObject      string          `xml:"AuObject,omitempty"`
	SpriteObject  string          `xml:"SpriteObject,omitempty"`
	BassObject    string          `xml:"BassObject,omitempty"`
	DrumObject    string          `xml:"DrumObject,omitempty"`
	StreamExtract []StreamExtract `xml:"StreamExtract,omitempty"`
}

// DelogoParam TODO
type DelogoParam struct {
	Switch string `xml:"Switch,omitempty"`
	Dx     string `xml:"Dx,omitempty"`
	Dy     string `xml:"Dy,omitempty"`
	Width  string `xml:"Width,omitempty"`
	Height string `xml:"Height,omitempty"`
}

// NodeSDRtoHDR TODO
type NodeSDRtoHDR struct {
	HdrMode string `xml:"HdrMode,omitempty"`
}

// NodeSCF TODO
type NodeSCF struct {
	Region       string `xml:"Region,omitempty"`
	FunctionName string `xml:"FunctionName,omitempty"`
	Namespace    string `xml:"Namespace,omitempty"`
}

// VideoStreamConfig TODO
type VideoStreamConfig struct {
	VideoStreamName string `xml:"VideoStreamName,omitempty"`
	BandWidth       string `xml:"BandWidth,omitempty"`
}

// NodeHlsPackInfo TODO
type NodeHlsPackInfo struct {
	VideoStreamConfig []VideoStreamConfig `xml:"VideoStreamConfig,omitempty"`
}

// WorkflowNodeCondition TODO
type WorkflowNodeCondition struct {
	Express string `xml:"Express,omitempty"`
}

// SegmentVideoBody TODO
type SegmentVideoBody struct {
	Mode              string `xml:"Mode,omitempty"`
	SegmentType       string `xml:"SegmentType,omitempty"`
	BackgroundBlue    string `xml:"BackgroundBlue,omitempty"`
	BackgroundRed     string `xml:"BackgroundRed,omitempty"`
	BackgroundGreen   string `xml:"BackgroundGreen,omitempty"`
	BackgroundLogoUrl string `xml:"BackgroundLogoUrl,omitempty"`
	BinaryThreshold   string `xml:"BinaryThreshold,omitempty"`
	RemoveRed         string `xml:"RemoveRed,omitempty"`
	RemoveGreen       string `xml:"RemoveGreen,omitempty"`
	RemoveBlue        string `xml:"RemoveBlue,omitempty"`
}

// NodeSmartCover TODO
type NodeSmartCover struct {
	Format           string `xml:"Format,omitempty"`
	Width            string `xml:"Width,omitempty"`
	Height           string `xml:"Height,omitempty"`
	Count            string `xml:"Count,omitempty"`
	DeleteDuplicates string `xml:"DeleteDuplicates,omitempty"`
}

// NodeSegmentConfig TODO
type NodeSegmentConfig struct {
	Format   string `xml:"Format,omitempty"`
	Duration string `xml:"Duration,omitempty"`
}

// NodeStreamPackConfigInfo TODO
type NodeStreamPackConfigInfo struct {
	PackType             string `xml:"PackType,omitempty"`
	IgnoreFailedStream   bool   `xml:"IgnoreFailedStream,omitempty"`
	ReserveAllStreamNode string `xml:"ReserveAllStreamNode,omitempty"`
}

// NodeOperation TODO
type NodeOperation struct {
	TemplateId           string                    `xml:"TemplateId,omitempty" json:"TemplateId,omitempty"`
	Output               *NodeOutput               `xml:"Output,omitempty" json:"Output,omitempty"`
	WatermarkTemplateId  interface{}               `xml:"WatermarkTemplateId,omitempty" json:"WatermarkTemplateId,omitempty"` // xml解析map有问题，必须interface结构
	DelogoParam          *DelogoParam              `xml:"DelogoParam,omitempty" json:"DelogoParam,omitempty"`
	SDRtoHDR             *NodeSDRtoHDR             `xml:"SDRtoHDR,omitempty" json:"SDRtoHDR,omitempty"`
	SCF                  *NodeSCF                  `xml:"SCF,omitempty" json:"SCF,omitempty"`
	HlsPackInfo          *NodeHlsPackInfo          `xml:"HlsPackInfo,omitempty" json:"HlsPackInfo,omitempty"`
	TranscodeTemplateId  string                    `xml:"TranscodeTemplateId,omitempty" json:"TranscodeTemplateId,omitempty"`
	SmartCover           *NodeSmartCover           `xml:"SmartCover,omitempty" json:"SmartCover,omitempty"`
	SegmentConfig        *NodeSegmentConfig        `xml:"Segment,omitempty" json:"Segment,omitempty"`
	DigitalWatermark     *DigitalWatermark         `xml:"DigitalWatermark,omitempty" json:"DigitalWatermark,omitempty"`
	StreamPackConfigInfo *NodeStreamPackConfigInfo `xml:"StreamPackConfig,omitempty" json:"StreamPackConfig,omitempty"`
	StreamPackInfo       *NodeHlsPackInfo          `xml:"StreamPackInfo,omitempty" json:"StreamPackInfo,omitempty"`
	Condition            *WorkflowNodeCondition    `xml:"Condition,omitempty" json:"Condition,omitempty"`
	SegmentVideoBody     *SegmentVideoBody         `xml:"SegmentVideoBody,omitempty" json:"SegmentVideoBody,omitempty"`
	ImageInspect         *ImageInspect             `xml:"ImageInspect,omitempty" json:"ImageInspect,omitempty"`
	TranscodeConfig      *struct {
		FreeTranscode string `xml:"FreeTranscode,omitempty" json:"FreeTranscode,omitempty"`
	} `xml:"TranscodeConfig,omitempty" json:"TranscodeConfig,omitempty"`
}

// Node TODO
type Node struct {
	Type      string         `xml:"Type"`
	Input     *NodeInput     `xml:"Input,omitempty" json:"Input,omitempty"`
	Operation *NodeOperation `xml:"Operation,omitempty" json:"Operation,omitempty"`
}

// Topology TODO
type Topology struct {
	Dependencies map[string]string `xml:"Dependencies,omitempty" json:"Dependencies,omitempty"`
	Nodes        map[string]Node   `xml:"Nodes,omitempty" json:"Nodes,omitempty"`
}

// UnmarshalXML TODO
func (m *Topology) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var v struct {
		XMLName      xml.Name //`xml:"Topology"`
		Dependencies struct {
			Inner []byte `xml:",innerxml"`
		} `xml:"Dependencies"`
		Nodes struct {
			Inner []byte `xml:",innerxml"`
		} `xml:"Nodes"`
	}
	err := d.DecodeElement(&v, &start)
	if err != nil {
		return err
	}

	myMap := make(map[string]interface{})

	// ... do the mxj magic here ... -

	temp := v.Nodes.Inner

	prefix := "<Nodes>"
	postfix := "</Nodes>"
	str := prefix + string(temp) + postfix
	// fmt.Println(str)
	myMxjMap, _ := mxj.NewMapXml([]byte(str))
	myMap, _ = myMxjMap["Nodes"].(map[string]interface{})
	nodesMap := make(map[string]Node)

	for k, v := range myMap {
		var node Node
		// fmt.Printf("%+v\n", v)
		mapstructure.Decode(v, &node)
		// fmt.Printf("%+v\n", err)
		nodesMap[k] = node
	}

	// fill myMap
	m.Nodes = nodesMap

	deps := make(map[string]interface{})

	tep := "<Dependencies>" + string(v.Dependencies.Inner) + "</Dependencies>"
	tepMxjMap, _ := mxj.NewMapXml([]byte(tep))
	deps, _ = tepMxjMap["Dependencies"].(map[string]interface{})
	depsString := make(map[string]string)
	for k, v := range deps {
		depsString[k] = v.(string)
	}
	m.Dependencies = depsString
	return nil
}

// WorkflowExecution TODO
type WorkflowExecution struct {
	RunId        string   `xml:"RunId,omitempty" json:"RunId,omitempty"`
	WorkflowId   string   `xml:"WorkflowId,omitempty" json:"WorkflowId,omitempty"`
	WorkflowName string   `xml:"WorkflowName,omitempty" json:"WorkflowName,omitempty"`
	State        string   `xml:"State,omitempty" json:"State,omitempty"`
	CreateTime   string   `xml:"CreateTime,omitempty" json:"CreateTime,omitempty"`
	Object       string   `xml:"Object,omitempty" json:"Object,omitempty"`
	Topology     Topology `xml:"Topology,omitempty" json:"Topology,omitempty"`
}

// DescribeWorkflowExecutionResult TODO
type DescribeWorkflowExecutionResult struct {
	XMLName           xml.Name            `xml:"Response"`
	WorkflowExecution []WorkflowExecution `xml:"WorkflowExecution,omitempty" json:"WorkflowExecution,omitempty"`
	NextToken         string              `xml:"NextToken,omitempty" json:"NextToken,omitempty"`
}

// DescribeWorkflowExecution TODO
// 获取工作流实例详情 https://cloud.tencent.com/document/product/460/45949
func (s *CIService) DescribeWorkflowExecution(ctx context.Context, runId string) (*DescribeWorkflowExecutionResult, *Response, error) {
	var res DescribeWorkflowExecutionResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/workflowexecution/" + runId,
		method:  http.MethodGet,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// SpeechRecognition TODO
type SpeechRecognition struct {
	ChannelNum         string `xml:"ChannelNum,omitempty"`
	ConvertNumMode     string `xml:"ConvertNumMode,omitempty"`
	EngineModelType    string `xml:"EngineModelType,omitempty"`
	FilterDirty        string `xml:"FilterDirty,omitempty"`
	FilterModal        string `xml:"FilterModal,omitempty"`
	ResTextFormat      string `xml:"ResTextFormat,omitempty"`
	SpeakerDiarization string `xml:"SpeakerDiarization,omitempty"`
	SpeakerNumber      string `xml:"SpeakerNumber,omitempty"`
	FilterPunc         string `xml:"FilterPunc,omitempty"`
	OutputFileType     string `xml:"OutputFileType,omitempty"`
	FlashAsr           string `xml:"FlashAsr,omitempty"`
	Format             string `xml:"Format,omitempty"`
	FirstChannelOnly   string `xml:"FirstChannelOnly,omitempty"`
	WordInfo           string `xml:"WordInfo,omitempty"`
	SentenceMaxLength  string `xml:"SentenceMaxLength,omitempty"`
}

// SpeechRecognitionResult TODO
type SpeechRecognitionResult struct {
	AudioTime                     float64                        `xml:"AudioTime,omitempty"`
	Result                        []string                       `xml:"Result,omitempty"`
	ObjectName                    string                         `xml:"ObjectName,omitempty"`
	DetailObjectName              string                         `xml:"DetailObjectName,omitempty"`
	SpeechRecognitionFlashResult  *SpeechRecognitionFlashResult  `xml:"FlashResult,omitempty"`
	SpeechRecognitionResultDetail *SpeechRecognitionResultDetail `xml:"ResultDetail,omitempty"`
}

type SpeechRecognitionFlashResult struct {
	ChannelId    int32                           `xml:"channel_id,omitempty"`
	Text         string                          `xml:"text,omitempty"`
	SentenceList []SpeechRecognitionSentenceList `xml:"sentence_list,omitempty"`
}

type SpeechRecognitionSentenceList struct {
	Text      string                      `xml:"text,omitempty"`
	StartTime string                      `xml:"start_time,omitempty"`
	EndTime   string                      `xml:"end_time,omitempty"`
	SpeakerId string                      `xml:"speaker_id,omitempty"`
	WordList  []SpeechRecognitionWordList `xml:"word_list,omitempty"`
}

type SpeechRecognitionWordList struct {
	Word      string `xml:"word,omitempty"`
	StartTime string `xml:"start_time,omitempty"`
	EndTime   string `xml:"end_time,omitempty"`
}

type SpeechRecognitionResultDetail struct {
	FinalSentence string                   `xml:"FinalSentence,omitempty"`
	SliceSentence string                   `xml:"SliceSentence,omitempty"`
	StartMs       string                   `xml:"StartMs,omitempty"`
	EndMs         string                   `xml:"EndMs,omitempty"`
	WordsNum      string                   `xml:"WordsNum,omitempty"`
	SpeechSpeed   string                   `xml:"SpeechSpeed,omitempty"`
	SpeakerId     string                   `xml:"SpeakerId,omitempty"`
	Words         []SpeechRecognitionWords `xml:"Words,omitempty"`
}

type SpeechRecognitionWords struct {
	Word          string `xml:"Word,omitempty"`
	OffsetStartMs string `xml:"OffsetStartMs,omitempty"`
	OffsetEndMs   string `xml:"OffsetEndMs,omitempty"`
}

// SoundHoundResult TODO
type SoundHoundResult struct {
	SongList struct {
		Inlier     int    `xml:"Inlier,omitempty"`
		SingerName string `xml:"SingerName,omitempty"`
		SongName   string `xml:"SongName,omitempty"`
	} `xml:"SongList,omitempty"`
}

// ASRJobOperation TODO
type ASRJobOperation struct {
	Tag                     string                   `xml:"Tag,omitempty"`
	Output                  *JobOutput               `xml:"Output,omitempty"`
	SpeechRecognition       *SpeechRecognition       `xml:"SpeechRecognition,omitempty"`
	SpeechRecognitionResult *SpeechRecognitionResult `xml:"SpeechRecognitionResult,omitempty"`
	SoundHoundResult        *SoundHoundResult        `xml:"SoundHoundResult,omitempty"`
	TemplateId              string                   `xml:"TemplateId,omitempty"`
	UserData                string                   `xml:"UserData,omitempty"`
	JobLevel                int                      `xml:"JobLevel,omitempty"`
}

// CreateASRJobsOptions TODO
type CreateASRJobsOptions struct {
	XMLName             xml.Name                      `xml:"Request"`
	Tag                 string                        `xml:"Tag,omitempty"`
	Input               *JobInput                     `xml:"Input,omitempty"`
	Operation           *ASRJobOperation              `xml:"Operation,omitempty"`
	QueueId             string                        `xml:"QueueId,omitempty"`
	CallBack            string                        `xml:"CallBack,omitempty"`
	QueueType           string                        `xml:"QueueType,omitempty"`
	CallBackFormat      string                        `xml:"CallBackFormat,omitempty"`
	CallBackType        string                        `xml:"CallBackType,omitempty"`
	CallBackMqConfig    *NotifyConfigCallBackMqConfig `xml:"CallBackMqConfig,omitempty"`
	CallBackKafkaConfig *KafkaConfig                  `xml:"CallBackKafkaConfig,omitempty"`
}

// ASRJobDetail TODO
type ASRJobDetail struct {
	Code         string           `xml:"Code,omitempty"`
	Message      string           `xml:"Message,omitempty"`
	JobId        string           `xml:"JobId,omitempty"`
	Tag          string           `xml:"Tag,omitempty"`
	State        string           `xml:"State,omitempty"`
	CreationTime string           `xml:"CreationTime,omitempty"`
	QueueId      string           `xml:"QueueId,omitempty"`
	Input        *JobInput        `xml:"Input,omitempty"`
	Operation    *ASRJobOperation `xml:"Operation,omitempty"`
}

// CreateASRJobsResult TODO
type CreateASRJobsResult struct {
	XMLName    xml.Name      `xml:"Response"`
	JobsDetail *ASRJobDetail `xml:"JobsDetail,omitempty"`
}

// CreateASRJobs TODO
func (s *CIService) CreateASRJobs(ctx context.Context, opt *CreateASRJobsOptions) (*CreateASRJobsResult, *Response, error) {
	var res CreateASRJobsResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/asr_jobs",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribeMutilASRJobResult TODO
type DescribeMutilASRJobResult struct {
	XMLName        xml.Name       `xml:"Response"`
	JobsDetail     []ASRJobDetail `xml:"JobsDetail,omitempty"`
	NonExistJobIds []string       `xml:"NonExistJobIds,omitempty"`
}

// DescribeMultiASRJob TODO
func (s *CIService) DescribeMultiASRJob(ctx context.Context, jobids []string) (*DescribeMutilASRJobResult, *Response, error) {
	jobidsStr := ""
	if len(jobids) < 1 {
		return nil, nil, errors.New("empty param jobids")
	} else {
		jobidsStr = strings.Join(jobids, ",")
	}

	var res DescribeMutilASRJobResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/asr_jobs/" + jobidsStr,
		method:  http.MethodGet,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribeMediaTemplateOptions TODO
type DescribeMediaTemplateOptions struct {
	Tag        string `url:"tag,omitempty"`
	Category   string `url:"category,omitempty"`
	Ids        string `url:"ids,omitempty"`
	Name       string `url:"name,omitempty"`
	PageNumber int    `url:"pageNumber,omitempty"`
	PageSize   int    `url:"pageSize,omitempty"`
}

// DescribeMediaTemplateResult TODO
type DescribeMediaTemplateResult struct {
	XMLName      xml.Name   `xml:"Response"`
	TemplateList []Template `xml:"TemplateList,omitempty"`
	RequestId    string     `xml:"RequestId,omitempty"`
	TotalCount   int        `xml:"TotalCount,omitempty"`
	PageNumber   int        `xml:"PageNumber,omitempty"`
	PageSize     int        `xml:"PageSize,omitempty"`
}

// DescribeMediaTemplate 搜索模板
func (s *CIService) DescribeMediaTemplate(ctx context.Context, opt *DescribeMediaTemplateOptions) (*DescribeMediaTemplateResult, *Response, error) {
	var res DescribeMediaTemplateResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/template",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DeleteMediaTemplateResult TODO
type DeleteMediaTemplateResult struct {
	RequestId  string `xml:"RequestId,omitempty"`
	TemplateId string `xml:"TemplateId,omitempty"`
}

// DeleteMediaTemplate TODO
func (s *CIService) DeleteMediaTemplate(ctx context.Context, tempalteId string) (*DeleteMediaTemplateResult, *Response, error) {
	var res DeleteMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + tempalteId,
		method:  http.MethodDelete,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateMediaSnapshotTemplateOptions TODO
type CreateMediaSnapshotTemplateOptions struct {
	XMLName  xml.Name  `xml:"Request"`
	Tag      string    `xml:"Tag,omitempty"`
	Name     string    `xml:"Name,omitempty"`
	Snapshot *Snapshot `xml:"Snapshot,omitempty"`
}

// CreateMediaTranscodeTemplateOptions TODO
type CreateMediaTranscodeTemplateOptions struct {
	XMLName       xml.Name      `xml:"Request"`
	Tag           string        `xml:"Tag,omitempty"`
	Name          string        `xml:"Name,omitempty"`
	Container     *Container    `xml:"Container,omitempty"`
	Video         *Video        `xml:"Video,omitempty"`
	Audio         *Audio        `xml:"Audio,omitempty"`
	TimeInterval  *TimeInterval `xml:"TimeInterval,omitempty"`
	TransConfig   *TransConfig  `xml:"TransConfig,omitempty"`
	AudioMix      *AudioMix     `xml:"AudioMix,omitempty"`
	AudioMixArray []AudioMix    `xml:"AudioMixArray,omitempty"`
}

// CreateMediaAnimationTemplateOptions TODO
type CreateMediaAnimationTemplateOptions struct {
	XMLName      xml.Name        `xml:"Request"`
	Tag          string          `xml:"Tag,omitempty"`
	Name         string          `xml:"Name,omitempty"`
	Container    *Container      `xml:"Container,omitempty"`
	Video        *AnimationVideo `xml:"Video,omitempty"`
	TimeInterval *TimeInterval   `xml:"TimeInterval,omitempty"`
}

// CreateMediaConcatTemplateOptions TODO
type CreateMediaConcatTemplateOptions struct {
	XMLName        xml.Name        `xml:"Request"`
	Tag            string          `xml:"Tag,omitempty"`
	Name           string          `xml:"Name,omitempty"`
	ConcatTemplate *ConcatTemplate `xml:"ConcatTemplate,omitempty"`
}

// CreateMediaVideoProcessTemplateOptions TODO
type CreateMediaVideoProcessTemplateOptions struct {
	XMLName      xml.Name      `xml:"Request"`
	Tag          string        `xml:"Tag,omitempty"`
	Name         string        `xml:"Name,omitempty"`
	ColorEnhance *ColorEnhance `xml:"ColorEnhance,omitempty"`
	MsSharpen    *MsSharpen    `xml:"MsSharpen,omitempty"`
}

// CreateMediaVideoMontageTemplateOptions TODO
type CreateMediaVideoMontageTemplateOptions struct {
	XMLName       xml.Name   `xml:"Request"`
	Tag           string     `xml:"Tag,omitempty"`
	Name          string     `xml:"Name,omitempty"`
	Duration      string     `xml:"Duration,omitempty"`
	Scene         string     `xml:"Scene,omitempty"`
	Container     *Container `xml:"Container,omitempty"`
	Video         *Video     `xml:"Video,omitempty"`
	Audio         *Audio     `xml:"Audio,omitempty"`
	AudioMix      *AudioMix  `xml:"AudioMix,omitempty"`
	AudioMixArray []AudioMix `xml:"AudioMixArray,omitempty"`
}

// CreateMediaVoiceSeparateTemplateOptions TODO
type CreateMediaVoiceSeparateTemplateOptions struct {
	XMLName     xml.Name     `xml:"Request"`
	Tag         string       `xml:"Tag,omitempty"`
	Name        string       `xml:"Name,omitempty"`
	AudioMode   string       `xml:"AudioMode,omitempty"`
	AudioConfig *AudioConfig `xml:"AudioConfig,omitempty"`
}

// CreateMediaSuperResolutionTemplateOptions TODO
type CreateMediaSuperResolutionTemplateOptions struct {
	XMLName       xml.Name `xml:"Request"`
	Tag           string   `xml:"Tag,omitempty"`
	Name          string   `xml:"Name,omitempty"`
	Resolution    string   `xml:"Resolution,omitempty"` // sdtohd、hdto4k
	EnableScaleUp string   `xml:"EnableScaleUp,omitempty"`
	Version       string   `xml:"Version,omitempty"`
}

// CreateMediaPicProcessTemplateOptions TODO
type CreateMediaPicProcessTemplateOptions struct {
	XMLName    xml.Name    `xml:"Request"`
	Tag        string      `xml:"Tag,omitempty"`
	Name       string      `xml:"Name,omitempty"`
	PicProcess *PicProcess `xml:"PicProcess,omitempty"`
}

// CreateMediaWatermarkTemplateOptions TODO
type CreateMediaWatermarkTemplateOptions struct {
	XMLName   xml.Name   `xml:"Request"`
	Tag       string     `xml:"Tag,omitempty"`
	Name      string     `xml:"Name,omitempty"`
	Watermark *Watermark `xml:"Watermark,omitempty"`
}

// CreateMediaTranscodeProTemplateOptions TODO
type CreateMediaTranscodeProTemplateOptions struct {
	XMLName      xml.Name           `xml:"Request"`
	Tag          string             `xml:"Tag,omitempty"`
	Name         string             `xml:"Name,omitempty"`
	Container    *Container         `xml:"Container,omitempty"`
	Video        *TranscodeProVideo `xml:"Video,omitempty"`
	Audio        *TranscodeProAudio `xml:"Audio,omitempty"`
	TimeInterval *TimeInterval      `xml:"TimeInterval,omitempty"`
	TransConfig  *TransConfig       `xml:"TransConfig,omitempty"`
}

// CreateMediaTtsTemplateOptions TODO
type CreateMediaTtsTemplateOptions struct {
	XMLName   xml.Name `xml:"Request"`
	Tag       string   `xml:"Tag,omitempty"`
	Name      string   `xml:"Name,omitempty"`
	Mode      string   `xml:"Mode,omitempty"`
	Codec     string   `xml:"Codec,omitempty"`
	VoiceType string   `xml:"VoiceType,omitempty"`
	Volume    string   `xml:"Volume,omitempty"`
	Speed     string   `xml:"Speed,omitempty"`
	Emotion   string   `xml:"Emotion,omitempty"`
}

// CreateMediaSmartcoverTemplateOptions TODO
type CreateMediaSmartCoverTemplateOptions struct {
	XMLName    xml.Name        `xml:"Request"`
	Tag        string          `xml:"Tag,omitempty"`
	Name       string          `xml:"Name,omitempty"`
	SmartCover *NodeSmartCover `xml:"SmartCover,omitempty" json:"SmartCover,omitempty"`
}

// CreateMediaSpeechRecognitionTemplateOptions TODO
type CreateMediaSpeechRecognitionTemplateOptions struct {
	XMLName           xml.Name           `xml:"Request"`
	Tag               string             `xml:"Tag,omitempty"`
	Name              string             `xml:"Name,omitempty"`
	SpeechRecognition *SpeechRecognition `xml:"SpeechRecognition,omitempty" json:"SpeechRecognition,omitempty"`
}

// CreateNoiseReductionTemplateOptions TODO
type CreateNoiseReductionTemplateOptions struct {
	XMLName        xml.Name        `xml:"Request"`
	Tag            string          `xml:"Tag,omitempty"`
	Name           string          `xml:"Name,omitempty"`
	NoiseReduction *NoiseReduction `xml:"NoiseReduction,omitempty" json:"NoiseReduction,omitempty"`
}

// NoiseReduction TODO
type NoiseReduction struct {
	Format     string `xml:"Format,omitempty"`
	Samplerate string `xml:"Samplerate,omitempty"`
}

// CreateVideoEnhanceTemplateOptions TODO
type CreateVideoEnhanceTemplateOptions struct {
	XMLName      xml.Name      `xml:"Request"`
	Tag          string        `xml:"Tag,omitempty"`
	Name         string        `xml:"Name,omitempty"`
	VideoEnhance *VideoEnhance `xml:"VideoEnhance,omitempty" json:"VideoEnhance,omitempty"`
}

// VideoEnhance TODO
type VideoEnhance struct {
	Transcode       *Transcode       `xml:"Transcode,omitempty"`
	SuperResolution *SuperResolution `xml:"SuperResolution,omitempty"`
	ColorEnhance    *ColorEnhance    `xml:"ColorEnhance,omitempty"`
	MsSharpen       *MsSharpen       `xml:"MsSharpen,omitempty"`
	SDRtoHDR        *SDRtoHDR        `xml:"SDRtoHDR,omitempty"`
	FrameEnhance    *FrameEnhance    `xml:"FrameEnhance,omitempty"`
}

// FrameEnhance TODO
type FrameEnhance struct {
	FrameDoubling string `xml:"FrameDoubling,omitempty"`
}

// CreateVideoTargetRecTemplateOptions TODO
type CreateVideoTargetRecTemplateOptions struct {
	XMLName        xml.Name        `xml:"Request"`
	Tag            string          `xml:"Tag,omitempty"`
	Name           string          `xml:"Name,omitempty"`
	VideoTargetRec *VideoTargetRec `xml:"VideoTargetRec,omitempty" json:"VideoTargetRec,omitempty"`
}

// VTRTranscode TODO
type VTRTranscode struct {
	Container *Container `xml:"Container,omitempty"`
	Video     *VTRVideo  `xml:"Video,omitempty"`
}

// VTRVideo TODO
type VTRVideo struct {
	Codec   string `xml:"Codec"`
	Bitrate string `xml:"Bitrate,omitempty"`
	Crf     string `xml:"Crf,omitempty"`
	Width   string `xml:"Width,omitempty"`
	Height  string `xml:"Height,omitempty"`
	Fps     string `xml:"Fps,omitempty"`
}

// VideoTargetRec TODO
type VideoTargetRec struct {
	Body        string        `xml:"Body,omitempty"`
	Pet         string        `xml:"Pet,omitempty"`
	Car         string        `xml:"Car,omitempty"`
	Face        string        `xml:"Face,omitempty"`
	Plate       string        `xml:"Plate,omitempty"`
	ProcessType string        `xml:"ProcessType,omitempty"`
	TransTpl    *VTRTranscode `xml:"TransTpl,omitempty"`
}

// VideoTargetRecResult TODO
type VideoTargetRecResult struct {
	BodyRecognition          []*BodyRecognition          `xml:"BodyRecognition,omitempty"`
	PetRecognition           []*PetRecognition           `xml:"PetRecognition,omitempty"`
	CarRecognition           []*CarRecognition           `xml:"CarRecognition,omitempty"`
	FaceRecognition          []*FaceRecognition          `xml:"FaceRecognition,omitempty"`
	LicenseRecognitionResult []*LicenseRecognitionResult `xml:"LicenseRecognitionResult,omitempty"`
	Sensitive                string                      `xml:"Sensitive,omitempty"`
}

// BodyRecognition TODO
type BodyRecognition struct {
	Time     string                `xml:"Time,omitempty"`
	Url      string                `xml:"Url,omitempty"`
	BodyInfo []*VideoTargetRecInfo `xml:"BodyInfo,omitempty"`
}

// PetRecognition TODO
type PetRecognition struct {
	Time    string                `xml:"Time,omitempty"`
	Url     string                `xml:"Url,omitempty"`
	PetInfo []*VideoTargetRecInfo `xml:"PetInfo,omitempty"`
}

// CarRecognition TODO
type CarRecognition struct {
	Time    string                `xml:"Time,omitempty"`
	Url     string                `xml:"Url,omitempty"`
	CarInfo []*VideoTargetRecInfo `xml:"CarInfo,omitempty"`
}

// FaceRecognition TODO
type FaceRecognition struct {
	Time     string                `xml:"Time,omitempty"`
	Url      string                `xml:"Url,omitempty"`
	FaceInfo []*VideoTargetRecInfo `xml:"FaceInfo,omitempty"`
}

// LicenseRecognitionResult TODO
type LicenseRecognitionResult struct {
	Time      string                `xml:"Time,omitempty"`
	Url       string                `xml:"Url,omitempty"`
	PlateInfo []*VideoTargetRecInfo `xml:"PlateInfo,omitempty"`
}

// BodyInfo TODO
type VideoTargetRecInfo struct {
	Name     string                  `xml:"Name,omitempty"`
	Score    string                  `xml:"Score,omitempty"`
	Location *VideoTargetRecLocation `xml:"Location,omitempty"`
}

// VideoTargetRecLocation TODO
type VideoTargetRecLocation struct {
	X      string `xml:"X,omitempty"`
	Y      string `xml:"Y,omitempty"`
	Height string `xml:"Height,omitempty"`
	Width  string `xml:"Width,omitempty"`
}

// SplitVideoParts TODO
type SplitVideoParts struct {
	Mode string `xml:"Mode,omitempty"`
}

// SplitVideoInfoResult TODO
type SplitVideoInfoResult struct {
	TimeInfo []struct {
		Index     string `xml:"Index,omitempty"`
		PartBegin string `xml:"PartBegin,omitempty"`
		PartEnd   string `xml:"PartEnd,omitempty"`
	} `xml:"TimeInfo,omitempty"`
}

// CreateMediaTemplateResult TODO
type CreateMediaTemplateResult struct {
	XMLName   xml.Name  `xml:"Response"`
	RequestId string    `xml:"RequestId,omitempty"`
	Template  *Template `xml:"Template,omitempty"`
}

// Template TODO
type Template struct {
	TemplateId        string             `xml:"TemplateId,omitempty"`
	Tag               string             `xml:"Code,omitempty"`
	Name              string             `xml:"Name,omitempty"`
	TransTpl          *Transcode         `xml:"TransTpl,omitempty"`
	CreateTime        string             `xml:"CreateTime,omitempty"`
	UpdateTime        string             `xml:"UpdateTime,omitempty"`
	BucketId          string             `xml:"BucketId,omitempty"`
	Category          string             `xml:"Category,omitempty"`
	Snapshot          *Snapshot          `xml:"Snapshot,omitempty"`
	Animation         *Animation         `xml:"Animation,omitempty"`
	ConcatTemplate    *ConcatTemplate    `xml:"ConcatTemplate,omitempty"`
	VideoProcess      *VideoProcess      `xml:"VideoProcess,omitempty"`
	VideoMontage      *VideoMontage      `xml:"VideoMontage,omitempty"`
	VoiceSeparate     *VoiceSeparate     `xml:"VoiceSeparate,omitempty"`
	SuperResolution   *SuperResolution   `xml:"SuperResolution,omitempty"`
	PicProcess        *PicProcess        `xml:"PicProcess,omitempty"`
	Watermark         *Watermark         `xml:"Watermark,omitempty"`
	TransProTpl       *TranscodePro      `xml:"TransProTpl,omitempty"`
	TtsTpl            *TtsTpl            `xml:"TtsTpl,omitempty"`
	SmartCover        *NodeSmartCover    `xml:"SmartCover,omitempty" json:"SmartCover,omitempty"`
	SpeechRecognition *SpeechRecognition `xml:"SpeechRecognition,omitempty" json:"SpeechRecognition,omitempty"`
	NoiseReduction    *NoiseReduction    `xml:"NoiseReduction,omitempty" json:"NoiseReduction,omitempty"`
	VideoEnhance      *VideoEnhance      `xml:"VideoEnhance,omitempty" json:"VideoEnhance,omitempty"`
	VideoTargetRec    *VideoTargetRec    `xml:"VideoTargetRec,omitempty" json:"VideoTargetRec,omitempty"`
	ImageOCR          *ImageOCRTemplate  `xml:"ImageOCR,omitempty" json:"ImageOCR,omitempty"`
}

// CreateMediaSnapshotTemplate 创建截图模板
func (s *CIService) CreateMediaSnapshotTemplate(ctx context.Context, opt *CreateMediaSnapshotTemplateOptions) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateMediaSnapshotTemplate 更新截图模板
func (s *CIService) UpdateMediaSnapshotTemplate(ctx context.Context, opt *CreateMediaSnapshotTemplateOptions, templateId string) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + templateId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateMediaTranscodeTemplate Options 创建转码模板
func (s *CIService) CreateMediaTranscodeTemplate(ctx context.Context, opt *CreateMediaTranscodeTemplateOptions) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateMediaTranscodeTemplate 更新转码模板
func (s *CIService) UpdateMediaTranscodeTemplate(ctx context.Context, opt *CreateMediaTranscodeTemplateOptions, templateId string) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + templateId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateMediaAnimationTemplate 创建动图模板
func (s *CIService) CreateMediaAnimationTemplate(ctx context.Context, opt *CreateMediaAnimationTemplateOptions) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateMediaAnimationTemplate 更新动图模板
func (s *CIService) UpdateMediaAnimationTemplate(ctx context.Context, opt *CreateMediaAnimationTemplateOptions, templateId string) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + templateId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateMediaConcatTemplate 创建拼接模板
func (s *CIService) CreateMediaConcatTemplate(ctx context.Context, opt *CreateMediaConcatTemplateOptions) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateMediaConcatTemplate 更新拼接模板
func (s *CIService) UpdateMediaConcatTemplate(ctx context.Context, opt *CreateMediaConcatTemplateOptions, templateId string) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + templateId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateMediaVideoProcessTemplate 创建视频增强模板
func (s *CIService) CreateMediaVideoProcessTemplate(ctx context.Context, opt *CreateMediaVideoProcessTemplateOptions) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateMediaVideoProcessTemplate 更新视频增强模板
func (s *CIService) UpdateMediaVideoProcessTemplate(ctx context.Context, opt *CreateMediaVideoProcessTemplateOptions, templateId string) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + templateId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateMediaVideoMontageTemplate 创建精彩集锦模板
func (s *CIService) CreateMediaVideoMontageTemplate(ctx context.Context, opt *CreateMediaVideoMontageTemplateOptions) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateMediaVideoMontageTemplate 更新精彩集锦模板
func (s *CIService) UpdateMediaVideoMontageTemplate(ctx context.Context, opt *CreateMediaVideoMontageTemplateOptions, templateId string) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + templateId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateMediaVoiceSeparateTemplate 创建人声分离模板
func (s *CIService) CreateMediaVoiceSeparateTemplate(ctx context.Context, opt *CreateMediaVoiceSeparateTemplateOptions) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateMediaVoiceSeparateTemplate 更新人声分离模板
func (s *CIService) UpdateMediaVoiceSeparateTemplate(ctx context.Context, opt *CreateMediaVoiceSeparateTemplateOptions, templateId string) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + templateId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateMediaSuperResolutionTemplate 创建超级分辨率模板
func (s *CIService) CreateMediaSuperResolutionTemplate(ctx context.Context, opt *CreateMediaSuperResolutionTemplateOptions) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateMediaSuperResolutionTemplate 更新超级分辨率模板
func (s *CIService) UpdateMediaSuperResolutionTemplate(ctx context.Context, opt *CreateMediaSuperResolutionTemplateOptions, templateId string) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + templateId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateMediaPicProcessTemplate 创建图片处理模板
func (s *CIService) CreateMediaPicProcessTemplate(ctx context.Context, opt *CreateMediaPicProcessTemplateOptions) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateMediaPicProcessTemplate 更新图片处理模板
func (s *CIService) UpdateMediaPicProcessTemplate(ctx context.Context, opt *CreateMediaPicProcessTemplateOptions, templateId string) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + templateId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateMediaWatermarkTemplate 创建水印模板
func (s *CIService) CreateMediaWatermarkTemplate(ctx context.Context, opt *CreateMediaWatermarkTemplateOptions) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateMediaWatermarkTemplate 更新水印模板
func (s *CIService) UpdateMediaWatermarkTemplate(ctx context.Context, opt *CreateMediaWatermarkTemplateOptions, templateId string) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + templateId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateMediaTranscodeProTemplate 创建广电转码模板
func (s *CIService) CreateMediaTranscodeProTemplate(ctx context.Context, opt *CreateMediaTranscodeProTemplateOptions) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateMediaTranscodeProTemplate 更新广电转码模板
func (s *CIService) UpdateMediaTranscodeProTemplate(ctx context.Context, opt *CreateMediaTranscodeProTemplateOptions, templateId string) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + templateId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateMediaTtsTemplate 创建语音合成模板
func (s *CIService) CreateMediaTtsTemplate(ctx context.Context, opt *CreateMediaTtsTemplateOptions) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateMediaTtsTemplate 更新语音合成模板
func (s *CIService) UpdateMediaTtsTemplate(ctx context.Context, opt *CreateMediaTtsTemplateOptions, templateId string) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + templateId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateMediaTtsTemplate 创建智能封面模板
func (s *CIService) CreateMediaSmartCoverTemplate(ctx context.Context, opt *CreateMediaSmartCoverTemplateOptions) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateMediaTtsTemplate 更新智能封面模板
func (s *CIService) UpdateMediaSmartCoverTemplate(ctx context.Context, opt *CreateMediaSmartCoverTemplateOptions, templateId string) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + templateId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateMediaTtsTemplate 创建语音识别模板
func (s *CIService) CreateMediaSpeechRecognitionTemplate(ctx context.Context, opt *CreateMediaSpeechRecognitionTemplateOptions) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateMediaTtsTemplate 更新语音识别模板
func (s *CIService) UpdateMediaSpeechRecognitionTemplate(ctx context.Context, opt *CreateMediaSpeechRecognitionTemplateOptions, templateId string) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + templateId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateNoiseReductionTemplate 创建音频降噪模板
func (s *CIService) CreateNoiseReductionTemplate(ctx context.Context, opt *CreateNoiseReductionTemplateOptions) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateNoiseReductionTemplate 更新音频降噪模板
func (s *CIService) UpdateNoiseReductionTemplate(ctx context.Context, opt *CreateNoiseReductionTemplateOptions, templateId string) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + templateId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateVideoEnhanceTemplate 创建画质增强模板
func (s *CIService) CreateVideoEnhanceTemplate(ctx context.Context, opt *CreateVideoEnhanceTemplateOptions) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateVideoEnhanceTemplate 更新画质增强模板
func (s *CIService) UpdateVideoEnhanceTemplate(ctx context.Context, opt *CreateVideoEnhanceTemplateOptions, templateId string) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + templateId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateVideoTargetRecTemplate 创建移动物体检测模板
func (s *CIService) CreateVideoTargetRecTemplate(ctx context.Context, opt *CreateVideoTargetRecTemplateOptions) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateVideoTargetRecTemplate 更新移动物体检测模板
func (s *CIService) UpdateVideoTargetRecTemplate(ctx context.Context, opt *CreateVideoTargetRecTemplateOptions, templateId string) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + templateId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// MediaWorkflow TODO
type MediaWorkflow struct {
	Name       string    `xml:"Name,omitempty"`
	WorkflowId string    `xml:"WorkflowId,omitempty"`
	State      string    `xml:"State,omitempty"`
	Topology   *Topology `xml:"Topology,omitempty"`
	CreateTime string    `xml:"CreateTime,omitempty"`
	UpdateTime string    `xml:"UpdateTime,omitempty"`
	BucketId   string    `xml:"BucketId,omitempty"`
}

// MarshalXML TODO
func (m *CreateMediaWorkflowOptions) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if m == nil {
		return nil
	}
	type xmlMapEntry struct {
		XMLName   xml.Name
		Type      string      `xml:"Type"`
		Input     interface{} `xml:",innerxml"`
		Operation interface{} `xml:",innerxml"`
	}
	tokens := []xml.Token{}
	tokens = append(tokens, xml.StartElement{Name: xml.Name{Local: "Request"}})
	tokens = append(tokens, xml.StartElement{Name: xml.Name{Local: "MediaWorkflow"}})
	t := xml.StartElement{Name: xml.Name{Local: "Name"}}
	tokens = append(tokens, t, xml.CharData(m.MediaWorkflow.Name), xml.EndElement{Name: t.Name})
	t = xml.StartElement{Name: xml.Name{Local: "State"}}
	tokens = append(tokens, t, xml.CharData(m.MediaWorkflow.State), xml.EndElement{Name: t.Name})
	tokens = append(tokens, xml.StartElement{Name: xml.Name{Local: "Topology"}})
	tokens = append(tokens, xml.StartElement{Name: xml.Name{Local: "Dependencies"}})

	var dependenciesKeys []string
	for k := range m.MediaWorkflow.Topology.Dependencies {
		dependenciesKeys = append(dependenciesKeys, k)
	}
	sort.Strings(dependenciesKeys)
	for _, k := range dependenciesKeys {
		t := xml.StartElement{Name: xml.Name{Local: k}}
		tokens = append(tokens, t, xml.CharData(m.MediaWorkflow.Topology.Dependencies[k]), xml.EndElement{Name: t.Name})
	}
	tokens = append(tokens, xml.EndElement{Name: xml.Name{Local: "Dependencies"}})
	// Nodes
	tokens = append(tokens, xml.StartElement{Name: xml.Name{Local: "Nodes"}})
	for _, t := range tokens {
		err := e.EncodeToken(t)
		if err != nil {
			return err
		}
	}
	tokens = tokens[:0]
	var nodesKeys []string
	for k := range m.MediaWorkflow.Topology.Nodes {
		nodesKeys = append(nodesKeys, k)
	}
	sort.Strings(nodesKeys)
	for _, k := range nodesKeys {
		e.Encode(xmlMapEntry{XMLName: xml.Name{Local: k}, Type: m.MediaWorkflow.Topology.Nodes[k].Type,
			Input: m.MediaWorkflow.Topology.Nodes[k].Input, Operation: m.MediaWorkflow.Topology.Nodes[k].Operation})
	}
	tokens = append(tokens, xml.EndElement{Name: xml.Name{Local: "Nodes"}})
	tokens = append(tokens, xml.EndElement{Name: xml.Name{Local: "Topology"}})
	tokens = append(tokens, xml.EndElement{Name: xml.Name{Local: "MediaWorkflow"}})
	tokens = append(tokens, xml.EndElement{Name: xml.Name{Local: "Request"}})
	for _, t := range tokens {
		err := e.EncodeToken(t)
		if err != nil {
			return err
		}
	}
	return e.Flush()
}

// UnmarshalXML TODO
func (m *CreateMediaWorkflowOptions) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var v struct {
		XMLName      xml.Name //`xml:"Topology"`
		Dependencies struct {
			Inner []byte `xml:",innerxml"`
		} `xml:"Dependencies"`
		Nodes struct {
			Inner []byte `xml:",innerxml"`
		} `xml:"Nodes"`
	}
	err := d.DecodeElement(&v, &start)
	if err != nil {
		return err
	}

	myMap := make(map[string]interface{})

	// ... do the mxj magic here ... -

	temp := v.Nodes.Inner

	prefix := "<Nodes>"
	postfix := "</Nodes>"
	str := prefix + string(temp) + postfix
	myMxjMap, _ := mxj.NewMapXml([]byte(str))
	myMap, _ = myMxjMap["Nodes"].(map[string]interface{})
	nodesMap := make(map[string]Node)

	for k, v := range myMap {
		var node Node
		mapstructure.Decode(v, &node)
		nodesMap[k] = node
	}

	// fill myMap
	m.MediaWorkflow.Topology.Nodes = nodesMap

	deps := make(map[string]interface{})

	tep := "<Dependencies>" + string(v.Dependencies.Inner) + "</Dependencies>"
	tepMxjMap, _ := mxj.NewMapXml([]byte(tep))
	deps, _ = tepMxjMap["Dependencies"].(map[string]interface{})
	depsString := make(map[string]string)
	for k, v := range deps {
		depsString[k] = v.(string)
	}
	m.MediaWorkflow.Topology.Dependencies = depsString
	return nil
}

// CreateMediaWorkflowOptions TODO
type CreateMediaWorkflowOptions struct {
	XMLName       xml.Name       `xml:"Request"`
	MediaWorkflow *MediaWorkflow `xml:"MediaWorkflow,omitempty"`
}

// CreateMediaWorkflowResult TODO
type CreateMediaWorkflowResult struct {
	XMLName       xml.Name       `xml:"Response"`
	RequestId     string         `xml:"RequestId,omitempty"`
	MediaWorkflow *MediaWorkflow `xml:"MediaWorkflow,omitempty"`
}

// CreateMediaWorkflow 创建工作流
func (s *CIService) CreateMediaWorkflow(ctx context.Context, opt *CreateMediaWorkflowOptions) (*CreateMediaWorkflowResult, *Response, error) {
	var res CreateMediaWorkflowResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/workflow",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateMediaWorkflow TODO
func (s *CIService) UpdateMediaWorkflow(ctx context.Context, opt *CreateMediaWorkflowOptions, workflowId string) (*CreateMediaWorkflowResult, *Response, error) {
	var res CreateMediaWorkflowResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/workflow/" + workflowId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateMediaWorkflow TODO
func (s *CIService) ActiveMediaWorkflow(ctx context.Context, workflowId string) (*Response, error) {
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/workflow/" + workflowId + "?active",
		method:  http.MethodPut,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return resp, err
}

// UpdateMediaWorkflow TODO
func (s *CIService) PausedMediaWorkflow(ctx context.Context, workflowId string) (*Response, error) {
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/workflow/" + workflowId + "?paused",
		method:  http.MethodPut,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return resp, err
}

// DescribeMediaWorkflowOptions TODO
type DescribeMediaWorkflowOptions struct {
	Ids        string `url:"ids,omitempty"`
	Name       string `url:"name,omitempty"`
	PageNumber int    `url:"pageNumber,omitempty"`
	PageSize   int    `url:"pageSize,omitempty"`
}

// DescribeMediaWorkflowResult TODO
type DescribeMediaWorkflowResult struct {
	XMLName           xml.Name        `xml:"Response"`
	MediaWorkflowList []MediaWorkflow `xml:"MediaWorkflowList,omitempty"`
	RequestId         string          `xml:"RequestId,omitempty"`
	TotalCount        int             `xml:"TotalCount,omitempty"`
	PageNumber        int             `xml:"PageNumber,omitempty"`
	PageSize          int             `xml:"PageSize,omitempty"`
	NonExistIDs       []string        `xml:"NonExistIDs,omitempty"`
}

// DescribeMediaWorkflow 搜索工作流
func (s *CIService) DescribeMediaWorkflow(ctx context.Context, opt *DescribeMediaWorkflowOptions) (*DescribeMediaWorkflowResult, *Response, error) {
	var res DescribeMediaWorkflowResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/workflow",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DeleteMediaWorkflowResult TODO
type DeleteMediaWorkflowResult struct {
	RequestId  string `xml:"RequestId,omitempty"`
	WorkflowId string `xml:"WorkflowId,omitempty"`
}

// DeleteMediaWorkflow 删除工作流
func (s *CIService) DeleteMediaWorkflow(ctx context.Context, workflowId string) (*DeleteMediaWorkflowResult, *Response, error) {
	var res DeleteMediaWorkflowResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/workflow/" + workflowId,
		method:  http.MethodDelete,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// InventoryTriggerJobInput TODO
type InventoryTriggerJobInput struct {
	Manifest string `xml:"Manifest,omitempty"`
	UrlFile  string `xml:"UrlFile,omitempty"`
	Prefix   string `xml:"Prefix,omitempty"`
	Object   string `xml:"Object,omitempty"`
}

// InventoryTriggerJobOperationTimeInterval TODO
type InventoryTriggerJobOperationTimeInterval struct {
	Start string `xml:"Start,omitempty"`
	End   string `xml:"End,omitempty"`
}

// InventoryTriggerJobOperation TODO
type InventoryTriggerJobOperation struct {
	WorkflowIds      string                                   `xml:"WorkflowIds,omitempty"`
	TimeInterval     InventoryTriggerJobOperationTimeInterval `xml:"TimeInterval,omitempty"`
	QueueId          string                                   `xml:"QueueId,omitempty"`
	UserData         string                                   `xml:"UserData,omitempty"`
	JobLevel         int                                      `xml:"JobLevel,omitempty"`
	CallBackFormat   string                                   `xml:"CallBackFormat,omitempty"`
	CallBackType     string                                   `xml:"CallBackType,omitempty"`
	CallBack         string                                   `xml:"CallBack,omitempty"`
	CallBackMqConfig *NotifyConfigCallBackMqConfig            `xml:"CallBackMqConfig,omitempty"`
	Tag              string                                   `xml:"Tag,omitempty"`
	JobParam         *InventoryTriggerJobOperationJobParam    `xml:"JobParam,omitempty"`
	Output           *JobOutput                               `xml:"Output,omitempty"`
	FreeTranscode    string                                   `xml:"FreeTranscode,omitempty"`
}

// InventoryTriggerJobOperationJobParam TODO
type InventoryTriggerJobOperationJobParam struct {
	MediaResult             *MediaResult             `xml:"MediaResult,omitempty"`
	MediaInfo               *MediaInfo               `xml:"MediaInfo,omitempty"`
	Transcode               *Transcode               `xml:"Transcode,omitempty"`
	Watermark               []Watermark              `xml:"Watermark,omitempty"`
	TemplateId              string                   `xml:"TemplateId,omitempty"`
	WatermarkTemplateId     []string                 `xml:"WatermarkTemplateId,omitempty"`
	ConcatTemplate          *ConcatTemplate          `xml:"ConcatTemplate,omitempty"`
	Snapshot                *Snapshot                `xml:"Snapshot,omitempty"`
	Animation               *Animation               `xml:"Animation,omitempty"`
	Segment                 *Segment                 `xml:"Segment,omitempty"`
	VideoMontage            *VideoMontage            `xml:"VideoMontage,omitempty"`
	VoiceSeparate           *VoiceSeparate           `xml:"VoiceSeparate,omitempty"`
	VideoProcess            *VideoProcess            `xml:"VideoProcess,omitempty"`
	TranscodeTemplateId     string                   `xml:"TranscodeTemplateId,omitempty"` // 视频增强、超分、SDRtoHDR任务类型，可以选择转码模板相关参数
	SDRtoHDR                *SDRtoHDR                `xml:"SDRtoHDR,omitempty"`
	SuperResolution         *SuperResolution         `xml:"SuperResolution,omitempty"`
	DigitalWatermark        *DigitalWatermark        `xml:"DigitalWatermark,omitempty"`
	ExtractDigitalWatermark *ExtractDigitalWatermark `xml:"ExtractDigitalWatermark,omitempty"`
	VideoTag                *VideoTag                `xml:"VideoTag,omitempty"`
	VideoTagResult          *VideoTagResult          `xml:"VideoTagResult,omitempty"`
	SmartCover              *NodeSmartCover          `xml:"SmartCover,omitempty"`
	QualityEstimate         *QualityEstimate         `xml:"QualityEstimate,omitempty"`
	TtsTpl                  *TtsTpl                  `xml:"TtsTpl,omitempty"`
	TtsConfig               *TtsConfig               `xml:"TtsConfig,omitempty"`
	Translation             *Translation             `xml:"Translation,omitempty"`
	WordsGeneralize         *WordsGeneralize         `xml:"WordsGeneralize,omitempty"`
	WordsGeneralizeResult   *WordsGeneralizeResult   `xml:"WordsGeneralizeResult,omitempty"`
	NoiseReduction          *NoiseReduction          `xml:"NoiseReduction,omitempty"`
	DnaConfig               *DnaConfig               `xml:"DnaConfig,omitempty"`
	DnaResult               *DnaResult               `xml:"DnaResult,omitempty"`
	VocalScore              *VocalScore              `xml:"VocalScore,omitempty"`
	VocalScoreResult        *VocalScoreResult        `xml:"VocalScoreResult,omitempty"`
	ImageInspect            *ImageInspect            `xml:"ImageInspect,omitempty"`
	ImageInspectResult      *ImageInspectResult      `xml:"ImageInspectResult,omitempty"`
	ImageOCR                *ImageOCRTemplate        `xml:"ImageOCR,omitempty"`
	Detection               *ImageOCRDetection       `xml:"Detection,omitempty"`
}

// InventoryTriggerJob TODO
type InventoryTriggerJob struct {
	Name      string                        `xml:"Name,omitempty"`
	Input     *InventoryTriggerJobInput     `xml:"Input,omitempty"`
	Operation *InventoryTriggerJobOperation `xml:"Operation,omitempty"`
}

// CreateInventoryTriggerJobOptions TODO
type CreateInventoryTriggerJobOptions struct {
	XMLName   xml.Name                      `xml:"Request"`
	Name      string                        `xml:"Name,omitempty"`
	Type      string                        `xml:"Type,omitempty"`
	Input     *InventoryTriggerJobInput     `xml:"Input,omitempty"`
	Operation *InventoryTriggerJobOperation `xml:"Operation,omitempty"`
}

// InventoryTriggerJobDetail TODO
type InventoryTriggerJobDetail struct {
	Code         string                        `xml:"Code,omitempty"`
	Message      string                        `xml:"Message,omitempty"`
	JobId        string                        `xml:"JobId,omitempty"`
	Tag          string                        `xml:"Tag,omitempty"`
	Progress     string                        `xml:"Progress,omitempty"`
	State        string                        `xml:"State,omitempty"`
	CreationTime string                        `xml:"CreationTime,omitempty"`
	StartTime    string                        `xml:"StartTime,omitempty"`
	EndTime      string                        `xml:"EndTime,omitempty"`
	QueueId      string                        `xml:"QueueId,omitempty"`
	Input        *InventoryTriggerJobInput     `xml:"Input,omitempty"`
	Operation    *InventoryTriggerJobOperation `xml:"Operation,omitempty"`
}

// CreateInventoryTriggerJobResult TODO
type CreateInventoryTriggerJobResult struct {
	XMLName    xml.Name                   `xml:"Response"`
	RequestId  string                     `xml:"RequestId,omitempty"`
	JobsDetail *InventoryTriggerJobDetail `xml:"JobsDetail,omitempty"`
}

// CreateInventoryTriggerJob TODO
func (s *CIService) CreateInventoryTriggerJob(ctx context.Context, opt *CreateInventoryTriggerJobOptions) (*CreateInventoryTriggerJobResult, *Response, error) {
	var res CreateInventoryTriggerJobResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/inventorytriggerjob",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribeInventoryTriggerJobResult TODO
type DescribeInventoryTriggerJobResult struct {
	XMLName       xml.Name                   `xml:"Response"`
	RequestId     string                     `xml:"RequestId,omitempty"`
	JobsDetail    *InventoryTriggerJobDetail `xml:"JobsDetail,omitempty"`
	NonExistJobId string                     `xml:"NonExistJobId,omitempty"`
}

// DescribeInventoryTriggerJob 查询指定存量触发工作流的任务
func (s *CIService) DescribeInventoryTriggerJob(ctx context.Context, jobId string) (*DescribeInventoryTriggerJobResult, *Response, error) {
	var res DescribeInventoryTriggerJobResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/inventorytriggerjob/" + jobId,
		method:  http.MethodGet,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribeInventoryTriggerJobsOptions TODO
type DescribeInventoryTriggerJobsOptions struct {
	NextToken         string `url:"nextToken,omitempty"`
	Size              string `url:"size,omitempty"`
	Type              string `url:"type,omitempty"`
	OrderByTime       string `url:"orderByTime,omitempty"`
	States            string `url:"states,omitempty"`
	StartCreationTime string `url:"startCreationTime,omitempty"`
	EndCreationTime   string `url:"endCreationTime,omitempty"`
	WorkflowId        string `url:"workflowId,omitempty"`
	JobId             string `url:"jobId,omitempty"`
	Name              string `url:"name,omitempty"`
}

// DescribeInventoryTriggerJobsResult TODO
type DescribeInventoryTriggerJobsResult struct {
	XMLName    xml.Name                   `xml:"Response"`
	RequestId  string                     `xml:"RequestId,omitempty"`
	JobsDetail *InventoryTriggerJobDetail `xml:"JobsDetail,omitempty"`
	NextToken  string                     `xml:"NextToken,omitempty"`
}

// DescribeInventoryTriggerJobs 查询存量触发工作流的任务
func (s *CIService) DescribeInventoryTriggerJobs(ctx context.Context, opt *DescribeInventoryTriggerJobsOptions) (*DescribeInventoryTriggerJobsResult, *Response, error) {
	var res DescribeInventoryTriggerJobsResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/inventorytriggerjob",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CancelInventoryTriggerJob TODO
func (s *CIService) CancelInventoryTriggerJob(ctx context.Context, jobId string) (*Response, error) {
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/inventorytriggerjob/" + jobId,
		method:  http.MethodPut,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return resp, err
}

// CreateImageSearchBucketOptions 开通以图搜图选项
type CreateImageSearchBucketOptions struct {
	XMLName     xml.Name `xml:"Request"`
	MaxCapacity string   `xml:"MaxCapacity,omitempty"`
	MaxQps      string   `xml:"MaxQps,omitempty"`
}

// CreateImageSearchBucket 开通以图搜图
func (s *CIService) CreateImageSearchBucket(ctx context.Context, opt *CreateImageSearchBucketOptions) (*Response, error) {
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/ImageSearchBucket",
		body:    opt,
		method:  http.MethodPost,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return resp, err
}

// AddImageOptions 添加图库图片选项
type AddImageOptions struct {
	XMLName       xml.Name `xml:"Request"`
	EntityId      string   `xml:"EntityId,omitempty"`
	CustomContent string   `xml:"CustomContent,omitempty"`
	Tags          string   `xml:"Tags,omitempty"`
}

// AddImage 添加图库图片
func (s *CIService) AddImage(ctx context.Context, name string, opt *AddImageOptions) (*Response, error) {
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.BucketURL,
		uri:     "/" + encodeURIComponent(name) + "?ci-process=ImageSearch&action=AddImage",
		body:    opt,
		method:  http.MethodPost,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return resp, err
}

// ImageSearchOptions 图片搜索接口选项
type ImageSearchOptions struct {
	MatchThreshold int    `url:"MatchThreshold,omitempty"`
	Offset         int    `url:"Offset,omitempty"`
	Limit          int    `url:"Limit,omitempty"`
	Filter         string `url:"Filter,omitempty"`
}

// ImageSearchResult 图片搜索接口结果
type ImageSearchResult struct {
	XMLName    xml.Name `xml:"Response"`
	Count      int      `xml:"Count"`
	RequestId  string   `xml:"RequestId"`
	ImageInfos []*struct {
		EntityId      string `xml:"EntityId"`
		CustomContent string `xml:"CustomContent"`
		Tags          string `xml:"Tags"`
		PicName       string `xml:"PicName"`
		Score         int    `xml:"Score"`
	} `xml:"ImageInfos,omitempty"`
}

// ImageSearch 图片搜索接口
func (s *CIService) ImageSearch(ctx context.Context, name string, opt *ImageSearchOptions) (*ImageSearchResult, *Response, error) {
	var res ImageSearchResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.BucketURL,
		uri:      "/" + encodeURIComponent(name) + "?ci-process=ImageSearch&action=SearchImage",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DelImageOptions 删除图库图片选项
type DelImageOptions struct {
	XMLName  xml.Name `xml:"Request"`
	EntityId string   `xml:"EntityId,omitempty"`
}

// DelImage 删除图库图片
func (s *CIService) DelImage(ctx context.Context, name string, opt *DelImageOptions) (*Response, error) {
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.BucketURL,
		uri:     "/" + encodeURIComponent(name) + "?ci-process=ImageSearch&action=DeleteImage",
		body:    opt,
		method:  http.MethodPost,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return resp, err
}

// CreateJobsOptions 提交任务的公用结构体
type CreateJobsOptions CreateMediaJobsOptions

// CreateJobsOptions 任务结果的公用结构体
type CreateJobsResult CreateMediaJobsResult

// CreateJobsOptions 提交任务的公用方法
func (s *CIService) CreateJob(ctx context.Context, opt *CreateJobsOptions) (*CreateJobsResult, *Response, error) {
	var res CreateJobsResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/jobs",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateJobsOptions 提交任务的公用方法
func (s *CIService) CancelJob(ctx context.Context, jobId string) (*Response, error) {
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/jobs/" + jobId + "?cancel",
		method:  http.MethodPut,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return resp, err
}

// DescribeJobsOptions 查询任务的公用选项
type DescribeJobsOptions DescribeMediaJobsOptions

// DescribeJobsResult 查询任务结果的公用结构体
type DescribeJobsResult DescribeMediaJobsResult

// DescribeJobs 查询任务列表的公用方法
func (s *CIService) DescribeJobs(ctx context.Context, opt *DescribeJobsOptions) (*DescribeJobsResult, *Response, error) {
	var res DescribeJobsResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/jobs",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribeJobs 查询指定任务的公用方法
func (s *CIService) DescribeJob(ctx context.Context, jobid string) (*DescribeJobsResult, *Response, error) {
	var res DescribeJobsResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/jobs/" + jobid,
		method:  http.MethodGet,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// JobsNotifyBody TODO
type JobsNotifyBody struct {
	XMLName    xml.Name                `xml:"Response"`
	EventName  string                  `xml:"EventName"`
	JobsDetail []MediaProcessJobDetail `xml:"JobsDetail,omitempty"`
}

// ModifyM3U8TokenOptions TODO
type ModifyM3U8TokenOptions struct {
	Token string `url:"token"`
}

// ModifyM3U8Token TODO
func (s *CIService) ModifyM3U8Token(ctx context.Context, name string, opt *ModifyM3U8TokenOptions, id ...string) (*Response, error) {
	var u string
	if len(id) == 1 {
		u = fmt.Sprintf("/%s?versionId=%s&ci-process=modifym3u8token", encodeURIComponent(name), id[0])
	} else if len(id) == 0 {
		u = fmt.Sprintf("/%s?ci-process=modifym3u8token", encodeURIComponent(name))
	} else {
		return nil, fmt.Errorf("wrong params")
	}

	sendOpt := sendOptions{
		baseURL:          s.client.BaseURL.BucketURL,
		uri:              u,
		method:           http.MethodGet,
		optQuery:         opt,
		disableCloseBody: true,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return resp, err
}

// DescribeJobsOptions 查询模板的公用选项
type DescribeTemplateOptions DescribeMediaTemplateOptions

// DescribeJobsResult 查询模板结果的公用结构体
type DescribeTemplateResult DescribeMediaTemplateResult

// DescribeTemplate 搜索模板的公用方法
func (s *CIService) DescribeTemplate(ctx context.Context, opt *DescribeTemplateOptions) (*DescribeTemplateResult, *Response, error) {
	var res DescribeTemplateResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/template",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribeJobsResult 删除模板结果的公用结构体
type DeleteTemplateResult DeleteMediaTemplateResult

// DeleteTemplate 删除模板的公用方法
func (s *CIService) DeleteTemplate(ctx context.Context, tempalteId string) (*DeleteTemplateResult, *Response, error) {
	var res DeleteTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + tempalteId,
		method:  http.MethodDelete,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// FillConcat 填充拼接
type FillConcat struct {
	Format    string            `xml:"Format,omitempty"`
	FillInput []FillConcatInput `xml:"FillInput,omitempty"`
	RefMode   string            `xml:"RefMode,omitempty"`
	RefIndex  string            `xml:"RefIndex,omitempty"`
}

// FillConcatInput 填充拼接输入
type FillConcatInput struct {
	Url      string `xml:"Url,omitempty"`
	FillTime string `xml:"FillTime,omitempty"`
}

// VideoSynthesis 视频合成
type VideoSynthesis struct {
	KeepAudioTrack string                     `xml:"KeepAudioTrack,omitempty"`
	SpliceInfo     []VideoSynthesisSpliceInfo `xml:"SpliceInfo,omitempty"`
}

// VideoSynthesisSpliceInfo 视频合成输入
type VideoSynthesisSpliceInfo struct {
	Url    string `xml:"Url,omitempty"`
	X      string `xml:"X,omitempty"`
	Y      string `xml:"Y,omitempty"`
	Width  string `xml:"Width,omitempty"`
	Height string `xml:"Height,omitempty"`
}

// DnaConfig DNA任务配置
type DnaConfig struct {
	RuleType string `xml:"RuleType,omitempty"`
	DnaDbId  string `xml:"DnaDbId,omitempty"`
	VideoId  string `xml:"VideoId,omitempty"`
}

// DnaResult DNA任务结果
type DnaResult struct {
	VideoId   string              `xml:"VideoId,omitempty"`
	Duration  int                 `xml:"Duration,omitempty"`
	Detection *DnaResultDetection `xml:"Detection,omitempty"`
}

// DnaResultDetection DNA任务结果
type DnaResultDetection struct {
	VideoId         string                 `xml:"VideoId,omitempty"`
	Similar         int                    `xml:"Similar,omitempty"`
	SimilarDuration int                    `xml:"SimilarDuration,omitempty"`
	Duration        int                    `xml:"Duration,omitempty"`
	MatchDetail     []DnaResultMatchDetail `xml:"MatchDetail,omitempty"`
	Audio           DnaResultAudio         `xml:"Audio,omitempty"`
}

// DnaResultMatchDetail DNA任务结果
type DnaResultMatchDetail struct {
	MatchStartTime int `xml:"MatchStartTime,omitempty"`
	MatchEndTime   int `xml:"MatchEndTime,omitempty"`
	SrcStartTime   int `xml:"SrcStartTime,omitempty"`
	SrcEndTime     int `xml:"SrcEndTime,omitempty"`
}

// DnaResultAudio DNA任务结果
type DnaResultAudio struct {
	Similar int `xml:"Similar,omitempty"`
}

// GetDnaDbOptions 查询 DNA 库列表参数
type GetDnaDbOptions struct {
	Ids        string `url:"ids,omitempty"`
	PageNumber string `url:"pageNumber,omitempty"`
	PageSize   string `url:"pageSize,omitempty"`
}

// GetDnaDbResult 查询 DNA 库列表结果
type GetDnaDbResult struct {
	XMLName     xml.Name      `xml:"Response"`
	RequestId   string        `xml:"RequestId,omitempty"`
	TotalCount  int           `xml:"TotalCount,omitempty"`
	PageNumber  int           `xml:"PageNumber,omitempty"`
	PageSize    int           `xml:"PageSize,omitempty"`
	DNADbConfig []DNADbConfig `xml:"DNADbConfig,omitempty"`
	NonExistIDs []string      `xml:"NonExistIDs,omitempty"`
}

// DNADbConfig DNA 库详情
type DNADbConfig struct {
	BucketId    string `xml:"BucketId,omitempty"`
	Region      string `xml:"Region,omitempty"`
	DNADbId     string `xml:"DNADbId,omitempty"`
	DNADbName   string `xml:"DNADbName,omitempty"`
	Capacity    int    `xml:"Capacity,omitempty"`
	Description string `xml:"Description,omitempty"`
	UpdateTime  string `xml:"UpdateTime,omitempty"`
	CreateTime  string `xml:"CreateTime,omitempty"`
}

// GetDnaDb 查询 DNA 库列表
func (s *CIService) GetDnaDb(ctx context.Context, opt *GetDnaDbOptions) (*GetDnaDbResult, *Response, error) {
	var res GetDnaDbResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/dnadb",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// GetDnaDbFilesOptions 获取 DNA 库中文件列表参数
type GetDnaDbFilesOptions struct {
	object     string `url:"object,omitempty"`
	DnaDbId    string `url:"dnaDbId,omitempty"`
	PageNumber string `url:"pageNumber,omitempty"`
	PageSize   string `url:"pageSize,omitempty"`
}

// GetDnaDbFilesResult 查询 DNA 库列表结果
type GetDnaDbFilesResult struct {
	XMLName    xml.Name     `xml:"Response"`
	RequestId  string       `xml:"RequestId,omitempty"`
	TotalCount int          `xml:"TotalCount,omitempty"`
	PageNumber int          `xml:"PageNumber,omitempty"`
	PageSize   int          `xml:"PageSize,omitempty"`
	DNADbFiles []DNADbFiles `xml:"DNADbFiles,omitempty"`
}

// DNADbFiles DNA 文件详情
type DNADbFiles struct {
	BucketId   string `xml:"BucketId,omitempty"`
	Region     string `xml:"Region,omitempty"`
	DNADbId    string `xml:"DNADbId,omitempty"`
	VideoId    string `xml:"VideoId,omitempty"`
	Object     int    `xml:"Object,omitempty"`
	ETag       string `xml:"ETag,omitempty"`
	UpdateTime string `xml:"UpdateTime,omitempty"`
	CreateTime string `xml:"CreateTime,omitempty"`
}

// GetDnaDbFiles 查询 DNA 库列表
func (s *CIService) GetDnaDbFiles(ctx context.Context, opt *GetDnaDbFilesOptions) (*GetDnaDbFilesResult, *Response, error) {
	var res GetDnaDbFilesResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/dnadb_files",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// VocalScore 音乐评分
type VocalScore struct {
	StandardObject string `xml:"StandardObject,omitempty"`
}

// VocalScoreResult 音乐评分结果
type VocalScoreResult struct {
	PitchScore   *VocalScoreResultPitchScore   `xml:"PitchScore,omitempty"`
	RhythemScore *VocalScoreResultRhythemScore `xml:"RhythemScore,omitempty"`
}

// VocalScoreResultPitchScore 音高评分
type VocalScoreResultPitchScore struct {
	TotalScore     float64                          `xml:"TotalScore,omitempty"`
	SentenceScores []VocalScoreResultSentenceScores `xml:"SentenceScores,omitempty"`
}

// VocalScoreResultRhythemScore 节奏评分
type VocalScoreResultSentenceScores struct {
	StartTime float64 `xml:"StartTime,omitempty"`
	EndTime   float64 `xml:"EndTime,omitempty"`
	Score     float64 `xml:"Score,omitempty"`
}

type VocalScoreResultRhythemScore struct {
	TotalScore     float64                          `xml:"TotalScore,omitempty"`
	SentenceScores []VocalScoreResultSentenceScores `xml:"SentenceScores,omitempty"`
}

// ImageInspect 黑产检测
type ImageInspect struct {
	AutoProcess string `xml:"AutoProcess,omitempty"`
	ProcessType string `xml:"ProcessType,omitempty"`
}

// ImageInspectResult 黑产检测结果
type ImageInspectResult struct {
	State           string                     `xml:"State,omitempty"`
	Code            string                     `xml:"Code,omitempty"`
	Message         string                     `xml:"Message,omitempty"`
	InputObjectName string                     `xml:"InputObjectName,omitempty"`
	InputObjectUrl  string                     `xml:"InputObjectUrl,omitempty"`
	ProcessResult   *ImageInspectProcessResult `xml:"ProcessResult,omitempty"`
}

// ImageInspectProcessResult 黑产检测结果
type ImageInspectProcessResult struct {
	PicSize             int                            `xml:"PicSize,omitempty"`
	PicType             string                         `xml:"PicType,omitempty"`
	Suspicious          string                         `xml:"Suspicious,omitempty"`
	SuspiciousBeginByte int                            `xml:"SuspiciousBeginByte,omitempty"`
	SuspiciousEndByte   int                            `xml:"SuspiciousEndByte,omitempty"`
	SuspiciousSize      int                            `xml:"SuspiciousSize,omitempty"`
	SuspiciousType      string                         `xml:"SuspiciousType,omitempty"`
	AutoProcessResult   *ImageInspectAutoProcessResult `xml:"AutoProcessResult,omitempty"`
}

// ImageInspectAutoProcessResult 黑产检测结果
type ImageInspectAutoProcessResult struct {
	Code    string `xml:"Code,omitempty"`
	Message string `xml:"Message,omitempty"`
}

type CosImageInspectOptions struct {
}

// CosImageInspectProcessResult 黑产检测同步接口结果
type CosImageInspectProcessResult struct {
	PicSize             int    `json:"picSize,omitempty"`
	PicType             string `json:"picType,omitempty"`
	Suspicious          bool   `json:"suspicious,omitempty"`
	SuspiciousBeginByte int    `json:"suspiciousBeginByte,omitempty"`
	SuspiciousEndByte   int    `json:"suspiciousEndByte,omitempty"`
	SuspiciousSize      int    `json:"suspiciousSize,omitempty"`
	SuspiciousType      string `json:"suspiciousType,omitempty"`
}

// Write 回包是json格式序列化
func (w *CosImageInspectProcessResult) Write(p []byte) (n int, err error) {
	err = json.Unmarshal(p, w)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// CosImageInspect 黑产检查同步接口
// 参数:
//
//	ctx: 上下文对象
//	name: 图片名称
//	opt: 选项对象
//
// 返回值:
//
//	*CosImageInspectProcessResult: 图片检查结果
//	*Response: HTTP响应对象
//	error: 错误信息
func (s *CIService) CosImageInspect(ctx context.Context, name string, opt *CosImageInspectOptions) (*CosImageInspectProcessResult, *Response, error) {
	var res CosImageInspectProcessResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.BucketURL,
		uri:      "/" + encodeURIComponent(name) + "?ci-process=ImageInspect",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateOCRTemplateOptions TODO
type CreateOCRTemplateOptions struct {
	XMLName  xml.Name          `xml:"Request"`
	Tag      string            `xml:"Tag,omitempty"`
	Name     string            `xml:"Name,omitempty"`
	ImageOCR *ImageOCRTemplate `xml:"ImageOCR,omitempty" json:"ImageOCR,omitempty"`
}

// ImageOCRTemplate TODO
type ImageOCRTemplate struct {
	Type              string `xml:"Type,omitempty"`
	LanguageType      string `xml:"LanguageType,omitempty"`
	IsPdf             string `xml:"IsPdf,omitempty"`
	PdfPageNumber     string `xml:"PdfPageNumber,omitempty"`
	IsWord            string `xml:"IsWord,omitempty"`
	EnableWordPolygon string `xml:"EnableWordPolygon,omitempty"`
}

// CreateOCRTemplate 创建OCR模板
func (s *CIService) CreateOCRTemplate(ctx context.Context, opt *CreateOCRTemplateOptions) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// UpdateOCRTemplate 更新OCR模板
func (s *CIService) UpdateOCRTemplate(ctx context.Context, opt *CreateOCRTemplateOptions, templateId string) (*CreateMediaTemplateResult, *Response, error) {
	var res CreateMediaTemplateResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/template/" + templateId,
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// ImageOCRDetection TODO
type ImageOCRDetection struct {
	InputObjectName string                   `xml:"InputObjectName,omitempty"`
	InputObjectUrl  string                   `xml:"InputObjectUrl,omitempty"`
	ObjectName      string                   `xml:"ObjectName,omitempty"`
	ObjectUrl       string                   `xml:"ObjectUrl,omitempty"`
	Code            string                   `xml:"Code,omitempty"`
	State           string                   `xml:"State,omitempty"`
	Message         string                   `xml:"Message,omitempty"`
	TextDetections  []ImageOCRTextDetections `xml:"TextDetections,omitempty"`
	Language        string                   `xml:"Language,omitempty"`
	Angel           string                   `xml:"Angel,omitempty"`
	PdfPageSize     int                      `xml:"PdfPageSize,omitempty"`
}

// ImageOCRTextDetections TODO
type ImageOCRTextDetections struct {
	DetectedText string                    `xml:"DetectedText,omitempty"`
	Confidence   int                       `xml:"Confidence,omitempty"`
	Polygon      []ImageOCRTextPolygon     `xml:"Polygon,omitempty"`
	ItemPolygon  []ImageOCRTextItemPolygon `xml:"ItemPolygon,omitempty"`
	Words        []ImageOCRTextWords       `xml:"Words,omitempty"`
	WordPolygon  []ImageOCRTextWordPolygon `xml:"WordPolygon,omitempty"`
}

// ImageOCRTextPolygon TODO
type ImageOCRTextPolygon struct {
	X int `xml:"X,omitempty"`
	Y int `xml:"Y,omitempty"`
}

// ImageOCRTextItemPolygon TODO
type ImageOCRTextItemPolygon struct {
	X      int `xml:"X,omitempty"`
	Y      int `xml:"Y,omitempty"`
	Width  int `xml:"Width,omitempty"`
	Height int `xml:"Height,omitempty"`
}

// ImageOCRTextWords TODO
type ImageOCRTextWords struct {
	Confidence     int    `xml:"Confidence,omitempty"`
	Character      string `xml:"Character,omitempty"`
	WordCoordPoint []struct {
		WordCoordinate []struct {
			X int `xml:"X,omitempty"`
			Y int `xml:"Y,omitempty"`
		} `xml:"WordCoordinate,omitempty"`
	} `xml:"WordCoordPoint,omitempty"`
}

// ImageOCRTextWordPolygon TODO
type ImageOCRTextWordPolygon struct {
	LeftTop []struct {
		X int `xml:"X,omitempty"`
		Y int `xml:"Y,omitempty"`
	} `xml:"LeftTop,omitempty"`
	RightTop []struct {
		X int `xml:"X,omitempty"`
		Y int `xml:"Y,omitempty"`
	} `xml:"RightTop,omitempty"`
	LeftBottom []struct {
		X int `xml:"X,omitempty"`
		Y int `xml:"Y,omitempty"`
	} `xml:"LeftBottom,omitempty"`
	RightBottom []struct {
		X int `xml:"X,omitempty"`
		Y int `xml:"Y,omitempty"`
	} `xml:"RightBottom,omitempty"`
}

type LiveTanscodeVideo struct {
	Codec         string           `xml:"Codec"`
	Width         string           `xml:"Width,omitempty"`
	Height        string           `xml:"Height,omitempty"`
	Fps           string           `xml:"Fps,omitempty"`
	Profile       string           `xml:"Profile,omitempty"`
	Bitrate       string           `xml:"Bitrate,omitempty"`
	Gop           string           `xml:"Gop,omitempty"`
	Maxrate       string           `xml:"Maxrate,omitempty"`
	Crf           string           `xml:"Crf,omitempty"`
	Pixfmt        string           `xml:"Pixfmt,omitempty"`
	LongShortMode string           `xml:"LongShortMode,omitempty"`
	Interlaced    string           `xml:"Interlaced,omitempty"`
	ColorParam    *VideoColorParam `xml:"ColorParam,omitempty"`
}

type LiveTanscodeTransConfig struct {
	InitialClipNum string      `xml:"InitialClipNum,omitempty"`
	CosTag         string      `xml:"CosTag,omitempty"`
	HlsEncrypt     *HlsEncrypt `xml:"HlsEncrypt,omitempty"`
}

type LiveTanscode struct {
	Container *Container         `xml:"Container,omitempty"`
	Video     *LiveTanscodeVideo `xml:"Video,omitempty"`
	// TimeInterval  *TimeInterval `xml:"TimeInterval,omitempty"`
	// Audio         *Audio        `xml:"Audio,omitempty"`
	TransConfig *LiveTanscodeTransConfig `xml:"TransConfig,omitempty"`
}

type GeneratePlayListJobOperation struct {
	Tag                 string        `xml:"Tag,omitempty"`
	Output              *JobOutput    `xml:"Output,omitempty"`
	MediaResult         *MediaResult  `xml:"MediaResult,omitempty"`
	MediaInfo           *MediaInfo    `xml:"MediaInfo,omitempty"`
	Transcode           *LiveTanscode `xml:"Transcode,omitempty"`
	UserData            string        `xml:"UserData,omitempty"`
	JobLevel            int           `xml:"JobLevel,omitempty"`
	Watermark           []Watermark   `xml:"Watermark,omitempty"`
	WatermarkTemplateId []string      `xml:"WatermarkTemplateId,omitempty"`
}

type CreateGeneratePlayListJobOptions struct {
	XMLName             xml.Name                      `xml:"Request"`
	Tag                 string                        `xml:"Tag,omitempty"`
	Input               *JobInput                     `xml:"Input,omitempty"`
	Operation           *GeneratePlayListJobOperation `xml:"Operation,omitempty"`
	QueueId             string                        `xml:"QueueId,omitempty"`
	QueueType           string                        `xml:"QueueType,omitempty"`
	CallBackFormat      string                        `xml:"CallBackFormat,omitempty"`
	CallBackType        string                        `xml:"CallBackType,omitempty"`
	CallBack            string                        `xml:"CallBack,omitempty"`
	CallBackMqConfig    *NotifyConfigCallBackMqConfig `xml:"CallBackMqConfig,omitempty"`
	CallBackKafkaConfig *KafkaConfig                  `xml:"CallBackKafkaConfig,omitempty"`
}

func (s *CIService) CreateGeneratePlayListJob(ctx context.Context, opt *CreateGeneratePlayListJobOptions) (*CreateJobsResult, *Response, error) {
	var res CreateJobsResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/jobs",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// CreateMultiGeneratePlayListJobsOptions TODO
type CreateMultiGeneratePlayListJobsOptions struct {
	XMLName             xml.Name                       `xml:"Request"`
	Tag                 string                         `xml:"Tag,omitempty"`
	Input               *JobInput                      `xml:"Input,omitempty"`
	Operation           []GeneratePlayListJobOperation `xml:"Operation,omitempty"`
	QueueId             string                         `xml:"QueueId,omitempty"`
	QueueType           string                         `xml:"QueueType,omitempty"`
	CallBackFormat      string                         `xml:"CallBackFormat,omitempty"`
	CallBackType        string                         `xml:"CallBackType,omitempty"`
	CallBack            string                         `xml:"CallBack,omitempty"`
	CallBackMqConfig    *NotifyConfigCallBackMqConfig  `xml:"CallBackMqConfig,omitempty"`
	CallBackKafkaConfig *KafkaConfig                   `xml:"CallBackKafkaConfig,omitempty"`
}

// CreateMultiGeneratePlayListJobs TODO
func (s *CIService) CreateMultiGeneratePlayListJobs(ctx context.Context, opt *CreateMultiGeneratePlayListJobsOptions) (*CreateMultiMediaJobsResult, *Response, error) {
	var res CreateMultiMediaJobsResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/jobs",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

type VocabularyWeight struct {
	Vocabulary string `xml:"Vocabulary,omitempty"`
	Weight     int    `xml:"Weight,omitempty"`
}

// CreateAsrVocabularyTableOptions TODO
type CreateAsrVocabularyTableOptions struct {
	XMLName             xml.Name           `xml:"Request"`
	TableName           string             `xml:"TableName,omitempty"`
	TableDescription    string             `xml:"TableDescription,omitempty"`
	VocabularyWeights   []VocabularyWeight `xml:"VocabularyWeights,omitempty"`
	VocabularyWeightStr string             `xml:"VocabularyWeightStr,omitempty"`
}

// CreateAsrVocabularyTableResult TODO
type CreateAsrVocabularyTableResult struct {
	XMLName   xml.Name `xml:"Response"`
	Code      string   `xml:"Code,omitempty"`
	Message   string   `xml:"Message,omitempty"`
	TableId   string   `xml:"TableId,omitempty"`
	RequestId string   `xml:"RequestId,omitempty"`
}

func (s *CIService) CreateAsrVocabularyTable(ctx context.Context, opt *CreateAsrVocabularyTableOptions) (*CreateAsrVocabularyTableResult, *Response, error) {
	var res CreateAsrVocabularyTableResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/asrhotvocabtable",
		method:  http.MethodPost,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DeleteAsrVocabularyTable TODO
func (s *CIService) DeleteAsrVocabularyTable(ctx context.Context, tableId string) (*Response, error) {
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/asrhotvocabtable/" + tableId,
		method:  http.MethodDelete,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return resp, err
}

// UpdateAsrVocabularyTableOptions TODO
type UpdateAsrVocabularyTableOptions struct {
	XMLName             xml.Name           `xml:"Request"`
	TableId             string             `xml:"TableId,omitempty"`
	TableName           string             `xml:"TableName,omitempty"`
	TableDescription    string             `xml:"TableDescription,omitempty"`
	VocabularyWeights   []VocabularyWeight `xml:"VocabularyWeights,omitempty"`
	VocabularyWeightStr string             `xml:"VocabularyWeightStr,omitempty"`
}

type UpdateAsrVocabularyTableResult CreateAsrVocabularyTableResult

// UpdateAsrVocabularyTable TODO
func (s *CIService) UpdateAsrVocabularyTable(ctx context.Context, opt *UpdateAsrVocabularyTableOptions) (*UpdateAsrVocabularyTableResult, *Response, error) {
	var res UpdateAsrVocabularyTableResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/asrhotvocabtable",
		method:  http.MethodPut,
		body:    opt,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

type VocabularyTable struct {
	TableId             string             `xml:"TableId,omitempty"`
	TableName           string             `xml:"TableName,omitempty"`
	TableDescription    string             `xml:"TableDescription,omitempty"`
	VocabularyWeights   []VocabularyWeight `xml:"VocabularyWeights,omitempty"`
	VocabularyWeightStr string             `xml:"VocabularyWeightStr,omitempty"`
	CreateTime          string             `xml:"CreateTime,omitempty"`
	UpdateTime          string             `xml:"UpdateTime,omitempty"`
}

// DescribeAsrVocabularyTableResult TODO
type DescribeAsrVocabularyTableResult struct {
	XMLName             xml.Name           `xml:"Response"`
	RequestId           string             `xml:"RequestId,omitempty"`
	TableId             string             `xml:"TableId,omitempty"`
	TableName           string             `xml:"TableName,omitempty"`
	TableDescription    string             `xml:"TableDescription,omitempty"`
	VocabularyWeights   []VocabularyWeight `xml:"VocabularyWeights,omitempty"`
	VocabularyWeightStr string             `xml:"VocabularyWeightStr,omitempty"`
	CreateTime          string             `xml:"CreateTime,omitempty"`
	UpdateTime          string             `xml:"UpdateTime,omitempty"`
}

// DescribeAsrVocabularyTable 查询指定的语音识别热词表
func (s *CIService) DescribeAsrVocabularyTable(ctx context.Context, tableId string) (*DescribeAsrVocabularyTableResult, *Response, error) {
	var res DescribeAsrVocabularyTableResult
	sendOpt := sendOptions{
		baseURL: s.client.BaseURL.CIURL,
		uri:     "/asrhotvocabtable/" + tableId,
		method:  http.MethodGet,
		result:  &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}

// DescribeAsrVocabularyTablesOptions TODO
type DescribeAsrVocabularyTablesOptions struct {
	Offset int `url:"offset,omitempty"`
	Limit  int `url:"limit,omitempty"`
}

type DescribeAsrVocabularyTablesResult struct {
	XMLName         xml.Name          `xml:"Response"`
	RequestId       string            `xml:"RequestId,omitempty"`
	TotalCount      string            `xml:"TotalCount,omitempty"`
	VocabularyTable []VocabularyTable `xml:"VocabularyTable,omitempty"`
}

// DescribeAsrVocabularyTables 查询语音识别热词表列表
func (s *CIService) DescribeAsrVocabularyTables(ctx context.Context, opt *DescribeAsrVocabularyTablesOptions) (*DescribeAsrVocabularyTablesResult, *Response, error) {
	var res DescribeAsrVocabularyTablesResult
	sendOpt := sendOptions{
		baseURL:  s.client.BaseURL.CIURL,
		uri:      "/asrhotvocabtable",
		optQuery: opt,
		method:   http.MethodGet,
		result:   &res,
	}
	resp, err := s.client.send(ctx, &sendOpt)
	return &res, resp, err
}
