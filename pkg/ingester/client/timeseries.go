package client

import (
	"flag"
	"fmt"
	"io"
	"strings"
	"sync"
	"unsafe"

	"github.com/prometheus/prometheus/pkg/labels"
)

var (
	expectedTimeseries       = 100
	expectedLabels           = 20
	expectedSamplesPerSeries = 10
	expectedMetadata         = 200

	/*
		We cannot pool these as pointer-to-slice because the place we use them is in WriteRequest which is generated from Protobuf
		and we don't have an option to make it a pointer. There is overhead here 24 bytes of garbage every time a PreallocTimeseries
		is re-used. But since the slices are far far larger, we come out ahead.
	*/
	slicePool = sync.Pool{
		New: func() interface{} {
			return make([]PreallocTimeseries, 0, expectedTimeseries)
		},
	}

	sliceMetadataPool = sync.Pool{
		New: func() interface{} {
			return make([]PreallocMetricMetadata, 0, expectedMetadata)
		},
	}

	timeSeriesPool = sync.Pool{
		New: func() interface{} {
			return &TimeSeries{
				Labels:  make([]LabelAdapter, 0, expectedLabels),
				Samples: make([]Sample, 0, expectedSamplesPerSeries),
			}
		},
	}
	metadataPool = sync.Pool{
		New: func() interface{} {
			return &MetricMetadata{}
		},
	}
)

// PreallocConfig configures how structures will be preallocated to optimise
// proto unmarshalling.
type PreallocConfig struct{}

// RegisterFlags registers configuration settings.
func (PreallocConfig) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&expectedTimeseries, "ingester-client.expected-timeseries", expectedTimeseries, "Expected number of timeseries per request, used for preallocations.")
	f.IntVar(&expectedMetadata, "ingester-client.expected-metadata", expectedMetadata, "Expected number of metadata per request, used for preallocations.")
	f.IntVar(&expectedLabels, "ingester-client.expected-labels", expectedLabels, "Expected number of labels per timeseries, used for preallocations.")
	f.IntVar(&expectedSamplesPerSeries, "ingester-client.expected-samples-per-series", expectedSamplesPerSeries, "Expected number of samples per timeseries, used for preallocations.")
}

// PreallocWriteRequest is a WriteRequest which preallocs slices on Unmarshal.
type PreallocWriteRequest struct {
	WriteRequest
}

// Unmarshal implements proto.Message.
func (p *PreallocWriteRequest) Unmarshal(dAtA []byte) error {
	p.Timeseries = slicePool.Get().([]PreallocTimeseries)
	p.Metadata = sliceMetadataPool.Get().([]PreallocMetricMetadata)
	return p.WriteRequest.Unmarshal(dAtA)
}

// PreallocTimeseries is a TimeSeries which preallocs slices on Unmarshal.
type PreallocTimeseries struct {
	*TimeSeries
}

// Unmarshal implements proto.Message.
func (p *PreallocTimeseries) Unmarshal(dAtA []byte) error {
	p.TimeSeries = timeSeriesPool.Get().(*TimeSeries)
	return p.TimeSeries.Unmarshal(dAtA)
}

type PreallocMetricMetadata struct {
	*MetricMetadata
}

func (p *PreallocMetricMetadata) Unmarshal(dAtA []byte) error {
	p.MetricMetadata = metadataPool.Get().(*MetricMetadata)
	return p.MetricMetadata.Unmarshal(dAtA)
}

// LabelAdapter is a labels.Label that can be marshalled to/from protos.
type LabelAdapter labels.Label

// Marshal implements proto.Marshaller.
func (bs *LabelAdapter) Marshal() ([]byte, error) {
	size := bs.Size()
	buf := make([]byte, size)
	n, err := bs.MarshalToSizedBuffer(buf[:size])
	if err != nil {
		return nil, err
	}
	return buf[:n], err
}

func (bs *LabelAdapter) MarshalTo(dAtA []byte) (int, error) {
	size := bs.Size()
	return bs.MarshalToSizedBuffer(dAtA[:size])
}

// MarshalTo implements proto.Marshaller.
func (bs *LabelAdapter) MarshalToSizedBuffer(buf []byte) (n int, err error) {
	ls := (*labels.Label)(bs)
	i := len(buf)
	if len(ls.Value) > 0 {
		i -= len(ls.Value)
		copy(buf[i:], ls.Value)
		i = encodeVarintCortex(buf, i, uint64(len(ls.Value)))
		i--
		buf[i] = 0x12
	}
	if len(ls.Name) > 0 {
		i -= len(ls.Name)
		copy(buf[i:], ls.Name)
		i = encodeVarintCortex(buf, i, uint64(len(ls.Name)))
		i--
		buf[i] = 0xa
	}
	return len(buf) - i, nil
}

// Unmarshal a LabelAdapter, implements proto.Unmarshaller.
// NB this is a copy of the autogenerated code to unmarshal a LabelPair,
// with the byte copying replaced with a yoloString.
func (bs *LabelAdapter) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowCortex
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: LabelPair: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: LabelPair: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowCortex
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthCortex
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthCortex
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			bs.Name = yoloString(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowCortex
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthCortex
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthCortex
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			bs.Value = yoloString(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipCortex(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthCortex
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthCortex
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func yoloString(buf []byte) string {
	return *((*string)(unsafe.Pointer(&buf)))
}

// Size implements proto.Sizer.
func (bs *LabelAdapter) Size() (n int) {
	ls := (*labels.Label)(bs)
	if bs == nil {
		return 0
	}
	var l int
	_ = l
	l = len(ls.Name)
	if l > 0 {
		n += 1 + l + sovCortex(uint64(l))
	}
	l = len(ls.Value)
	if l > 0 {
		n += 1 + l + sovCortex(uint64(l))
	}
	return n
}

// Equal implements proto.Equaler.
func (bs *LabelAdapter) Equal(other LabelAdapter) bool {
	return bs.Name == other.Name && bs.Value == other.Value
}

// Compare implements proto.Comparer.
func (bs *LabelAdapter) Compare(other LabelAdapter) int {
	if c := strings.Compare(bs.Name, other.Name); c != 0 {
		return c
	}
	return strings.Compare(bs.Value, other.Value)
}

// ReuseSlice puts the slice back into a sync.Pool for reuse.
func ReuseSlice(ts []PreallocTimeseries, m []PreallocMetricMetadata) {
	for i := range ts {
		ReuseTimeseries(ts[i].TimeSeries)
	}

	for i := range m {
		metadataPool.Put(m[i].MetricMetadata)
	}

	slicePool.Put(ts[:0]) //nolint:staticcheck //see comment on slicePool for more details
	sliceMetadataPool.Put(m[:0])
}

// ReuseTimeseries puts the timeseries back into a sync.Pool for reuse.
func ReuseTimeseries(ts *TimeSeries) {
	ts.Labels = ts.Labels[:0]
	ts.Samples = ts.Samples[:0]
	timeSeriesPool.Put(ts)
}
