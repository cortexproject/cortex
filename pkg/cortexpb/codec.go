package cortexpb

import (
	"fmt"

	gogoproto "github.com/gogo/protobuf/proto"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

// Name is the name registered for the proto codec.
const Name = "proto"

func init() {
	encoding.RegisterCodecV2(&cortexCodec{})
}

type ReleasableMessage interface {
	RegisterBuffer(mem.Buffer)
	buffers() []mem.Buffer
	unref()
}

type GogoProtoMessage interface {
	MarshalToSizedBuffer(dAtA []byte) (int, error)
}

type cortexCodec struct{}

func (c cortexCodec) Name() string {
	return Name
}

// Marshal is basically the same as https://github.com/grpc/grpc-go/blob/d2e836604b36400a54fbf04af495d12b38fa1e3a/encoding/proto/proto.go#L43-L67
// but it uses gogo proto methods where applicable.
func (c *cortexCodec) Marshal(v any) (data mem.BufferSlice, err error) {
	vv := messageV2Of(v)
	if vv == nil {
		return nil, fmt.Errorf("proto: failed to marshal, message is %T, want proto.Message", v)
	}

	var size int
	if sizer, ok := v.(gogoproto.Sizer); ok {
		size = sizer.Size()
	} else {
		size = proto.Size(vv)
	}

	if mem.IsBelowBufferPoolingThreshold(size) {
		var buf mem.SliceBuffer

		// If v implements MarshalToSizedBuffer we should use it as it is more optimized
		if m, ok := v.(GogoProtoMessage); ok {
			buf = make([]byte, size)
			if _, err := m.MarshalToSizedBuffer(buf[:size]); err != nil {
				return nil, err
			}
		} else {
			buf, err = proto.Marshal(vv)
			if err != nil {
				return nil, err
			}
		}

		data = append(data, buf)
	} else {
		pool := mem.DefaultBufferPool()
		buf := pool.Get(size)

		// If v implements MarshalToSizedBuffer we should use it as it is more optimized
		if m, ok := v.(GogoProtoMessage); ok {
			if _, err := m.MarshalToSizedBuffer((*buf)[:size]); err != nil {
				pool.Put(buf)
				return nil, err
			}
		} else {
			if _, err := (proto.MarshalOptions{}).MarshalAppend((*buf)[:0], vv); err != nil {
				pool.Put(buf)
				return nil, err
			}
		}

		data = append(data, mem.NewBuffer(buf, pool))
	}

	return data, nil
}

// Unmarshal Copied from https://github.com/grpc/grpc-go/blob/d2e836604b36400a54fbf04af495d12b38fa1e3a/encoding/proto/proto.go#L69-L81
// but without releasing the buffer
func (c *cortexCodec) Unmarshal(data mem.BufferSlice, v any) error {
	vv := messageV2Of(v)
	if vv == nil {
		return fmt.Errorf("failed to unmarshal, message is %T, want proto.Message", v)
	}

	// To be in the safe side, we will never automatically release the buffer used to Unmarshal the message automatically.
	// This should simulate the same behavior of grpc v1.65.0 and before.
	buf := data.MaterializeToBuffer(mem.DefaultBufferPool())

	err := proto.Unmarshal(buf.ReadOnlyData(), vv)

	if err != nil {
		defer buf.Free()
		return err
	}

	// If v implements ReleasableMessage interface, we add the buff to be freed later when the request is no longer being used
	if fm, ok := v.(ReleasableMessage); ok {
		fm.RegisterBuffer(buf)
	}

	return err
}

func messageV2Of(v any) proto.Message {
	switch v := v.(type) {
	case protoadapt.MessageV1:
		return protoadapt.MessageV2Of(v)
	case protoadapt.MessageV2:
		return v
	}

	return nil
}

var _ ReleasableMessage = &MessageWithBufRef{}

type MessageWithBufRef struct {
	bs mem.BufferSlice
}

func (m *MessageWithBufRef) unref() {
	m.bs = nil
}

func (m *MessageWithBufRef) buffers() []mem.Buffer {
	return m.bs
}

func (m *MessageWithBufRef) RegisterBuffer(buffer mem.Buffer) {
	m.bs = append(m.bs, buffer)
}

func (m *MessageWithBufRef) MergeBuffers(o any) {
	if fm, ok := o.(ReleasableMessage); ok {
		m.bs = append(m.bs, fm.buffers()...)
		fm.unref()
	}
}

func (m *MessageWithBufRef) Free() {
	m.bs.Free()
	m.bs = m.bs[:0]
}
