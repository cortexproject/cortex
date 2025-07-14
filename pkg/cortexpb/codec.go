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

var noOpBufferPool = &mem.NopBufferPool{}

func init() {
	encoding.RegisterCodecV2(&cortexCodec{})
}

type ReleasableMessage interface {
	RegisterBuffer(mem.Buffer)
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

	// To be safe, we avoid automatically releasing the buffer used to unmarshal the message.
	// Additionally, we avoid using a pooled byte slice unless the message implements ReleasableMessage.
	// This mimics the behavior of gRPC versions 1.65.0 and earlier.
	rm, ok := v.(ReleasableMessage)
	bufferPool := mem.DefaultBufferPool()

	if !ok {
		bufferPool = noOpBufferPool
	}

	buf := data.MaterializeToBuffer(bufferPool)
	err := proto.Unmarshal(buf.ReadOnlyData(), vv)

	if err != nil {
		defer buf.Free()
		return err
	}

	// If v implements ReleasableMessage interface, we add the buff to be freed later when the request is no longer being used
	if rm != nil {
		rm.RegisterBuffer(buf)
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

func (m *MessageWithBufRef) RegisterBuffer(buffer mem.Buffer) {
	m.bs = append(m.bs, buffer)
}

func (m *MessageWithBufRef) Free() {
	m.bs.Free()
	m.bs = m.bs[:0]
}
