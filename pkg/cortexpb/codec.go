package cortexpb

import (
	"fmt"

	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

func init() {
	c := encoding.GetCodecV2("proto")
	encoding.RegisterCodecV2(&cortexCodec{c: c})
}

type ReleasableMessage interface {
	RegisterBuffer(mem.Buffer)
}

type cortexCodec struct {
	c encoding.CodecV2
}

func (c cortexCodec) Name() string {
	return c.c.Name()
}

func (c cortexCodec) Marshal(v any) (mem.BufferSlice, error) {
	return c.c.Marshal(v)
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

func (m *MessageWithBufRef) RegisterBuffer(buffer mem.Buffer) {
	m.bs = append(m.bs, buffer)
}

func (m *MessageWithBufRef) Free() {
	m.bs.Free()
	m.bs = m.bs[:0]
}
