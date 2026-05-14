package storepb

import (
	"google.golang.org/grpc/mem"
)

// MessageWithBufRef holds a reference to gRPC unmarshal buffers for explicit lifecycle management.
// It satisfies cortexpb.ReleasableMessage via structural typing.
type MessageWithBufRef struct {
	bs mem.BufferSlice
}

func (m *MessageWithBufRef) RegisterBuffer(buffer mem.Buffer) {
	m.bs = append(m.bs, buffer)
}

// Free releases all registered buffers. Idempotent and safe on zero-value.
func (m *MessageWithBufRef) Free() {
	m.bs.Free()
	m.bs = m.bs[:0]
}

// Proto serialization no-ops (MessageWithBufRef has no wire representation).

func (m *MessageWithBufRef) Size() int                          { return 0 }
func (m *MessageWithBufRef) Marshal() ([]byte, error)           { return nil, nil }
func (m *MessageWithBufRef) MarshalTo(dAtA []byte) (int, error) { return 0, nil }
func (m *MessageWithBufRef) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	return 0, nil
}
func (m *MessageWithBufRef) Unmarshal(dAtA []byte) error      { return nil }
func (m MessageWithBufRef) Equal(that MessageWithBufRef) bool { return true }
