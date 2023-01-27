package codec

import (
	"github.com/gogo/protobuf/proto"
)

type MultiKey interface {
	Clonable

	// SplitByID Split interface in array of key and value. THe key is a unique identifier of an instance in the ring. The value is
	// interface with its data. The interface resultant need to be a proto.Message
	SplitByID() map[string]interface{}

	// JoinIds update the current interface to add receiving key value information. The key is an unique identifier for an instance.
	// The value is the information for that instance.
	JoinIds(in map[string]interface{})

	// GetItemFactory method to be used for deserilaize the value information from an instance
	GetItemFactory() proto.Message

	// FindDifference returns the difference between two Multikeys. The returns are an interface which also implements Multikey
	// with an array of keys which were changed, and an array of strings which are unique identifiers deleted. An error is
	// returned when that does not implement the correct codec
	FindDifference(that MultiKey) (interface{}, []string, error)
}
