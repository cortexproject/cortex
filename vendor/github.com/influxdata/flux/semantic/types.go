package semantic

import (
	"strconv"
)

// Nature is the primitive description of a type.
type Nature int

const (
	Invalid Nature = iota
	String
	Bytes
	Int
	UInt
	Float
	Bool
	Time
	Duration
	Regexp
	Array
	Object
	Function
	Dictionary
)

var natureNames = []string{
	Invalid:    "invalid",
	String:     "string",
	Bytes:      "bytes",
	Int:        "int",
	UInt:       "uint",
	Float:      "float",
	Bool:       "bool",
	Time:       "time",
	Duration:   "duration",
	Regexp:     "regexp",
	Array:      "array",
	Object:     "object",
	Function:   "function",
	Dictionary: "dictionary",
}

func (n Nature) String() string {
	if int(n) < len(natureNames) {
		return natureNames[n]
	}
	return "nature" + strconv.Itoa(int(n))
}
