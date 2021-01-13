package values

import "github.com/influxdata/flux/semantic"

var (
	trueValue Value = value{
		t: semantic.BasicBool,
		v: true,
	}
	falseValue Value = value{
		t: semantic.BasicBool,
		v: false,
	}
)

func NewBool(v bool) Value {
	if v {
		return trueValue
	}
	return falseValue
}
