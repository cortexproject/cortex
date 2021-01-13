package ast

import (
	"regexp"
	"time"
)

func IntegerLiteralFromValue(v int64) *IntegerLiteral {
	return &IntegerLiteral{Value: v}
}
func UnsignedIntegerLiteralFromValue(v uint64) *UnsignedIntegerLiteral {
	return &UnsignedIntegerLiteral{Value: v}
}
func FloatLiteralFromValue(v float64) *FloatLiteral {
	return &FloatLiteral{Value: v}
}
func StringLiteralFromValue(v string) *StringLiteral {
	return &StringLiteral{Value: v}
}
func BooleanLiteralFromValue(v bool) *BooleanLiteral {
	return &BooleanLiteral{Value: v}
}
func DateTimeLiteralFromValue(v time.Time) *DateTimeLiteral {
	return &DateTimeLiteral{Value: v}
}
func RegexpLiteralFromValue(v *regexp.Regexp) *RegexpLiteral {
	return &RegexpLiteral{Value: v}
}

func IntegerFromLiteral(lit *IntegerLiteral) int64 {
	return lit.Value
}
func UnsignedIntegerFromLiteral(lit *UnsignedIntegerLiteral) uint64 {
	return lit.Value
}
func FloatFromLiteral(lit *FloatLiteral) float64 {
	return lit.Value
}
func StringFromLiteral(lit *StringLiteral) string {
	return lit.Value
}
func BooleanFromLiteral(lit *BooleanLiteral) bool {
	return lit.Value
}
func DateTimeFromLiteral(lit *DateTimeLiteral) time.Time {
	return lit.Value
}
func RegexpFromLiteral(lit *RegexpLiteral) *regexp.Regexp {
	return lit.Value
}
