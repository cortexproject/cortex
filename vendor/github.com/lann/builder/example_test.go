package builder_test

import (
	"fmt"
	"github.com/lann/builder"
)

// Simple math expression allowing multiple adds/subtracts and a single
// (optional) multiply.
type simpleExpr struct {
	Multiplier int
	Adds       []int
	Subtracts  []int
}

func (e simpleExpr) Equals() (total int) {
	for _, i := range e.Adds { total += i }
	for _, i := range e.Subtracts { total -= i }
	if e.Multiplier != 0 { total *= e.Multiplier }
	return
}

// Start builder definition

type simpleExprBuilder builder.Builder

func (b simpleExprBuilder) Multiplier(i int) simpleExprBuilder {
	return builder.Set(b, "Multiplier", i).(simpleExprBuilder)
}

func (b simpleExprBuilder) Add(i int) simpleExprBuilder {
	return builder.Append(b, "Adds", i).(simpleExprBuilder)
}

func (b simpleExprBuilder) Subtract(i int) simpleExprBuilder {
	return builder.Append(b, "Subtracts", i).(simpleExprBuilder)
}

func (b simpleExprBuilder) Equals() int {
	return builder.GetStruct(b).(simpleExpr).Equals()
}

// SimpleExprBuilder is an empty builder
var SimpleExprBuilder =
	builder.Register(simpleExprBuilder{}, simpleExpr{}).(simpleExprBuilder)

// End builder definition

func Example_basic() {
	b := SimpleExprBuilder.Add(10).Subtract(3)

	// Intermediate values can be reused
	b2 := b.Multiplier(2)
	b3 := b.Multiplier(3)

	fmt.Println(b.Equals(), b2.Equals(), b3.Equals())

	// Output:
	// 7 14 21
}
