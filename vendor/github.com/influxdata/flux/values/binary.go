package values

import (
	"math"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
	"github.com/influxdata/flux/semantic"
)

type BinaryFunction func(l, r Value) (Value, error)

type BinaryFuncSignature struct {
	Operator    ast.OperatorKind
	Left, Right semantic.Nature
}

// LookupBinaryFunction returns an appropriate binary function that evaluates two values and returns another value.
// If the two types are not compatible with the given operation, this returns an error.
func LookupBinaryFunction(sig BinaryFuncSignature) (BinaryFunction, error) {
	f, ok := binaryFuncLookup[sig]
	if !ok {
		return nil, errors.Newf(codes.Invalid, "unsupported binary expression %v %v %v", sig.Left, sig.Operator, sig.Right)
	}
	return binaryFuncNullCheck(f), nil
}

// binaryFuncNullCheck will wrap any BinaryFunction and
// check that both of the arguments are non-nil.
//
// If either value is null, then it will return null.
// Otherwise, it will invoke the function to retrieve the result.
func binaryFuncNullCheck(fn BinaryFunction) BinaryFunction {
	return func(lv, rv Value) (Value, error) {
		if lv.IsNull() || rv.IsNull() {
			return Null, nil
		}
		return fn(lv, rv)
	}
}

// binaryFuncLookup contains a mapping of BinaryFuncSignature's to
// the BinaryFunction that implements them.
//
// The values passed into these functions will be non-nil so a null
// check is unnecessary inside of them.
//
// Even though nulls will never be passed to these functions,
// the left or right type can be defined as nil. This is used to
// mark that it is valid to use the operator between those two types,
// but the function will never be invoked so it can be nil.
var binaryFuncLookup = map[BinaryFuncSignature]BinaryFunction{
	//---------------
	// Math Operators
	//---------------
	{Operator: ast.AdditionOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.Int()
		return NewInt(l + r), nil
	},
	{Operator: ast.AdditionOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.UInt()
		return NewUInt(l + r), nil
	},
	{Operator: ast.AdditionOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.Float()
		return NewFloat(l + r), nil
	},
	{Operator: ast.AdditionOperator, Left: semantic.String, Right: semantic.String}: func(lv, rv Value) (Value, error) {
		l := lv.Str()
		r := rv.Str()
		return NewString(l + r), nil
	},
	{Operator: ast.AdditionOperator, Left: semantic.Duration, Right: semantic.Duration}: func(lv, rv Value) (Value, error) {
		l := lv.Duration()
		r := rv.Duration()
		d := ConvertDurationNsecs(l.Duration() + r.Duration())
		return NewDuration(d), nil
	},
	{Operator: ast.SubtractionOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.Int()
		return NewInt(l - r), nil
	},
	{Operator: ast.SubtractionOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.UInt()
		return NewUInt(l - r), nil
	},
	{Operator: ast.SubtractionOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.Float()
		return NewFloat(l - r), nil
	},
	{Operator: ast.SubtractionOperator, Left: semantic.Duration, Right: semantic.Duration}: func(lv, rv Value) (Value, error) {
		l := lv.Duration()
		r := rv.Duration()
		d := ConvertDurationNsecs(l.Duration() - r.Duration())
		return NewDuration(d), nil
	},
	{Operator: ast.MultiplicationOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.Int()
		return NewInt(l * r), nil
	},
	{Operator: ast.MultiplicationOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.UInt()
		return NewUInt(l * r), nil
	},
	{Operator: ast.MultiplicationOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.Float()
		return NewFloat(l * r), nil
	},
	{Operator: ast.DivisionOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.Int()
		if r == 0 {
			return nil, errors.Newf(codes.FailedPrecondition, "cannot divide by zero")
		}
		return NewInt(l / r), nil
	},
	{Operator: ast.DivisionOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.UInt()
		if r == 0 {
			return nil, errors.Newf(codes.FailedPrecondition, "cannot divide by zero")
		}
		return NewUInt(l / r), nil
	},
	{Operator: ast.DivisionOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.Float()
		if r == 0 {
			return nil, errors.Newf(codes.FailedPrecondition, "cannot divide by zero")
		}
		return NewFloat(l / r), nil
	},
	{Operator: ast.ModuloOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.Int()
		if r == 0 {
			return nil, errors.Newf(codes.FailedPrecondition, "cannot mod zero")
		}
		return NewInt(l % r), nil
	},
	{Operator: ast.ModuloOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.UInt()
		if r == 0 {
			return nil, errors.Newf(codes.FailedPrecondition, "cannot mod zero")
		}
		return NewUInt(l % r), nil
	},
	{Operator: ast.ModuloOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.Float()
		if r == 0 {
			return nil, errors.Newf(codes.FailedPrecondition, "cannot mod zero")
		}
		return NewFloat(math.Mod(l, r)), nil
	},
	{Operator: ast.PowerOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.Int()
		return NewFloat(math.Pow(float64(l), float64(r))), nil
	},
	{Operator: ast.PowerOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.UInt()
		return NewFloat(math.Pow(float64(l), float64(r))), nil
	},
	{Operator: ast.PowerOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.Float()
		return NewFloat(math.Pow(float64(l), float64(r))), nil
	},
	//---------------------
	// Comparison Operators
	//---------------------

	// LessThanEqualOperator

	{Operator: ast.LessThanEqualOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.Int()
		return NewBool(l <= r), nil
	},
	{Operator: ast.LessThanEqualOperator, Left: semantic.Int, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.UInt()
		if l < 0 {
			return NewBool(true), nil
		}
		return NewBool(uint64(l) <= r), nil
	},
	{Operator: ast.LessThanEqualOperator, Left: semantic.Int, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.Float()
		return NewBool(float64(l) <= r), nil
	},
	{Operator: ast.LessThanEqualOperator, Left: semantic.UInt, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.Int()
		if r < 0 {
			return NewBool(false), nil
		}
		return NewBool(l <= uint64(r)), nil
	},
	{Operator: ast.LessThanEqualOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.UInt()
		return NewBool(l <= r), nil
	},
	{Operator: ast.LessThanEqualOperator, Left: semantic.UInt, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.Float()
		return NewBool(float64(l) <= r), nil
	},
	{Operator: ast.LessThanEqualOperator, Left: semantic.Float, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.Int()
		return NewBool(l <= float64(r)), nil
	},
	{Operator: ast.LessThanEqualOperator, Left: semantic.Float, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.UInt()
		return NewBool(l <= float64(r)), nil
	},
	{Operator: ast.LessThanEqualOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.Float()
		return NewBool(l <= r), nil
	},
	{Operator: ast.LessThanEqualOperator, Left: semantic.String, Right: semantic.String}: func(lv, rv Value) (Value, error) {
		l := lv.Str()
		r := rv.Str()
		return NewBool(l <= r), nil
	},
	{Operator: ast.LessThanEqualOperator, Left: semantic.Time, Right: semantic.Time}: func(lv, rv Value) (Value, error) {
		l := lv.Time().Time()
		r := rv.Time().Time()
		return NewBool(!l.After(r)), nil
	},

	// LessThanOperator

	{Operator: ast.LessThanOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.Int()
		return NewBool(l < r), nil
	},
	{Operator: ast.LessThanOperator, Left: semantic.Int, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.UInt()
		if l < 0 {
			return NewBool(true), nil
		}
		return NewBool(uint64(l) < r), nil
	},
	{Operator: ast.LessThanOperator, Left: semantic.Int, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.Float()
		return NewBool(float64(l) < r), nil
	},
	{Operator: ast.LessThanOperator, Left: semantic.UInt, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.Int()
		if r < 0 {
			return NewBool(false), nil
		}
		return NewBool(l < uint64(r)), nil
	},
	{Operator: ast.LessThanOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.UInt()
		return NewBool(l < r), nil
	},
	{Operator: ast.LessThanOperator, Left: semantic.UInt, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.Float()
		return NewBool(float64(l) < r), nil
	},
	{Operator: ast.LessThanOperator, Left: semantic.Float, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.Int()
		return NewBool(l < float64(r)), nil
	},
	{Operator: ast.LessThanOperator, Left: semantic.Float, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.UInt()
		return NewBool(l < float64(r)), nil
	},
	{Operator: ast.LessThanOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.Float()
		return NewBool(l < r), nil
	},
	{Operator: ast.LessThanOperator, Left: semantic.String, Right: semantic.String}: func(lv, rv Value) (Value, error) {
		l := lv.Str()
		r := rv.Str()
		return NewBool(l < r), nil
	},
	{Operator: ast.LessThanOperator, Left: semantic.Time, Right: semantic.Time}: func(lv, rv Value) (Value, error) {
		l := lv.Time().Time()
		r := rv.Time().Time()
		return NewBool(l.Before(r)), nil
	},

	// GreaterThanEqualOperator

	{Operator: ast.GreaterThanEqualOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.Int()
		return NewBool(l >= r), nil
	},
	{Operator: ast.GreaterThanEqualOperator, Left: semantic.Int, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.UInt()
		if l < 0 {
			return NewBool(true), nil
		}
		return NewBool(uint64(l) >= r), nil
	},
	{Operator: ast.GreaterThanEqualOperator, Left: semantic.Int, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.Float()
		return NewBool(float64(l) >= r), nil
	},
	{Operator: ast.GreaterThanEqualOperator, Left: semantic.UInt, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.Int()
		if r < 0 {
			return NewBool(false), nil
		}
		return NewBool(l >= uint64(r)), nil
	},
	{Operator: ast.GreaterThanEqualOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.UInt()
		return NewBool(l >= r), nil
	},
	{Operator: ast.GreaterThanEqualOperator, Left: semantic.UInt, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.Float()
		return NewBool(float64(l) >= r), nil
	},
	{Operator: ast.GreaterThanEqualOperator, Left: semantic.Float, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.Int()
		return NewBool(l >= float64(r)), nil
	},
	{Operator: ast.GreaterThanEqualOperator, Left: semantic.Float, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.UInt()
		return NewBool(l >= float64(r)), nil
	},
	{Operator: ast.GreaterThanEqualOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.Float()
		return NewBool(l >= r), nil
	},
	{Operator: ast.GreaterThanEqualOperator, Left: semantic.String, Right: semantic.String}: func(lv, rv Value) (Value, error) {
		l := lv.Str()
		r := rv.Str()
		return NewBool(l >= r), nil
	},
	{Operator: ast.GreaterThanEqualOperator, Left: semantic.Time, Right: semantic.Time}: func(lv, rv Value) (Value, error) {
		l := lv.Time().Time()
		r := rv.Time().Time()
		return NewBool(!r.After(l)), nil
	},

	// GreaterThanOperator

	{Operator: ast.GreaterThanOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.Int()
		return NewBool(l > r), nil
	},
	{Operator: ast.GreaterThanOperator, Left: semantic.Int, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.UInt()
		if l < 0 {
			return NewBool(true), nil
		}
		return NewBool(uint64(l) > r), nil
	},
	{Operator: ast.GreaterThanOperator, Left: semantic.Int, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.Float()
		return NewBool(float64(l) > r), nil
	},
	{Operator: ast.GreaterThanOperator, Left: semantic.UInt, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.Int()
		if r < 0 {
			return NewBool(false), nil
		}
		return NewBool(l > uint64(r)), nil
	},
	{Operator: ast.GreaterThanOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.UInt()
		return NewBool(l > r), nil
	},
	{Operator: ast.GreaterThanOperator, Left: semantic.UInt, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.Float()
		return NewBool(float64(l) > r), nil
	},
	{Operator: ast.GreaterThanOperator, Left: semantic.Float, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.Int()
		return NewBool(l > float64(r)), nil
	},
	{Operator: ast.GreaterThanOperator, Left: semantic.Float, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.UInt()
		return NewBool(l > float64(r)), nil
	},
	{Operator: ast.GreaterThanOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.Float()
		return NewBool(l > r), nil
	},
	{Operator: ast.GreaterThanOperator, Left: semantic.String, Right: semantic.String}: func(lv, rv Value) (Value, error) {
		l := lv.Str()
		r := rv.Str()
		return NewBool(l > r), nil
	},
	{Operator: ast.GreaterThanOperator, Left: semantic.Time, Right: semantic.Time}: func(lv, rv Value) (Value, error) {
		l := lv.Time().Time()
		r := rv.Time().Time()
		return NewBool(l.After(r)), nil
	},

	// EqualOperator

	{Operator: ast.EqualOperator, Left: semantic.Bool, Right: semantic.Bool}: func(lv, rv Value) (Value, error) {
		l := lv.Bool()
		r := rv.Bool()
		return NewBool(l == r), nil
	},
	{Operator: ast.EqualOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.Int()
		return NewBool(l == r), nil
	},
	{Operator: ast.EqualOperator, Left: semantic.Int, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.UInt()
		if l < 0 {
			return NewBool(false), nil
		}
		return NewBool(uint64(l) == r), nil
	},
	{Operator: ast.EqualOperator, Left: semantic.Int, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.Float()
		return NewBool(float64(l) == r), nil
	},
	{Operator: ast.EqualOperator, Left: semantic.UInt, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.Int()
		if r < 0 {
			return NewBool(false), nil
		}
		return NewBool(l == uint64(r)), nil
	},
	{Operator: ast.EqualOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.UInt()
		return NewBool(l == r), nil
	},
	{Operator: ast.EqualOperator, Left: semantic.UInt, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.Float()
		return NewBool(float64(l) == r), nil
	},
	{Operator: ast.EqualOperator, Left: semantic.Float, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.Int()
		return NewBool(l == float64(r)), nil
	},
	{Operator: ast.EqualOperator, Left: semantic.Float, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.UInt()
		return NewBool(l == float64(r)), nil
	},
	{Operator: ast.EqualOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.Float()
		return NewBool(l == r), nil
	},
	{Operator: ast.EqualOperator, Left: semantic.String, Right: semantic.String}: func(lv, rv Value) (Value, error) {
		l := lv.Str()
		r := rv.Str()
		return NewBool(l == r), nil
	},
	{Operator: ast.EqualOperator, Left: semantic.Time, Right: semantic.Time}: func(lv, rv Value) (Value, error) {
		l := lv.Time().Time()
		r := rv.Time().Time()
		return NewBool(l.Equal(r)), nil
	},
	{Operator: ast.EqualOperator, Left: semantic.Array, Right: semantic.Array}: func(lv, rv Value) (Value, error) {
		return NewBool(lv.Equal(rv)), nil
	},
	{Operator: ast.EqualOperator, Left: semantic.Object, Right: semantic.Object}: func(lv, rv Value) (Value, error) {
		return NewBool(lv.Equal(rv)), nil
	},
	{Operator: ast.EqualOperator, Left: semantic.Bytes, Right: semantic.Bytes}: func(lv, rv Value) (Value, error) {
		return NewBool(lv.Equal(rv)), nil
	},

	// NotEqualOperator

	{Operator: ast.NotEqualOperator, Left: semantic.Bool, Right: semantic.Bool}: func(lv, rv Value) (Value, error) {
		l := lv.Bool()
		r := rv.Bool()
		return NewBool(l != r), nil
	},
	{Operator: ast.NotEqualOperator, Left: semantic.Int, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.Int()
		return NewBool(l != r), nil
	},
	{Operator: ast.NotEqualOperator, Left: semantic.Int, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.UInt()
		if l < 0 {
			return NewBool(true), nil
		}
		return NewBool(uint64(l) != r), nil
	},
	{Operator: ast.NotEqualOperator, Left: semantic.Int, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.Int()
		r := rv.Float()
		return NewBool(float64(l) != r), nil
	},
	{Operator: ast.NotEqualOperator, Left: semantic.UInt, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.Int()
		if r < 0 {
			return NewBool(true), nil
		}
		return NewBool(l != uint64(r)), nil
	},
	{Operator: ast.NotEqualOperator, Left: semantic.UInt, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.UInt()
		return NewBool(l != r), nil
	},
	{Operator: ast.NotEqualOperator, Left: semantic.UInt, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.UInt()
		r := rv.Float()
		return NewBool(float64(l) != r), nil
	},
	{Operator: ast.NotEqualOperator, Left: semantic.Float, Right: semantic.Int}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.Int()
		return NewBool(l != float64(r)), nil
	},
	{Operator: ast.NotEqualOperator, Left: semantic.Float, Right: semantic.UInt}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.UInt()
		return NewBool(l != float64(r)), nil
	},
	{Operator: ast.NotEqualOperator, Left: semantic.Float, Right: semantic.Float}: func(lv, rv Value) (Value, error) {
		l := lv.Float()
		r := rv.Float()
		return NewBool(l != r), nil
	},
	{Operator: ast.NotEqualOperator, Left: semantic.String, Right: semantic.String}: func(lv, rv Value) (Value, error) {
		l := lv.Str()
		r := rv.Str()
		return NewBool(l != r), nil
	},
	{Operator: ast.NotEqualOperator, Left: semantic.Time, Right: semantic.Time}: func(lv, rv Value) (Value, error) {
		l := lv.Time().Time()
		r := rv.Time().Time()
		return NewBool(!l.Equal(r)), nil
	},

	{Operator: ast.RegexpMatchOperator, Left: semantic.String, Right: semantic.Regexp}: func(lv, rv Value) (Value, error) {
		l := lv.Str()
		r := rv.Regexp()
		return NewBool(r.MatchString(l)), nil
	},
	{Operator: ast.NotRegexpMatchOperator, Left: semantic.String, Right: semantic.Regexp}: func(lv, rv Value) (Value, error) {
		l := lv.Str()
		r := rv.Regexp()
		return NewBool(!r.MatchString(l)), nil
	},
}
