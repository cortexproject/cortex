package flux

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"
)

const (
	TablesParameter = "tables"
	tableKindKey    = "kind"
	tableParentsKey = "parents"
	tableSpecKey    = "spec"
)

type CreateOperationSpec func(args Arguments, a *Administration) (OperationSpec, error)

// MustValue panics if err is not nil, otherwise value is returned.
func MustValue(v values.Value, err error) values.Value {
	if err != nil {
		panic(err)
	}
	return v
}

// FunctionValue creates a values.Value from the operation spec and signature.
// Name is the name of the function as it would be called.
// c is a function reference of type CreateOperationSpec
// sig is a function signature type that specifies the names and types of each argument for the function.
func FunctionValue(name string, c CreateOperationSpec, ft semantic.MonoType) (values.Value, error) {
	return functionValue(name, c, ft, false)
}

// FunctionValueWithSideEffect creates a values.Value from the operation spec and signature.
// Name is the name of the function as it would be called.
// c is a function reference of type CreateOperationSpec
// sig is a function signature type that specifies the names and types of each argument for the function.
func FunctionValueWithSideEffect(name string, c CreateOperationSpec, ft semantic.MonoType) (values.Value, error) {
	return functionValue(name, c, ft, true)
}

func functionValue(name string, c CreateOperationSpec, mt semantic.MonoType, sideEffects bool) (values.Value, error) {
	if c == nil {
		c = func(args Arguments, a *Administration) (OperationSpec, error) {
			return nil, errors.Newf(codes.Unimplemented, "function %q is not implemented", name)
		}
	}
	if mt.Nature() != semantic.Function {
		return nil, errors.Newf(codes.Invalid, "cannot implement function %q with value of type %v", name, mt)
	}
	return &function{
		t:             mt,
		name:          name,
		createOpSpec:  c,
		hasSideEffect: sideEffects,
	}, nil
}

var _ = tableSpecKey // So that linter doesn't think tableSpecKey is unused, considering above TODO.

// IDer produces the mapping of table Objects to OperationIDs
type IDer interface {
	ID(*TableObject) OperationID
}

// IDerOpSpec is the interface any operation spec that needs
// access to OperationIDs in the query spec must implement.
type IDerOpSpec interface {
	IDer(ider IDer)
}

// TableObject represents the value returned by a transformation.
// As such, it holds the OperationSpec of the transformation it is associated with,
// and it is a values.Value (and, also, a values.Object).
// It can be compiled and executed as a flux.Program by using a lang.TableObjectCompiler.
type TableObject struct {
	// TODO(Josh): Remove args once the
	// OperationSpec interface has an Equal method.
	t       semantic.MonoType
	args    Arguments
	Kind    OperationKind
	Spec    OperationSpec
	Source  OperationSource
	Parents []*TableObject
}

func (t *TableObject) Operation(ider IDer) *Operation {
	if iderOpSpec, ok := t.Spec.(IDerOpSpec); ok {
		iderOpSpec.IDer(ider)
	}

	return &Operation{
		ID:     ider.ID(t),
		Spec:   t.Spec,
		Source: t.Source,
	}
}
func (t *TableObject) IsNull() bool {
	return false
}
func (t *TableObject) String() string {
	str := new(strings.Builder)
	t.str(str, false)
	return str.String()
}
func (t *TableObject) str(b *strings.Builder, arrow bool) {
	multiParent := len(t.Parents) > 1
	if multiParent {
		b.WriteString("( ")
	}
	for _, v := range t.Parents {
		v.str(b, !multiParent)
		if multiParent {
			b.WriteString("; ")
		}
	}
	if multiParent {
		b.WriteString(" ) -> ")
	}
	b.WriteString(string(t.Kind))
	if arrow {
		b.WriteString(" -> ")
	}
}

func (t *TableObject) Type() semantic.MonoType {
	return t.t
}

func (t *TableObject) Str() string {
	panic(values.UnexpectedKind(semantic.Array, semantic.String))
}
func (t *TableObject) Bytes() []byte {
	panic(values.UnexpectedKind(semantic.Array, semantic.Bytes))
}
func (t *TableObject) Int() int64 {
	panic(values.UnexpectedKind(semantic.Array, semantic.Int))
}
func (t *TableObject) UInt() uint64 {
	panic(values.UnexpectedKind(semantic.Array, semantic.UInt))
}
func (t *TableObject) Float() float64 {
	panic(values.UnexpectedKind(semantic.Array, semantic.Float))
}
func (t *TableObject) Bool() bool {
	panic(values.UnexpectedKind(semantic.Array, semantic.Bool))
}
func (t *TableObject) Time() values.Time {
	panic(values.UnexpectedKind(semantic.Array, semantic.Time))
}
func (t *TableObject) Duration() values.Duration {
	panic(values.UnexpectedKind(semantic.Array, semantic.Duration))
}
func (t *TableObject) Regexp() *regexp.Regexp {
	panic(values.UnexpectedKind(semantic.Array, semantic.Regexp))
}
func (t *TableObject) Array() values.Array {
	return t
}
func (t *TableObject) Object() values.Object {
	panic(values.UnexpectedKind(semantic.Array, semantic.Object))
}
func (t *TableObject) Equal(rhs values.Value) bool {
	v, ok := rhs.(*TableObject)
	return ok && t == v
}
func (t *TableObject) Function() values.Function {
	panic(values.UnexpectedKind(semantic.Array, semantic.Function))
}
func (t *TableObject) Dict() values.Dictionary {
	panic(values.UnexpectedKind(semantic.Array, semantic.Dictionary))
}

func (t *TableObject) Get(i int) values.Value {
	panic("cannot index into stream")
}
func (t *TableObject) Set(i int, v values.Value) {
	panic("cannot index into stream")
}
func (t *TableObject) Append(v values.Value) {
	panic("cannot append onto stream")
}
func (t *TableObject) Len() int {
	panic("length of stream not supported")
}
func (t *TableObject) Range(f func(i int, v values.Value)) {
	panic("cannot range over values in stream")
}
func (t *TableObject) Sort(f func(i, j values.Value) bool) {
	panic("cannot sort stream")
}

type Administration struct {
	parents []*TableObject
}

func newAdministration() *Administration {
	return &Administration{
		parents: make([]*TableObject, 0, 8),
	}
}

// AddParentFromArgs reads the args for the `table` argument and adds the value as a parent.
func (a *Administration) AddParentFromArgs(args Arguments) error {
	parent, ok := args.Get(TablesParameter)
	if !ok {
		return errors.Newf(codes.Invalid, "could not find %s parameter", TablesParameter)
	}
	p, ok := parent.(*TableObject)
	if !ok {
		return errors.Newf(codes.Invalid, "argument is not a table object: got %T", parent)
	}
	a.AddParent(p)
	return nil
}

// AddParent instructs the evaluation Context that a new edge should be created from the parent to the current operation.
// Duplicate parents will be removed, so the caller need not concern itself with which parents have already been added.
func (a *Administration) AddParent(np *TableObject) {
	// Check for duplicates
	for _, v := range a.parents {
		if v == np {
			return
		}
	}
	a.parents = append(a.parents, np)
}

type function struct {
	name          string
	t             semantic.MonoType
	createOpSpec  CreateOperationSpec
	hasSideEffect bool
}

func (f *function) Type() semantic.MonoType {
	return f.t
}
func (f *function) IsNull() bool {
	return false
}
func (f *function) Str() string {
	panic(values.UnexpectedKind(semantic.Function, semantic.String))
}
func (f *function) Bytes() []byte {
	panic(values.UnexpectedKind(semantic.Function, semantic.Bytes))
}
func (f *function) Int() int64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Int))
}
func (f *function) UInt() uint64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.UInt))
}
func (f *function) Float() float64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Float))
}
func (f *function) Bool() bool {
	panic(values.UnexpectedKind(semantic.Function, semantic.Bool))
}
func (f *function) Time() values.Time {
	panic(values.UnexpectedKind(semantic.Function, semantic.Time))
}
func (f *function) Duration() values.Duration {
	panic(values.UnexpectedKind(semantic.Function, semantic.Duration))
}
func (f *function) Regexp() *regexp.Regexp {
	panic(values.UnexpectedKind(semantic.Function, semantic.Regexp))
}
func (f *function) Array() values.Array {
	panic(values.UnexpectedKind(semantic.Function, semantic.Array))
}
func (f *function) Object() values.Object {
	panic(values.UnexpectedKind(semantic.Function, semantic.Object))
}
func (f *function) Function() values.Function {
	return f
}
func (f *function) Dict() values.Dictionary {
	panic(values.UnexpectedKind(semantic.Function, semantic.Dictionary))
}
func (f *function) Equal(rhs values.Value) bool {
	if f.Type() != rhs.Type() {
		return false
	}
	v, ok := rhs.(*function)
	return ok && (f == v)
}
func (f *function) HasSideEffect() bool {
	return f.hasSideEffect
}

func (f *function) Call(ctx context.Context, args values.Object) (values.Value, error) {
	return interpreter.DoFunctionCallContext(f.call, ctx, args)
}

func (f *function) call(ctx context.Context, args interpreter.Arguments) (values.Value, error) {
	returnType, err := f.t.ReturnType()
	if err != nil {
		return nil, err
	}

	a := newAdministration()
	arguments := Arguments{Arguments: args}
	spec, err := f.createOpSpec(arguments, a)
	if err != nil {
		return nil, err
	}

	stack := interpreter.Stack(ctx)
	t := &TableObject{
		t:    returnType,
		args: arguments,
		Kind: spec.Kind(),
		Spec: spec,
		Source: OperationSource{
			Stack: stack,
		},
		Parents: a.parents,
	}
	return t, nil
}
func (f *function) String() string {
	return fmt.Sprintf("%v", f.t)
}

type Arguments struct {
	interpreter.Arguments
}

func (a Arguments) GetTime(name string) (Time, bool, error) {
	v, ok := a.Get(name)
	if !ok {
		return Time{}, false, nil
	}
	qt, err := ToQueryTime(v)
	if err != nil {
		return Time{}, ok, err
	}
	return qt, ok, nil
}

func (a Arguments) GetRequiredTime(name string) (Time, error) {
	qt, ok, err := a.GetTime(name)
	if err != nil {
		return Time{}, err
	}
	if !ok {
		return Time{}, errors.Newf(codes.Invalid, "missing required keyword argument %q", name)
	}
	return qt, nil
}

func (a Arguments) GetDuration(name string) (Duration, bool, error) {
	v, ok := a.Get(name)
	if !ok {
		return ConvertDuration(0), false, nil
	}
	return v.Duration(), true, nil
}

func (a Arguments) GetRequiredDuration(name string) (Duration, error) {
	d, ok, err := a.GetDuration(name)
	if err != nil {
		return ConvertDuration(0), err
	}
	if !ok {
		return ConvertDuration(0), errors.Newf(codes.Invalid, "missing required keyword argument %q", name)
	}
	return d, nil
}

func ToQueryTime(value values.Value) (Time, error) {
	switch value.Type().Nature() {
	case semantic.Time:
		return Time{
			Absolute: value.Time().Time(),
		}, nil
	case semantic.Duration:
		return Time{
			Relative:   value.Duration().Duration(),
			IsRelative: true,
		}, nil
	case semantic.Int:
		return Time{
			Absolute: time.Unix(value.Int(), 0),
		}, nil
	default:
		return Time{}, errors.Newf(codes.Invalid, "value is not a time, got %v", value.Type())
	}
}
