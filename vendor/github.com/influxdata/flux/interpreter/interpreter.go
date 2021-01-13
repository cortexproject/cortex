package interpreter

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"
)

const (
	PackageMain = "main"
	NowPkg      = "universe"
	NowOption   = "now"
)

// This interface is used by the interpreter to set options that are relevant
// to the execution engine. For most cases it would be sufficient to pull
// options out after the interpreter is run, however it is possible for the
// interpreter to invoke the execution engine via tableFind and chain
// functions. These options need to get immediately installed in the execution
// dependencies when they are interpreted. We cannot access them directly here
// due to circular dependencies, so we use an interface, with an implementation
// defined by the caller.
type ExecOptsConfig interface {
	ConfigureProfiler(ctx context.Context, profilerNames []string)
	ConfigureNow(ctx context.Context, now time.Time)
}

// A default execution options implementation that discards the settings.
type defExecOptsConfig struct{}

func (es *defExecOptsConfig) ConfigureProfiler(ctx context.Context, profilerNames []string) {}
func (es *defExecOptsConfig) ConfigureNow(ctx context.Context, now time.Time)               {}

type Interpreter struct {
	sideEffects    []SideEffect // a list of the side effects occurred during the last call to `Eval`.
	pkgName        string
	execOptsConfig ExecOptsConfig
}

func NewInterpreter(pkg *Package, eoc ExecOptsConfig) *Interpreter {
	var pkgName string
	if pkg != nil {
		pkgName = pkg.Name()
	}
	if eoc == nil {
		eoc = &defExecOptsConfig{}
	}
	return &Interpreter{
		pkgName:        pkgName,
		execOptsConfig: eoc,
	}
}

func (itrp *Interpreter) PackageName() string {
	return itrp.pkgName
}

// SideEffect contains its value, and the semantic node that generated it.
type SideEffect struct {
	Node  semantic.Node
	Value values.Value
}

// Eval evaluates the expressions composing a Flux package and returns any side effects that occurred during this evaluation.
func (itrp *Interpreter) Eval(ctx context.Context, node semantic.Node, scope values.Scope, importer Importer) ([]SideEffect, error) {
	itrp.sideEffects = itrp.sideEffects[:0]
	if err := itrp.doRoot(ctx, node, scope, importer); err != nil {
		return nil, err
	}
	return itrp.sideEffects, nil
}

func (itrp *Interpreter) doRoot(ctx context.Context, node semantic.Node, scope values.Scope, importer Importer) error {
	switch n := node.(type) {
	case *semantic.Package:
		return itrp.doPackage(ctx, n, scope, importer)
	case *semantic.File:
		return itrp.doFile(ctx, n, scope, importer)
	default:
		return errors.Newf(codes.Internal, "unsupported root node %T", node)
	}
}

func (itrp *Interpreter) doPackage(ctx context.Context, pkg *semantic.Package, scope values.Scope, importer Importer) error {
	for _, file := range pkg.Files {
		if err := itrp.doFile(ctx, file, scope, importer); err != nil {
			return err
		}
	}
	return nil
}

func (itrp *Interpreter) doFile(ctx context.Context, file *semantic.File, scope values.Scope, importer Importer) error {
	if err := itrp.doPackageClause(file.Package); err != nil {
		return err
	}
	for _, i := range file.Imports {
		if err := itrp.doImport(i, scope, importer); err != nil {
			return err
		}
	}
	for _, stmt := range file.Body {
		val, err := itrp.doStatement(ctx, stmt, scope)
		if err != nil {
			return err
		}
		if es, ok := stmt.(*semantic.ExpressionStatement); ok {
			// Only in the main package are all unassigned package
			// level expressions coerced into producing side effects.
			if itrp.pkgName == PackageMain {
				itrp.sideEffects = append(itrp.sideEffects, SideEffect{Node: es, Value: val})
			}
		}
	}
	return nil
}

func (itrp *Interpreter) doPackageClause(pkg *semantic.PackageClause) error {
	name := PackageMain
	if pkg != nil {
		name = pkg.Name.Name
	}
	if itrp.pkgName == "" {
		itrp.pkgName = name
	} else if itrp.pkgName != name {
		return errors.Newf(codes.Invalid, "package name mismatch %q != %q", itrp.pkgName, name)
	}
	return nil
}

func (itrp *Interpreter) doImport(dec *semantic.ImportDeclaration, scope values.Scope, importer Importer) error {
	path := dec.Path.Value
	pkg, err := importer.ImportPackageObject(path)
	if err != nil {
		return err
	}
	name := pkg.Name()
	if dec.As != nil {
		name = dec.As.Name
	}
	scope.Set(name, pkg)
	// Packages can import side effects
	itrp.sideEffects = append(itrp.sideEffects, pkg.SideEffects()...)
	return nil
}

// doStatement returns the resolved value of a top-level statement
func (itrp *Interpreter) doStatement(ctx context.Context, stmt semantic.Statement, scope values.Scope) (values.Value, error) {
	scope.SetReturn(values.InvalidValue)
	switch s := stmt.(type) {
	case *semantic.OptionStatement:
		return itrp.doOptionStatement(ctx, s, scope)
	case *semantic.BuiltinStatement:
		// Nothing to do
		return nil, nil
	case *semantic.TestStatement:
		return itrp.doTestStatement(ctx, s, scope)
	case *semantic.NativeVariableAssignment:
		return itrp.doVariableAssignment(ctx, s, scope)
	case *semantic.MemberAssignment:
		return itrp.doMemberAssignment(ctx, s, scope)
	case *semantic.ExpressionStatement:
		v, err := itrp.doExpression(ctx, s.Expression, scope)
		if err != nil {
			return nil, err
		}
		scope.SetReturn(v)
		return v, nil
	case *semantic.ReturnStatement:
		v, err := itrp.doExpression(ctx, s.Argument, scope)
		if err != nil {
			return nil, err
		}
		scope.SetReturn(v)
	default:
		return nil, errors.Newf(codes.Internal, "unsupported statement type %T", stmt)
	}
	return nil, nil
}

// If the option is "now", evaluate the function and store in the execution
// dependencies.
func (irtp *Interpreter) evaluateNowOption(ctx context.Context, name string, init values.Value) {
	if name != NowOption {
		return
	}

	// Evaluate now.
	nowTime, err := init.Function().Call(ctx, nil)
	if err != nil {
		return
	}
	now := nowTime.Time().Time()

	irtp.execOptsConfig.ConfigureNow(ctx, now)
}

func convert(rules values.Array) ([]string, error) {
	noRules := rules.Len()
	rs := make([]string, noRules)
	rules.Range(func(i int, v values.Value) {
		rs[i] = v.Str()
	})
	return rs, nil
}

func (irtp *Interpreter) evaluateProfilerOption(ctx context.Context, pkg values.Package, name string, init values.Value) {
	if pkg.Name() == "profiler" && name == "enabledProfilers" {

		arr := init.Array()

		profilerNames, err := convert(arr)
		if err != nil {
			return
		}

		irtp.execOptsConfig.ConfigureProfiler(ctx, profilerNames)
	}
}

func (itrp *Interpreter) doOptionStatement(ctx context.Context, s *semantic.OptionStatement, scope values.Scope) (values.Value, error) {
	switch a := s.Assignment.(type) {
	case *semantic.NativeVariableAssignment:
		init, err := itrp.doExpression(ctx, a.Init, scope)
		if err != nil {
			return nil, err
		}

		// Some functions require access to now from the execution dependencies
		// (eg tableFind). For those cases we immediately evaluate and store it
		// in the execution deps.
		itrp.evaluateNowOption(ctx, a.Identifier.Name, init)

		// Retrieve an option with the name from the scope.
		// If it exists and is an option, then set the option
		// as it is from the prelude.
		if opt, ok := scope.Lookup(a.Identifier.Name); ok {
			if opt, ok := opt.(*values.Option); ok {
				opt.Value = init
				return opt, nil
			}
		}

		// Create a new option and set it within the current scope.
		v := &values.Option{Value: init}
		scope.Set(a.Identifier.Name, v)
		return v, nil
	case *semantic.MemberAssignment:
		init, err := itrp.doExpression(ctx, a.Init, scope)
		if err != nil {
			return nil, err
		}

		obj, err := itrp.doExpression(ctx, a.Member.Object, scope)
		if err != nil {
			return nil, err
		}

		pkg, ok := obj.(values.Package)
		if !ok {
			return nil, errors.Newf(codes.Invalid, "%s: cannot set option %q on non-package value", a.Location(), a.Member.Property)
		}

		v, _ := values.SetOption(pkg, a.Member.Property, init)

		itrp.evaluateProfilerOption(ctx, pkg, a.Member.Property, init)

		return v, nil
	default:
		return nil, errors.Newf(codes.Internal, "unsupported assignment %T", a)
	}
}

func (itrp *Interpreter) doTestStatement(ctx context.Context, s *semantic.TestStatement, scope values.Scope) (values.Value, error) {
	return itrp.doAssignment(ctx, s.Assignment, scope)
}

func (itrp *Interpreter) doVariableAssignment(ctx context.Context, dec *semantic.NativeVariableAssignment, scope values.Scope) (values.Value, error) {
	value, err := itrp.doExpression(ctx, dec.Init, scope)
	if err != nil {
		return nil, err
	}
	scope.Set(dec.Identifier.Name, value)
	return value, nil
}

func (itrp *Interpreter) doMemberAssignment(ctx context.Context, a *semantic.MemberAssignment, scope values.Scope) (values.Value, error) {
	object, err := itrp.doExpression(ctx, a.Member.Object, scope)
	if err != nil {
		return nil, err
	}
	init, err := itrp.doExpression(ctx, a.Init, scope)
	if err != nil {
		return nil, err
	}
	object.Object().Set(a.Member.Property, init)
	return object, nil
}

func (itrp *Interpreter) doAssignment(ctx context.Context, a semantic.Assignment, scope values.Scope) (values.Value, error) {
	switch a := a.(type) {
	case *semantic.NativeVariableAssignment:
		return itrp.doVariableAssignment(ctx, a, scope)
	case *semantic.MemberAssignment:
		return itrp.doMemberAssignment(ctx, a, scope)
	default:
		return nil, errors.Newf(codes.Internal, "unsupported assignment %T", a)
	}
}

func (itrp *Interpreter) doExpression(ctx context.Context, expr semantic.Expression, scope values.Scope) (ret values.Value, err error) {
	switch e := expr.(type) {
	case semantic.Literal:
		return itrp.doLiteral(e)
	case *semantic.StringExpression:
		return itrp.doStringExpression(ctx, e, scope)
	case *semantic.ArrayExpression:
		return itrp.doArray(ctx, e, scope)
	case *semantic.DictExpression:
		return itrp.doDict(ctx, e, scope)
	case *semantic.IdentifierExpression:
		value, ok := scope.Lookup(e.Name)
		if !ok {
			return nil, errors.Newf(codes.Invalid, "undefined identifier %q", e.Name)
		}
		return value, nil
	case *semantic.CallExpression:
		return itrp.doCall(ctx, e, scope)
	case *semantic.MemberExpression:
		obj, err := itrp.doExpression(ctx, e.Object, scope)
		if err != nil {
			return nil, err
		}
		if typ := obj.Type().Nature(); typ != semantic.Object {
			return nil, errors.Newf(codes.Invalid, "cannot access property %q on value of type %s", e.Property, typ)
		}
		v, _ := obj.Object().Get(e.Property)
		if pkg, ok := v.(*Package); ok {
			// If the property of a member expression represents a package, then the object itself must be a package.
			return nil, errors.Newf(codes.Invalid, "cannot access imported package %q of imported package %q", pkg.Name(), obj.(*Package).Name())
		}
		return v, nil
	case *semantic.IndexExpression:
		arr, err := itrp.doExpression(ctx, e.Array, scope)
		if err != nil {
			return nil, err
		}
		idx, err := itrp.doExpression(ctx, e.Index, scope)
		if err != nil {
			return nil, err
		}
		ix := int(idx.Int())
		l := arr.Array().Len()
		if ix < 0 || ix >= l {
			return nil, errors.Newf(codes.Invalid, "cannot access element %v of array of length %v", ix, l)
		}
		return arr.Array().Get(ix), nil
	case *semantic.ObjectExpression:
		return itrp.doObject(ctx, e, scope)
	case *semantic.UnaryExpression:
		v, err := itrp.doExpression(ctx, e.Argument, scope)
		if err != nil {
			return nil, err
		}
		switch e.Operator {
		case ast.NotOperator:
			if v.Type().Nature() != semantic.Bool {
				return nil, errors.Newf(codes.Invalid, "operand to unary expression is not a boolean value, got %v", v.Type())
			}
			return values.NewBool(!v.Bool()), nil
		case ast.SubtractionOperator:
			switch t := v.Type().Nature(); t {
			case semantic.Int:
				return values.NewInt(-v.Int()), nil
			case semantic.Float:
				return values.NewFloat(-v.Float()), nil
			case semantic.Duration:
				return values.NewDuration(v.Duration().Mul(-1)), nil
			default:
				return nil, errors.Newf(codes.Invalid, "operand to unary expression is not a number value, got %v", v.Type())
			}
		case ast.ExistsOperator:
			return values.NewBool(!v.IsNull()), nil
		default:
			return nil, errors.Newf(codes.Invalid, "unsupported operator %q to unary expression", e.Operator)
		}
	case *semantic.BinaryExpression:
		l, err := itrp.doExpression(ctx, e.Left, scope)
		if err != nil {
			return nil, err
		}

		r, err := itrp.doExpression(ctx, e.Right, scope)
		if err != nil {
			return nil, err
		}

		bf, err := values.LookupBinaryFunction(values.BinaryFuncSignature{
			Operator: e.Operator,
			Left:     l.Type().Nature(),
			Right:    r.Type().Nature(),
		})
		if err != nil {
			return nil, err
		}
		return bf(l, r)
	case *semantic.LogicalExpression:
		l, err := itrp.doExpression(ctx, e.Left, scope)
		if err != nil {
			return nil, err
		}
		if l.Type().Nature() != semantic.Bool {
			return nil, errors.Newf(codes.Invalid, "left operand to logcial expression is not a boolean value, got %v", l.Type())
		}
		left := l.Bool()

		if e.Operator == ast.AndOperator && !left {
			// Early return
			return values.NewBool(false), nil
		} else if e.Operator == ast.OrOperator && left {
			// Early return
			return values.NewBool(true), nil
		}

		r, err := itrp.doExpression(ctx, e.Right, scope)
		if err != nil {
			return nil, err
		}
		if r.Type().Nature() != semantic.Bool {
			return nil, errors.New(codes.Invalid, "right operand to logical expression is not a boolean value")
		}
		right := r.Bool()

		switch e.Operator {
		case ast.AndOperator:
			return values.NewBool(left && right), nil
		case ast.OrOperator:
			return values.NewBool(left || right), nil
		default:
			return nil, errors.Newf(codes.Invalid, "invalid logical operator %v", e.Operator)
		}
	case *semantic.ConditionalExpression:
		t, err := itrp.doExpression(ctx, e.Test, scope)
		if err != nil {
			return nil, err
		}
		if t.Type().Nature() != semantic.Bool {
			return nil, errors.New(codes.Invalid, "conditional test expression is not a boolean value")
		}
		if t.Bool() {
			return itrp.doExpression(ctx, e.Consequent, scope)
		}
		return itrp.doExpression(ctx, e.Alternate, scope)
	case *semantic.FunctionExpression:
		// In the case of builtin functions this function value is shared across all query requests
		// and as such must NOT be a pointer value.
		return function{
			e:     e,
			scope: scope,
			itrp:  itrp,
		}, nil
	default:
		return nil, errors.Newf(codes.Internal, "unsupported expression %T", expr)
	}
}

func (itrp *Interpreter) doStringExpression(ctx context.Context, s *semantic.StringExpression, scope values.Scope) (values.Value, error) {
	var b strings.Builder
	for _, p := range s.Parts {
		part, err := itrp.doStringPart(ctx, p, scope)
		if err != nil {
			return nil, err
		}
		b.WriteString(part.Str())
	}
	return values.NewString(b.String()), nil
}

func (itrp *Interpreter) doStringPart(ctx context.Context, part semantic.StringExpressionPart, scope values.Scope) (values.Value, error) {
	switch p := part.(type) {
	case *semantic.TextPart:
		return values.NewString(p.Value), nil
	case *semantic.InterpolatedPart:
		v, err := itrp.doExpression(ctx, p.Expression, scope)
		if err != nil {
			return nil, err
		} else if v.IsNull() {
			return nil, errors.Newf(codes.Invalid, "%s: interpolated expression produced a null value",
				p.Location())
		} else if v.Type().Nature() != semantic.String {
			return nil, errors.Newf(codes.Invalid, "%s: expected interpolated expression to have type %s, but it had type %s",
				p.Location(), semantic.String, v.Type().Nature())
		}
		return v, nil
	}
	return nil, errors.New(codes.Internal, "expecting interpolated string part")
}

func (itrp *Interpreter) doArray(ctx context.Context, a *semantic.ArrayExpression, scope values.Scope) (values.Value, error) {
	elements := make([]values.Value, len(a.Elements))

	for i, el := range a.Elements {
		v, err := itrp.doExpression(ctx, el, scope)
		if err != nil {
			return nil, err
		}
		elements[i] = v
	}
	return values.NewArrayWithBacking(a.TypeOf(), elements), nil
}

func (itrp *Interpreter) doDict(ctx context.Context, e *semantic.DictExpression, scope values.Scope) (values.Value, error) {
	if len(e.Elements) == 0 {
		return values.NewEmptyDict(e.TypeOf()), nil
	}
	builder := values.NewDictBuilder(e.TypeOf())
	for _, pair := range e.Elements {
		key, err := itrp.doExpression(ctx, pair.Key, scope)
		if err != nil {
			return nil, err
		}
		val, err := itrp.doExpression(ctx, pair.Val, scope)
		if err != nil {
			return nil, err
		}
		if err := builder.Insert(key, val); err != nil {
			return nil, err
		}
	}
	return builder.Dict(), nil
}

func (itrp *Interpreter) doObject(ctx context.Context, m *semantic.ObjectExpression, scope values.Scope) (values.Value, error) {
	if label, nok := itrp.checkForDuplicates(m.Properties); nok {
		return nil, errors.Newf(codes.Invalid, "duplicate key in object: %q", label)
	}

	return values.BuildObject(func(set values.ObjectSetter) error {
		// Evaluate the expression from the with statement and add
		// each of the key/value pairs to the object in the order
		// they are encountered.
		if m.With != nil {
			with, err := itrp.doExpression(ctx, m.With, scope)
			if err != nil {
				return err
			}
			with.Object().Range(func(name string, v values.Value) {
				set(name, v)
			})
		}

		// Evaluate each of the properties overwriting the value
		// from the with if it is present. New properties are appended
		// to the end of the list.
		for _, p := range m.Properties {
			v, err := itrp.doExpression(ctx, p.Value, scope)
			if err != nil {
				return err
			}
			set(p.Key.Key(), v)
		}
		return nil
	})
}

func (itrp *Interpreter) checkForDuplicates(properties []*semantic.Property) (string, bool) {
	for i := 1; i < len(properties); i++ {
		label := properties[i].Key.Key()
		// Check all of the previous keys to see if any of them
		// match the existing key. This avoids allocating a new
		// structure to check for duplicates.
		for _, p := range properties[:i] {
			if p.Key.Key() == label {
				return label, true
			}
		}
	}
	return "", false
}

func (itrp *Interpreter) doLiteral(lit semantic.Literal) (values.Value, error) {
	switch l := lit.(type) {
	case *semantic.DateTimeLiteral:
		return values.NewTime(values.Time(l.Value.UnixNano())), nil
	case *semantic.DurationLiteral:
		dur, err := values.FromDurationValues(l.Values)
		if err != nil {
			return nil, err
		}
		return values.NewDuration(dur), nil
	case *semantic.FloatLiteral:
		return values.NewFloat(l.Value), nil
	case *semantic.IntegerLiteral:
		return values.NewInt(l.Value), nil
	case *semantic.UnsignedIntegerLiteral:
		return values.NewUInt(l.Value), nil
	case *semantic.StringLiteral:
		return values.NewString(l.Value), nil
	case *semantic.RegexpLiteral:
		return values.NewRegexp(l.Value), nil
	case *semantic.BooleanLiteral:
		return values.NewBool(l.Value), nil
	default:
		return nil, errors.Newf(codes.Internal, "unknown literal type %T", lit)
	}
}

func functionName(call *semantic.CallExpression) string {
	switch callee := call.Callee.(type) {
	case *semantic.IdentifierExpression:
		return callee.Name
	case *semantic.MemberExpression:
		return callee.Property
	default:
		return "<anonymous function>"
	}
}

// DoFunctionCall will call DoFunctionCallContext with a background context.
func DoFunctionCall(f func(args Arguments) (values.Value, error), argsObj values.Object) (values.Value, error) {
	return DoFunctionCallContext(func(_ context.Context, args Arguments) (values.Value, error) {
		return f(args)
	}, context.Background(), argsObj)
}

// DoFunctionCallContext will treat the argsObj as the arguments to a function.
// It will then invoke that function with the Arguments and return the
// value from the function.
//
// This function verifies that all of the arguments have been consumed
// by the function call.
func DoFunctionCallContext(f func(ctx context.Context, args Arguments) (values.Value, error), ctx context.Context, argsObj values.Object) (values.Value, error) {
	args := NewArguments(argsObj)
	v, err := f(ctx, args)
	if err != nil {
		return nil, err
	}
	if unused := args.listUnused(); len(unused) > 0 {
		return nil, errors.Newf(codes.Invalid, "unused arguments %v", unused)
	}
	return v, nil
}

func (itrp *Interpreter) doCall(ctx context.Context, call *semantic.CallExpression, scope values.Scope) (values.Value, error) {
	callee, err := itrp.doExpression(ctx, call.Callee, scope)
	if err != nil {
		return nil, err
	}
	ft := callee.Type()
	if ft.Nature() != semantic.Function {
		return nil, errors.Newf(codes.Invalid, "cannot call function: %s: value is of type %v", call.Callee.Location(), callee.Type())
	}
	argObj, err := itrp.doArguments(ctx, call.Arguments, scope, ft, call.Pipe)
	if err != nil {
		return nil, err
	}

	f := callee.Function()

	// Check if the function is an interpFunction and rebind it.
	// This is needed so that any side effects produced when
	// calling this function are bound to the correct interpreter.
	if af, ok := f.(function); ok {
		af.itrp = itrp
		f = af
	}

	// Call the function. We attach source location information
	// to this call so it can be available for the function if needed.
	// We do not attach this source location information when evaluating
	// arguments as this source location information is only
	// for the currently called function.
	fname := functionName(call)
	ctx = withStackEntry(ctx, fname, call.Location())
	value, err := f.Call(ctx, argObj)
	if err != nil {
		return nil, errors.Wrapf(err, codes.Inherit, "error calling function %q @%s", fname, call.Location())
	}

	if f.HasSideEffect() {
		itrp.sideEffects = append(itrp.sideEffects, SideEffect{Node: call, Value: value})
	}

	return value, nil
}

func (itrp *Interpreter) doArguments(ctx context.Context, args *semantic.ObjectExpression, scope values.Scope, funcType semantic.MonoType, pipe semantic.Expression) (values.Object, error) {
	if label, nok := itrp.checkForDuplicates(args.Properties); nok {
		return nil, errors.Newf(codes.Invalid, "duplicate keyword parameter specified: %q", label)
	}

	if pipe == nil && (args == nil || len(args.Properties) == 0) {
		typ := semantic.NewObjectType(nil)
		return values.NewObject(typ), nil
	}

	// Determine which argument matches the pipe argument.
	var pipeArgument string
	if pipe != nil {
		n, err := funcType.NumArguments()
		if err != nil {
			return nil, err
		}

		for i := 0; i < n; i++ {
			arg, err := funcType.Argument(i)
			if err != nil {
				return nil, err
			}
			if arg.Pipe() {
				pipeArgument = string(arg.Name())
				break
			}
		}

		if pipeArgument == "" {
			return nil, errors.New(codes.Invalid, "pipe parameter value provided to function with no pipe parameter defined")
		}
	}

	return values.BuildObject(func(set values.ObjectSetter) error {
		for _, p := range args.Properties {
			if pipe != nil && p.Key.Key() == pipeArgument {
				return errors.Newf(codes.Invalid, "pipe argument also specified as a keyword parameter: %q", p.Key.Key())
			}
			value, err := itrp.doExpression(ctx, p.Value, scope)
			if err != nil {
				return err
			}
			set(p.Key.Key(), value)
		}

		if pipe != nil {
			value, err := itrp.doExpression(ctx, pipe, scope)
			if err != nil {
				return err
			}
			set(pipeArgument, value)
		}
		return nil
	})
}

// Value represents any value that can be the result of evaluating any expression.
type Value interface {
	// Type reports the type of value
	Type() semantic.MonoType
	// Value returns the actual value represented.
	Value() interface{}
	// Property returns a new value which is a property of this value.
	Property(name string) (values.Value, error)
}

// function represents an interpretable function definition.
// Values of this type are shared across multiple interpreter runs as such
// this type implements the values.Function interface using a non-pointer receiver.
type function struct {
	e     *semantic.FunctionExpression
	scope values.Scope

	itrp *Interpreter
}

func (f function) Type() semantic.MonoType {
	return f.e.TypeOf()
}

func (f function) IsNull() bool {
	return false
}
func (f function) Str() string {
	panic(values.UnexpectedKind(semantic.Function, semantic.String))
}
func (f function) Bytes() []byte {
	panic(values.UnexpectedKind(semantic.Function, semantic.Bytes))
}
func (f function) Int() int64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Int))
}
func (f function) UInt() uint64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.UInt))
}
func (f function) Float() float64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Float))
}
func (f function) Bool() bool {
	panic(values.UnexpectedKind(semantic.Function, semantic.Bool))
}
func (f function) Time() values.Time {
	panic(values.UnexpectedKind(semantic.Function, semantic.Time))
}
func (f function) Duration() values.Duration {
	panic(values.UnexpectedKind(semantic.Function, semantic.Duration))
}
func (f function) Regexp() *regexp.Regexp {
	panic(values.UnexpectedKind(semantic.Function, semantic.Regexp))
}
func (f function) Array() values.Array {
	panic(values.UnexpectedKind(semantic.Function, semantic.Array))
}
func (f function) Object() values.Object {
	panic(values.UnexpectedKind(semantic.Function, semantic.Object))
}
func (f function) Function() values.Function {
	return f
}
func (f function) Dict() values.Dictionary {
	panic(values.UnexpectedKind(semantic.Function, semantic.Dictionary))
}
func (f function) Equal(rhs values.Value) bool {
	if f.Type() != rhs.Type() {
		return false
	}
	v, ok := rhs.(function)
	return ok && f.e == v.e && f.scope == v.scope
}
func (f function) HasSideEffect() bool {
	// Function definitions do not produce side effects.
	// Only a function call expression can produce side effects.
	return false
}

func (f function) Call(ctx context.Context, args values.Object) (values.Value, error) {
	argsNew := newArguments(args)
	v, err := f.doCall(ctx, argsNew)
	if err != nil {
		return nil, err
	}
	if unused := argsNew.listUnused(); len(unused) > 0 {
		return nil, errors.Newf(codes.Invalid, "unused arguments %s", unused)
	}
	return v, nil
}
func (f function) doCall(ctx context.Context, args Arguments) (values.Value, error) {
	if f.itrp == nil {
		// Create an new interpreter
		f.itrp = &Interpreter{}
	}

	blockScope := f.scope.Nest(nil)
	if f.e.Parameters != nil {
	PARAMETERS:
		for _, p := range f.e.Parameters.List {
			if f.e.Defaults != nil {
				for _, d := range f.e.Defaults.Properties {
					if d.Key.Key() == p.Key.Name {
						v, ok := args.Get(p.Key.Name)
						if !ok {
							// Use default value
							var err error
							// evaluate default expressions outside the block scope
							v, err = f.itrp.doExpression(ctx, d.Value, f.scope)
							if err != nil {
								return nil, err
							}
						}
						blockScope.Set(p.Key.Name, v)
						continue PARAMETERS
					}
				}
			}
			v, err := args.GetRequired(p.Key.Name)
			if err != nil {
				return nil, err
			}
			blockScope.Set(p.Key.Name, v)
		}
	}

	// Validate the function block.
	if !isValidFunctionBlock(f.e.Block) {
		return nil, errors.New(codes.Invalid, "return statement is not the last statement in the block")
	}

	nested := blockScope.Nest(nil)
	for _, stmt := range f.e.Block.Body {
		if _, err := f.itrp.doStatement(ctx, stmt, nested); err != nil {
			return nil, err
		}
	}
	return nested.Return(), nil
}

// isValidFunctionBlock returns true if the function block has at least one
// statement and the last statement is a return statement.
func isValidFunctionBlock(fn *semantic.Block) bool {
	// Must have at least one statement.
	if len(fn.Body) == 0 {
		return false
	}

	// Validate a return statement is the last statement.
	_, ok := fn.Body[len(fn.Body)-1].(*semantic.ReturnStatement)
	return ok
}

func (f function) String() string {
	return fmt.Sprintf("%v", f.Type())
}

// Resolver represents a value that can resolve itself.
// Resolving is the action of capturing the scope at function declaration and
// replacing any identifiers with static values from the scope where possible.
// TODO(nathanielc): Improve implementations of scope to only preserve values
// in the scope that are referrenced.
type Resolver interface {
	Resolve() (semantic.Node, error)
	Scope() values.Scope
}

// ResolveFunction produces a function that can execute externally.
func ResolveFunction(f values.Function) (ResolvedFunction, error) {
	resolver, ok := f.(Resolver)
	if !ok {
		return ResolvedFunction{}, errors.Newf(codes.Internal, "function is not resolvable")
	}
	resolved, err := resolver.Resolve()
	if err != nil {
		return ResolvedFunction{}, err
	}
	fn, ok := resolved.(*semantic.FunctionExpression)
	if !ok {
		return ResolvedFunction{}, errors.New(codes.Internal, "resolved function is not a function")
	}
	return ResolvedFunction{
		Fn:    fn,
		Scope: resolver.Scope(),
	}, nil
}

// ResolvedFunction represents a function that can be passed down to the compiler.
// Both the function expression and scope are captured.
// The scope cannot be serialized, which is no longer a problem in the current design
// with the exception of the REPL which will not be able to correctly pass through the scope.
type ResolvedFunction struct {
	Fn    *semantic.FunctionExpression `json:"fn"`
	Scope values.Scope                 `json:"-"`
}

func (r ResolvedFunction) Copy() ResolvedFunction {
	var nr ResolvedFunction
	if r.Fn != nil {
		nr.Fn = r.Fn.Copy().(*semantic.FunctionExpression)
	}
	if r.Scope != nil {
		nr.Scope = r.Scope.Copy()
	}
	return nr
}

func (f function) Scope() values.Scope {
	return f.scope
}

// Resolve rewrites the function resolving any identifiers not listed in the function params.
func (f function) Resolve() (semantic.Node, error) {
	n := f.e.Copy()
	localIdentifiers := make([]string, 0, 10)
	node, err := ResolveIdsInFunction(f.scope, f.e, n, &localIdentifiers)
	if err != nil {
		return nil, err
	}
	return node, nil
}

func ResolveIdsInFunction(scope values.Scope, origFn *semantic.FunctionExpression, n semantic.Node, localIdentifiers *[]string) (semantic.Node, error) {
	switch n := n.(type) {
	case *semantic.MemberExpression:
		node, err := ResolveIdsInFunction(scope, origFn, n.Object, localIdentifiers)
		if err != nil {
			return nil, err
		}
		// Substitute member expressions with the object properties
		// they point to if possible.
		//
		// TODO(jlapacik): The following is a complete hack
		// and should be replaced with a proper eval/reduction
		// of the semantic graph. It has been added to aid the
		// planner in pushing down as many predicates to storage
		// as possible.
		//
		// The planner will now be able to push down predicates
		// involving member expressions like so:
		//
		//     r.env == v.env
		//
		if obj, ok := node.(*semantic.ObjectExpression); ok {
			for _, prop := range obj.Properties {
				if prop.Key.Key() == n.Property {
					return ResolveIdsInFunction(scope, origFn, prop.Value, localIdentifiers)
				}
			}
		}
		n.Object = node.(semantic.Expression)
	case *semantic.IdentifierExpression:
		if origFn.Parameters != nil {
			for _, p := range origFn.Parameters.List {
				if n.Name == p.Key.Name {
					// Identifier is a parameter do not resolve
					return n, nil
				}
			}
		}

		// if we are looking at a reference to a locally defined variable,
		// then we can't resolve it because it hasn't been evaluated yet.
		for _, id := range *localIdentifiers {
			if id == n.Name {
				return n, nil
			}
		}

		v, ok := scope.Lookup(n.Name)
		if ok {
			// Attempt to resolve the value if it is possible to inline.
			node, ok, err := resolveValue(v)
			if !ok {
				return n, nil
			}
			return node, err
		}
		return nil, errors.Newf(codes.Invalid, "name %q does not exist in scope", n.Name)
	case *semantic.Block:
		for i, s := range n.Body {
			node, err := ResolveIdsInFunction(scope, origFn, s, localIdentifiers)
			if err != nil {
				return nil, err
			}
			n.Body[i] = node.(semantic.Statement)
		}
	case *semantic.OptionStatement:
		node, err := ResolveIdsInFunction(scope, origFn, n.Assignment, localIdentifiers)
		if err != nil {
			return nil, err
		}
		n.Assignment = node.(semantic.Assignment)
	case *semantic.ExpressionStatement:
		node, err := ResolveIdsInFunction(scope, origFn, n.Expression, localIdentifiers)
		if err != nil {
			return nil, err
		}
		n.Expression = node.(semantic.Expression)
	case *semantic.ReturnStatement:
		node, err := ResolveIdsInFunction(scope, origFn, n.Argument, localIdentifiers)
		if err != nil {
			return nil, err
		}
		n.Argument = node.(semantic.Expression)
	case *semantic.NativeVariableAssignment:
		node, err := ResolveIdsInFunction(scope, origFn, n.Init, localIdentifiers)
		if err != nil {
			return nil, err
		}
		*localIdentifiers = append(*localIdentifiers, n.Identifier.Name)
		n.Init = node.(semantic.Expression)
	case *semantic.CallExpression:
		node, err := ResolveIdsInFunction(scope, origFn, n.Arguments, localIdentifiers)
		if err != nil {
			return nil, err
		}
		// TODO(adam): lookup the function definition, call the function if it's found in scope.
		n.Arguments = node.(*semantic.ObjectExpression)
	case *semantic.FunctionExpression:
		node, err := ResolveIdsInFunction(scope, origFn, n.Block, localIdentifiers)
		if err != nil {
			return nil, err
		}
		n.Block = node.(*semantic.Block)
	case *semantic.BinaryExpression:
		node, err := ResolveIdsInFunction(scope, origFn, n.Left, localIdentifiers)
		if err != nil {
			return nil, err
		}
		n.Left = node.(semantic.Expression)

		node, err = ResolveIdsInFunction(scope, origFn, n.Right, localIdentifiers)
		if err != nil {
			return nil, err
		}
		n.Right = node.(semantic.Expression)
	case *semantic.UnaryExpression:
		node, err := ResolveIdsInFunction(scope, origFn, n.Argument, localIdentifiers)
		if err != nil {
			return nil, err
		}
		n.Argument = node.(semantic.Expression)

	case *semantic.LogicalExpression:
		node, err := ResolveIdsInFunction(scope, origFn, n.Left, localIdentifiers)
		if err != nil {
			return nil, err
		}
		n.Left = node.(semantic.Expression)
		node, err = ResolveIdsInFunction(scope, origFn, n.Right, localIdentifiers)
		if err != nil {
			return nil, err
		}
		n.Right = node.(semantic.Expression)
	case *semantic.ArrayExpression:
		for i, el := range n.Elements {
			node, err := ResolveIdsInFunction(scope, origFn, el, localIdentifiers)
			if err != nil {
				return nil, err
			}
			n.Elements[i] = node.(semantic.Expression)
		}
	case *semantic.IndexExpression:
		node, err := ResolveIdsInFunction(scope, origFn, n.Array, localIdentifiers)
		if err != nil {
			return nil, err
		}
		n.Array = node.(semantic.Expression)
		node, err = ResolveIdsInFunction(scope, origFn, n.Index, localIdentifiers)
		if err != nil {
			return nil, err
		}
		n.Index = node.(semantic.Expression)
	case *semantic.ObjectExpression:
		for i, p := range n.Properties {
			node, err := ResolveIdsInFunction(scope, origFn, p, localIdentifiers)
			if err != nil {
				return nil, err
			}
			n.Properties[i] = node.(*semantic.Property)
		}
	case *semantic.ConditionalExpression:
		node, err := ResolveIdsInFunction(scope, origFn, n.Test, localIdentifiers)
		if err != nil {
			return nil, err
		}
		n.Test = node.(semantic.Expression)

		node, err = ResolveIdsInFunction(scope, origFn, n.Alternate, localIdentifiers)
		if err != nil {
			return nil, err
		}
		n.Alternate = node.(semantic.Expression)

		node, err = ResolveIdsInFunction(scope, origFn, n.Consequent, localIdentifiers)
		if err != nil {
			return nil, err
		}
		n.Consequent = node.(semantic.Expression)
	case *semantic.Property:
		node, err := ResolveIdsInFunction(scope, origFn, n.Value, localIdentifiers)
		if err != nil {
			return nil, err
		}
		n.Value = node.(semantic.Expression)
	}
	return n, nil
}

func resolveValue(v values.Value) (semantic.Node, bool, error) {
	switch k := v.Type().Nature(); k {
	case semantic.String:
		return &semantic.StringLiteral{
			Value: v.Str(),
		}, true, nil
	case semantic.Int:
		return &semantic.IntegerLiteral{
			Value: v.Int(),
		}, true, nil
	case semantic.UInt:
		return &semantic.UnsignedIntegerLiteral{
			Value: v.UInt(),
		}, true, nil
	case semantic.Float:
		return &semantic.FloatLiteral{
			Value: v.Float(),
		}, true, nil
	case semantic.Bool:
		return &semantic.BooleanLiteral{
			Value: v.Bool(),
		}, true, nil
	case semantic.Time:
		return &semantic.DateTimeLiteral{
			Value: v.Time().Time(),
		}, true, nil
	case semantic.Regexp:
		return &semantic.RegexpLiteral{
			Value: v.Regexp(),
		}, true, nil
	case semantic.Duration:
		d := v.Duration()
		var node semantic.Expression = &semantic.DurationLiteral{
			Values: d.AsValues(),
		}
		if d.IsNegative() {
			node = &semantic.UnaryExpression{
				Operator: ast.SubtractionOperator,
				Argument: node,
			}
		}
		return node, true, nil
	case semantic.Function:
		resolver, ok := v.Function().(Resolver)
		if ok {
			node, err := resolver.Resolve()
			return node, true, err
		}
		return nil, false, nil
	case semantic.Array:
		arr := v.Array()
		node := new(semantic.ArrayExpression)
		node.Elements = make([]semantic.Expression, arr.Len())
		var (
			err error
			ok  = true
		)
		arr.Range(func(i int, el values.Value) {
			if err != nil || !ok {
				return
			}
			var n semantic.Node
			n, ok, err = resolveValue(el)
			if err != nil {
				return
			} else if ok {
				node.Elements[i] = n.(semantic.Expression)
			}
		})
		if err != nil || !ok {
			return nil, false, err
		}
		// Determine the element type by looking at the first
		// element. If there are no elements, then we have an
		// array type with an invalid element type.
		elemType := semantic.MonoType{}
		if len(node.Elements) > 0 {
			elemType = node.Elements[0].TypeOf()
		}
		node.Type = semantic.NewArrayType(elemType)
		return node, true, nil
	case semantic.Dictionary:
		dict := v.Dict()
		elements := []struct {
			Key semantic.Expression
			Val semantic.Expression
		}{}
		var (
			err error
			ok  = true
		)
		dict.Range(func(key, val values.Value) {
			if err != nil || !ok {
				return
			}
			var k, v semantic.Node
			k, ok, err = resolveValue(key)
			if err != nil || !ok {
				return
			}
			v, ok, err = resolveValue(val)
			if err != nil || !ok {
				return
			}
			elements = append(elements, struct {
				Key semantic.Expression
				Val semantic.Expression
			}{Key: k.(semantic.Expression), Val: v.(semantic.Expression)})
		})
		if err != nil || !ok {
			return nil, false, err
		}
		return &semantic.DictExpression{
			Elements: elements,
			Type:     dict.Type(),
		}, true, nil
	case semantic.Object:
		obj := v.Object()
		node := new(semantic.ObjectExpression)
		node.Properties = make([]*semantic.Property, 0, obj.Len())
		var (
			err error
			ok  = true
		)
		obj.Range(func(k string, v values.Value) {
			if err != nil || !ok {
				return
			}
			var n semantic.Node
			n, ok, err = resolveValue(v)
			if err != nil {
				return
			} else if ok {
				node.Properties = append(node.Properties, &semantic.Property{
					Key:   &semantic.Identifier{Name: k},
					Value: n.(semantic.Expression),
				})
			}
		})
		if err != nil || !ok {
			return nil, false, err
		}
		return node, true, nil
	default:
		return nil, false, errors.Newf(codes.Internal, "cannot resolve value of type %v", k)
	}
}

func ToStringArray(a values.Array) ([]string, error) {
	t, err := a.Type().ElemType()
	if err != nil {
		return nil, err
	}
	if t.Nature() != semantic.String {
		return nil, errors.Newf(codes.Invalid, "cannot convert array of %v to an array of strings", t)
	}
	strs := make([]string, a.Len())
	a.Range(func(i int, v values.Value) {
		strs[i] = v.Str()
	})
	return strs, nil
}
func ToFloatArray(a values.Array) ([]float64, error) {
	t, err := a.Type().ElemType()
	if err != nil {
		return nil, err
	}
	if t.Nature() != semantic.Float {
		return nil, errors.Newf(codes.Invalid, "cannot convert array of %v to an array of floats", t)
	}
	vs := make([]float64, a.Len())
	a.Range(func(i int, v values.Value) {
		vs[i] = v.Float()
	})
	return vs, nil
}

// Arguments provides access to the keyword arguments passed to a function.
// semantic.The Get{Type} methods return three values: the typed value of the arg,
// whether the argument was specified and any errors about the argument type.
// semantic.The GetRequired{Type} methods return only two values, the typed value of the arg and any errors, a missing argument is considered an error in this case.
type Arguments interface {
	GetAll() []string
	Get(name string) (values.Value, bool)
	GetRequired(name string) (values.Value, error)

	GetString(name string) (string, bool, error)
	GetInt(name string) (int64, bool, error)
	GetFloat(name string) (float64, bool, error)
	GetBool(name string) (bool, bool, error)
	GetFunction(name string) (values.Function, bool, error)
	GetArray(name string, t semantic.Nature) (values.Array, bool, error)
	GetObject(name string) (values.Object, bool, error)
	GetDictionary(name string) (values.Dictionary, bool, error)

	GetRequiredString(name string) (string, error)
	GetRequiredInt(name string) (int64, error)
	GetRequiredFloat(name string) (float64, error)
	GetRequiredBool(name string) (bool, error)
	GetRequiredFunction(name string) (values.Function, error)
	GetRequiredArray(name string, t semantic.Nature) (values.Array, error)
	GetRequiredArrayAllowEmpty(name string, t semantic.Nature) (values.Array, error)
	GetRequiredObject(name string) (values.Object, error)
	GetRequiredDictionary(name string) (values.Dictionary, error)

	// listUnused returns the list of provided arguments that were not used by the function.
	listUnused() []string
}

type arguments struct {
	obj  values.Object
	used map[string]bool
}

func newArguments(obj values.Object) *arguments {
	if obj == nil {
		return new(arguments)
	}
	return &arguments{
		obj:  obj,
		used: make(map[string]bool, obj.Len()),
	}
}
func NewArguments(obj values.Object) Arguments {
	return newArguments(obj)
}

func (a *arguments) GetAll() []string {
	args := make([]string, 0, a.obj.Len())
	a.obj.Range(func(name string, v values.Value) {
		args = append(args, name)
	})
	return args
}

func (a *arguments) Get(name string) (values.Value, bool) {
	a.used[name] = true
	v, ok := a.obj.Get(name)
	return v, ok
}

func (a *arguments) GetRequired(name string) (values.Value, error) {
	a.used[name] = true
	v, ok := a.obj.Get(name)
	if !ok {
		return nil, errors.Newf(codes.Invalid, "missing required keyword argument %q", name)
	}
	return v, nil
}

func (a *arguments) GetString(name string) (string, bool, error) {
	v, ok, err := a.get(name, semantic.String, false)
	if err != nil || !ok {
		return "", ok, err
	}
	return v.Str(), ok, nil
}
func (a *arguments) GetRequiredString(name string) (string, error) {
	v, _, err := a.get(name, semantic.String, true)
	if err != nil {
		return "", err
	}
	return v.Str(), nil
}
func (a *arguments) GetInt(name string) (int64, bool, error) {
	v, ok, err := a.get(name, semantic.Int, false)
	if err != nil || !ok {
		return 0, ok, err
	}
	return v.Int(), ok, nil
}
func (a *arguments) GetRequiredInt(name string) (int64, error) {
	v, _, err := a.get(name, semantic.Int, true)
	if err != nil {
		return 0, err
	}
	return v.Int(), nil
}
func (a *arguments) GetFloat(name string) (float64, bool, error) {
	v, ok, err := a.get(name, semantic.Float, false)
	if err != nil || !ok {
		return 0, ok, err
	}
	return v.Float(), ok, nil
}
func (a *arguments) GetRequiredFloat(name string) (float64, error) {
	v, _, err := a.get(name, semantic.Float, true)
	if err != nil {
		return 0, err
	}
	return v.Float(), nil
}
func (a *arguments) GetBool(name string) (bool, bool, error) {
	v, ok, err := a.get(name, semantic.Bool, false)
	if err != nil || !ok {
		return false, ok, err
	}
	return v.Bool(), ok, nil
}
func (a *arguments) GetRequiredBool(name string) (bool, error) {
	v, _, err := a.get(name, semantic.Bool, true)
	if err != nil {
		return false, err
	}
	return v.Bool(), nil
}

func (a *arguments) GetArray(name string, t semantic.Nature) (values.Array, bool, error) {
	v, ok, err := a.get(name, semantic.Array, false)
	if err != nil || !ok {
		return nil, ok, err
	}
	arr := v.Array()
	et, err := arr.Type().ElemType()
	if err != nil {
		return nil, false, err
	}
	if et.Nature() != t {
		return nil, true, errors.Newf(codes.Invalid, "keyword argument %q should be of an array of type %v, but got an array of type %v", name, t, arr.Type())
	}
	return v.Array(), ok, nil
}
func (a *arguments) GetRequiredArray(name string, t semantic.Nature) (values.Array, error) {
	v, _, err := a.get(name, semantic.Array, true)
	if err != nil {
		return nil, err
	}
	arr := v.Array()
	et, err := arr.Type().ElemType()
	if err != nil {
		return nil, err
	}
	if et.Nature() != t {
		return nil, errors.Newf(codes.Invalid, "keyword argument %q should be of an array of type %v, but got an array of type %v", name, t, arr.Type())
	}
	return arr, nil
}

// Ensures a required array (with element type) is present, but unlike
// GetRequiredArray, does not fail if the array is empty.
func (a *arguments) GetRequiredArrayAllowEmpty(name string, t semantic.Nature) (values.Array, error) {
	v, _, err := a.get(name, semantic.Array, true)
	if err != nil {
		return nil, err
	}
	arr := v.Array()
	if arr.Array().Len() > 0 {
		et, err := arr.Type().ElemType()
		if err != nil {
			return nil, err
		}
		if et.Nature() != t {
			return nil, errors.Newf(codes.Invalid, "keyword argument %q should be of an array of type %v, but got an array of type %v", name, t, arr.Type())
		}
	}
	return arr, nil
}
func (a *arguments) GetFunction(name string) (values.Function, bool, error) {
	v, ok, err := a.get(name, semantic.Function, false)
	if err != nil || !ok {
		return nil, ok, err
	}
	return v.Function(), ok, nil
}
func (a *arguments) GetRequiredFunction(name string) (values.Function, error) {
	v, _, err := a.get(name, semantic.Function, true)
	if err != nil {
		return nil, err
	}
	return v.Function(), nil
}

func (a *arguments) GetObject(name string) (values.Object, bool, error) {
	v, ok, err := a.get(name, semantic.Object, false)
	if err != nil || !ok {
		return nil, ok, err
	}
	return v.Object(), ok, nil
}
func (a *arguments) GetRequiredObject(name string) (values.Object, error) {
	v, _, err := a.get(name, semantic.Object, true)
	if err != nil {
		return nil, err
	}
	return v.Object(), nil
}

func (a *arguments) GetDictionary(name string) (values.Dictionary, bool, error) {
	v, ok, err := a.get(name, semantic.Dictionary, false)
	if err != nil || !ok {
		return nil, ok, err
	}
	return v.Dict(), ok, nil
}
func (a *arguments) GetRequiredDictionary(name string) (values.Dictionary, error) {
	v, _, err := a.get(name, semantic.Dictionary, true)
	if err != nil {
		return nil, err
	}
	return v.Dict(), nil
}

func (a *arguments) get(name string, kind semantic.Nature, required bool) (values.Value, bool, error) {
	a.used[name] = true
	v, ok := a.obj.Get(name)
	if !ok {
		if required {
			return nil, false, errors.Newf(codes.Invalid, "missing required keyword argument %q", name)
		}
		return nil, false, nil
	}
	if v.Type().Nature() != kind {
		return nil, true, errors.Newf(codes.Invalid, "keyword argument %q should be of kind %v, but got %v", name, kind, v.Type().Nature())
	}
	return v, true, nil
}

func (a *arguments) listUnused() []string {
	var unused []string
	if a.obj != nil {
		a.obj.Range(func(k string, v values.Value) {
			if !a.used[k] {
				unused = append(unused, k)
			}
		})
	}
	return unused
}

type contextKey int

const (
	callStackKey contextKey = iota
)

// StackEntry describes a single entry in the call stack.
type StackEntry struct {
	FunctionName string
	Location     ast.SourceLocation
}

// stackElement describes a single entry in the call stack.
type stackElement struct {
	parent *stackElement
	entry  StackEntry
	depth  int
}

// Stack retrieves the call stack for a given context.
func Stack(ctx context.Context) []StackEntry {
	e, ok := ctx.Value(callStackKey).(*stackElement)
	if !ok {
		return nil
	}

	stack := make([]StackEntry, 0, e.depth+1)
	for e != nil {
		stack = append(stack, e.entry)
		e = e.parent
	}
	return stack
}

// withStackEntry will attach StackEntry information
// to the context to be retrieved by Stack.
func withStackEntry(ctx context.Context, name string, loc ast.SourceLocation) context.Context {
	stack := &stackElement{
		entry: StackEntry{
			FunctionName: name,
			Location:     loc,
		},
	}
	if parent := ctx.Value(callStackKey); parent != nil {
		stack.parent = parent.(*stackElement)
		stack.depth = stack.parent.depth + 1
	}
	return context.WithValue(ctx, callStackKey, stack)
}
