package semantic

import (
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
)

// New creates a semantic graph from the provided AST
func New(pkg *ast.Package) (*Package, error) {
	p, err := analyzePackage(pkg)
	if err != nil {
		return nil, err
	}
	vars := make(map[string]bool)
	opts := make(map[string]bool)
	if err := runChecks(p, vars, opts); err != nil {
		return nil, err
	}
	return p, nil
}

func analyzePackage(pkg *ast.Package) (*Package, error) {
	p := &Package{
		Package: pkg.Package,
		Files:   make([]*File, len(pkg.Files)),
	}
	for i, file := range pkg.Files {
		f, err := analyzeFile(file)
		if err != nil {
			return nil, err
		}
		p.Files[i] = f
	}
	return p, nil
}

func analyzeFile(file *ast.File) (*File, error) {
	f := &File{
		Loc:  Loc(file.Location()),
		Body: make([]Statement, len(file.Body)),
	}
	pkg, err := analyzePackageClause(file.Package)
	if err != nil {
		return nil, err
	}
	f.Package = pkg

	if len(file.Imports) > 0 {
		f.Imports = make([]*ImportDeclaration, len(file.Imports))
		for i, imp := range file.Imports {
			n, err := analyzeImportDeclaration(imp)
			if err != nil {
				return nil, err
			}
			f.Imports[i] = n
		}
	}

	for i, s := range file.Body {
		n, err := analyzeStatement(s)
		if err != nil {
			return nil, err
		}
		f.Body[i] = n
	}
	return f, nil
}

func analyzePackageClause(pkg *ast.PackageClause) (*PackageClause, error) {
	if pkg == nil {
		return nil, nil
	}
	name, err := analyzeIdentifier(pkg.Name)
	if err != nil {
		return nil, err
	}
	return &PackageClause{
		Loc:  Loc(pkg.Location()),
		Name: name,
	}, nil
}

func analyzeImportDeclaration(imp *ast.ImportDeclaration) (*ImportDeclaration, error) {
	n := &ImportDeclaration{
		Loc: Loc(imp.Location()),
	}
	if imp.As != nil {
		as, err := analyzeIdentifier(imp.As)
		if err != nil {
			return nil, err
		}
		n.As = as
	}

	path, err := analyzeStringLiteral(imp.Path)
	if err != nil {
		return nil, err
	}
	n.Path = path
	return n, nil
}

func analyzeNode(n ast.Node) (Node, error) {
	switch n := n.(type) {
	case ast.Statement:
		return analyzeStatement(n)
	case ast.Expression:
		return analyzeExpression(n)
	case *ast.Block:
		return analyzeBlock(n)
	default:
		return nil, errors.Newf(codes.Internal, "unsupported node %T", n)
	}
}

func analyzeStatement(s ast.Statement) (Statement, error) {
	switch s := s.(type) {
	case *ast.OptionStatement:
		return analyzeOptionStatement(s)
	case *ast.BuiltinStatement:
		return analyzeBuiltinStatement(s)
	case *ast.TestStatement:
		return analyzeTestStatement(s)
	case *ast.ExpressionStatement:
		return analyzeExpressionStatement(s)
	case *ast.ReturnStatement:
		return analyzeReturnStatement(s)
	case *ast.VariableAssignment:
		return analyzeVariableAssignment(s)
	case *ast.MemberAssignment:
		return analyzeMemberAssignment(s)
	default:
		return nil, errors.Newf(codes.Internal, "unsupported statement %T", s)
	}
}

func analyzeAssignment(a ast.Assignment) (Assignment, error) {
	switch a := a.(type) {
	case *ast.VariableAssignment:
		return analyzeVariableAssignment(a)
	case *ast.MemberAssignment:
		return analyzeMemberAssignment(a)
	default:
		return nil, errors.Newf(codes.Internal, "unsupported assignment %T", a)
	}
}

func analyzeBlock(block *ast.Block) (*Block, error) {
	b := &Block{
		Loc:  Loc(block.Location()),
		Body: make([]Statement, len(block.Body)),
	}
	for i, s := range block.Body {
		n, err := analyzeStatement(s)
		if err != nil {
			return nil, err
		}
		b.Body[i] = n
	}
	last := len(b.Body) - 1
	if _, ok := b.Body[last].(*ReturnStatement); !ok {
		return nil, errors.New(codes.Invalid, "missing return statement in block")
	}
	return b, nil
}

func analyzeOptionStatement(option *ast.OptionStatement) (*OptionStatement, error) {
	assignment, err := analyzeAssignment(option.Assignment)
	if err != nil {
		return nil, err
	}
	return &OptionStatement{
		Loc:        Loc(option.Location()),
		Assignment: assignment,
	}, nil
}

func analyzeBuiltinStatement(builtin *ast.BuiltinStatement) (*BuiltinStatement, error) {
	ident, err := analyzeIdentifier(builtin.ID)
	if err != nil {
		return nil, err
	}
	return &BuiltinStatement{
		Loc: Loc(builtin.Location()),
		ID:  ident,
	}, nil
}
func analyzeTestStatement(test *ast.TestStatement) (*TestStatement, error) {
	assignment, err := analyzeVariableAssignment(test.Assignment)
	if err != nil {
		return nil, err
	}
	return &TestStatement{
		Loc:        Loc(test.Location()),
		Assignment: assignment,
	}, nil
}
func analyzeExpressionStatement(expr *ast.ExpressionStatement) (*ExpressionStatement, error) {
	e, err := analyzeExpression(expr.Expression)
	if err != nil {
		return nil, err
	}
	return &ExpressionStatement{
		Loc:        Loc(expr.Location()),
		Expression: e,
	}, nil
}

func analyzeReturnStatement(ret *ast.ReturnStatement) (*ReturnStatement, error) {
	arg, err := analyzeExpression(ret.Argument)
	if err != nil {
		return nil, err
	}
	return &ReturnStatement{
		Loc:      Loc(ret.Location()),
		Argument: arg,
	}, nil
}

func analyzeVariableAssignment(decl *ast.VariableAssignment) (*NativeVariableAssignment, error) {
	id, err := analyzeIdentifier(decl.ID)
	if err != nil {
		return nil, err
	}
	init, err := analyzeExpression(decl.Init)
	if err != nil {
		return nil, err
	}
	vd := &NativeVariableAssignment{
		Loc:        Loc(decl.Location()),
		Identifier: id,
		Init:       init,
	}
	return vd, nil
}

func analyzeMemberAssignment(a *ast.MemberAssignment) (*MemberAssignment, error) {
	member, err := analyzeMemberExpression(a.Member)
	if err != nil {
		return nil, err
	}
	init, err := analyzeExpression(a.Init)
	if err != nil {
		return nil, err
	}
	return &MemberAssignment{
		Loc:    Loc(a.Location()),
		Member: member,
		Init:   init,
	}, nil
}

func analyzeExpression(expr ast.Expression) (Expression, error) {
	switch expr := expr.(type) {
	case *ast.FunctionExpression:
		return analyzeFunctionExpression(expr)
	case *ast.CallExpression:
		return analyzeCallExpression(expr)
	case *ast.MemberExpression:
		return analyzeMemberExpression(expr)
	case *ast.IndexExpression:
		return analyzeIndexExpression(expr)
	case *ast.PipeExpression:
		return analyzePipeExpression(expr)
	case *ast.BinaryExpression:
		return analyzeBinaryExpression(expr)
	case *ast.UnaryExpression:
		return analyzeUnaryExpression(expr)
	case *ast.LogicalExpression:
		return analyzeLogicalExpression(expr)
	case *ast.ConditionalExpression:
		return analyzeConditionalExpression(expr)
	case *ast.ObjectExpression:
		return analyzeObjectExpression(expr)
	case *ast.ArrayExpression:
		return analyzeArrayExpression(expr)
	case *ast.Identifier:
		return analyzeIdentifierExpression(expr)
	case *ast.StringExpression:
		return analyzeStringExpression(expr)
	case *ast.ParenExpression:
		// Unwrap the parenthesis and analyze underlying Expression.
		return analyzeExpression(expr.Expression)
	case ast.Literal:
		return analyzeLiteral(expr)
	default:
		return nil, errors.Newf(codes.Internal, "unsupported expression %T", expr)
	}
}

func analyzeStringExpression(expr *ast.StringExpression) (Expression, error) {
	parts := make([]StringExpressionPart, len(expr.Parts))
	for i, p := range expr.Parts {
		part, err := analyzeStringExpressionPart(p)
		if err != nil {
			return nil, err
		}
		parts[i] = part
	}
	return &StringExpression{
		Loc:   Loc(expr.Location()),
		Parts: parts,
	}, nil
}

func analyzeStringExpressionPart(part ast.StringExpressionPart) (StringExpressionPart, error) {
	switch p := part.(type) {
	case *ast.TextPart:
		return analyzeTextPart(p)
	case *ast.InterpolatedPart:
		return analyzeInterpolatedPart(p)
	}
	return nil, errors.Newf(codes.Internal, "unsupported string interpolation part %T", part)
}

func analyzeTextPart(part *ast.TextPart) (*TextPart, error) {
	return &TextPart{
		Loc:   Loc(part.Location()),
		Value: part.Value,
	}, nil
}

func analyzeInterpolatedPart(part *ast.InterpolatedPart) (*InterpolatedPart, error) {
	expr, err := analyzeExpression(part.Expression)
	if err != nil {
		return nil, err
	}
	return &InterpolatedPart{
		Loc:        Loc(part.Location()),
		Expression: expr,
	}, nil
}

func analyzeLiteral(lit ast.Literal) (Literal, error) {
	switch lit := lit.(type) {
	case *ast.StringLiteral:
		return analyzeStringLiteral(lit)
	case *ast.BooleanLiteral:
		return analyzeBooleanLiteral(lit)
	case *ast.FloatLiteral:
		return analyzeFloatLiteral(lit)
	case *ast.IntegerLiteral:
		return analyzeIntegerLiteral(lit)
	case *ast.UnsignedIntegerLiteral:
		return analyzeUnsignedIntegerLiteral(lit)
	case *ast.RegexpLiteral:
		return analyzeRegexpLiteral(lit)
	case *ast.DurationLiteral:
		return analyzeDurationLiteral(lit)
	case *ast.DateTimeLiteral:
		return analyzeDateTimeLiteral(lit)
	case *ast.PipeLiteral:
		return nil, errors.New(codes.Invalid, "a pipe literal may only be used as a default value for an argument in a function definition")
	default:
		return nil, errors.Newf(codes.Internal, "unsupported literal %T", lit)
	}
}

func analyzePropertyKey(key ast.PropertyKey) (PropertyKey, error) {
	switch key := key.(type) {
	case *ast.Identifier:
		return analyzeIdentifier(key)
	case *ast.StringLiteral:
		return analyzeStringLiteral(key)
	default:
		return nil, errors.Newf(codes.Internal, "unsupported key %T", key)
	}
}

func analyzeFunctionExpression(arrow *ast.FunctionExpression) (*FunctionExpression, error) {
	var parameters *FunctionParameters
	var defaults *ObjectExpression
	if len(arrow.Params) > 0 {
		pipedCount := 0
		parameters = &FunctionParameters{
			Loc: Loc(arrow.Location()),
		}
		parameters.List = make([]*FunctionParameter, len(arrow.Params))
		for i, p := range arrow.Params {
			ident, ok := p.Key.(*ast.Identifier)
			if !ok {
				return nil, errors.New(codes.Invalid, "function params must be identifiers")
			}
			key, err := analyzeIdentifier(ident)
			if err != nil {
				return nil, err
			}

			var def Expression
			var piped bool
			if p.Value != nil {
				if _, ok := p.Value.(*ast.PipeLiteral); ok {
					// Special case the PipeLiteral
					piped = true
					pipedCount++
					if pipedCount > 1 {
						return nil, errors.New(codes.Invalid, "only a single argument may be piped")
					}
				} else {
					d, err := analyzeExpression(p.Value)
					if err != nil {
						return nil, err
					}
					def = d
				}
			}

			parameters.List[i] = &FunctionParameter{
				Loc: Loc(p.Location()),
				Key: key,
			}
			if def != nil {
				if defaults == nil {
					defaults = &ObjectExpression{
						Loc:        Loc(arrow.Location()),
						Properties: make([]*Property, 0, len(arrow.Params)),
					}
				}
				defaults.Properties = append(defaults.Properties, &Property{
					Loc:   Loc(p.Location()),
					Key:   key,
					Value: def,
				})
			}
			if piped {
				parameters.Pipe = key
			}
		}
	}

	b, err := analyzeNode(arrow.Body)
	if err != nil {
		return nil, err
	}

	body, ok := b.(*Block)
	if !ok {
		// Is a single semantic expression.
		expr, ok := b.(Expression)
		if !ok {
			return nil, errors.Newf(codes.Internal, "function body must be a block or expression, got %T", b)
		}
		body = &Block{
			Loc: Loc(expr.Location()),
			Body: []Statement{
				&ReturnStatement{
					Loc:      Loc(expr.Location()),
					Argument: expr,
				},
			},
		}
	}

	f := &FunctionExpression{
		Loc:        Loc(arrow.Location()),
		Parameters: parameters,
		Defaults:   defaults,
		Block:      body,
	}

	return f, nil
}

func analyzeCallExpression(call *ast.CallExpression) (*CallExpression, error) {
	callee, err := analyzeExpression(call.Callee)
	if err != nil {
		return nil, err
	}
	var args *ObjectExpression
	if l := len(call.Arguments); l > 1 {
		return nil, errors.Newf(codes.Internal, "arguments are not a single object expression %v", args)
	} else if l == 1 {
		obj, ok := call.Arguments[0].(*ast.ObjectExpression)
		if !ok {
			return nil, errors.New(codes.Internal, "arguments not an object expression")
		}
		var err error
		args, err = analyzeObjectExpression(obj)
		if err != nil {
			return nil, err
		}
	} else {
		args = &ObjectExpression{
			Loc: Loc(call.Location()),
		}
	}

	return &CallExpression{
		Loc:       Loc(call.Location()),
		Callee:    callee,
		Arguments: args,
	}, nil
}

func analyzeMemberExpression(member *ast.MemberExpression) (*MemberExpression, error) {
	obj, err := analyzeExpression(member.Object)
	if err != nil {
		return nil, err
	}
	var prop string
	switch n := member.Property.(type) {
	case *ast.Identifier:
		prop = n.Name
	case *ast.StringLiteral:
		prop = n.Value
	}
	return &MemberExpression{
		Loc:      Loc(member.Location()),
		Object:   obj,
		Property: prop,
	}, nil
}

func analyzeIndexExpression(e *ast.IndexExpression) (Expression, error) {
	array, err := analyzeExpression(e.Array)
	if err != nil {
		return nil, err
	}
	index, err := analyzeExpression(e.Index)
	if err != nil {
		return nil, err
	}
	return &IndexExpression{
		Loc:   Loc(e.Location()),
		Array: array,
		Index: index,
	}, nil
}

func analyzePipeExpression(pipe *ast.PipeExpression) (*CallExpression, error) {
	call, err := analyzeCallExpression(pipe.Call)
	if err != nil {
		return nil, err
	}

	value, err := analyzeExpression(pipe.Argument)
	if err != nil {
		return nil, err
	}

	call.Pipe = value
	return call, nil
}

func analyzeBinaryExpression(binary *ast.BinaryExpression) (*BinaryExpression, error) {
	left, err := analyzeExpression(binary.Left)
	if err != nil {
		return nil, err
	}
	right, err := analyzeExpression(binary.Right)
	if err != nil {
		return nil, err
	}
	return &BinaryExpression{
		Loc:      Loc(binary.Location()),
		Operator: binary.Operator,
		Left:     left,
		Right:    right,
	}, nil
}

func analyzeUnaryExpression(unary *ast.UnaryExpression) (*UnaryExpression, error) {
	arg, err := analyzeExpression(unary.Argument)
	if err != nil {
		return nil, err
	}
	return &UnaryExpression{
		Loc:      Loc(unary.Location()),
		Operator: unary.Operator,
		Argument: arg,
	}, nil
}
func analyzeLogicalExpression(logical *ast.LogicalExpression) (*LogicalExpression, error) {
	left, err := analyzeExpression(logical.Left)
	if err != nil {
		return nil, err
	}
	right, err := analyzeExpression(logical.Right)
	if err != nil {
		return nil, err
	}
	return &LogicalExpression{
		Loc:      Loc(logical.Location()),
		Operator: logical.Operator,
		Left:     left,
		Right:    right,
	}, nil
}
func analyzeConditionalExpression(ce *ast.ConditionalExpression) (*ConditionalExpression, error) {
	t, err := analyzeExpression(ce.Test)
	if err != nil {
		return nil, err
	}
	c, err := analyzeExpression(ce.Consequent)
	if err != nil {
		return nil, err
	}
	a, err := analyzeExpression(ce.Alternate)
	if err != nil {
		return nil, err
	}
	return &ConditionalExpression{
		Loc:        Loc(ce.Location()),
		Test:       t,
		Consequent: c,
		Alternate:  a,
	}, nil
}
func analyzeObjectExpression(obj *ast.ObjectExpression) (*ObjectExpression, error) {
	o := &ObjectExpression{
		Loc:        Loc(obj.Location()),
		Properties: make([]*Property, len(obj.Properties)),
	}
	if obj.With != nil {
		w, err := analyzeIdentifierExpression(obj.With)
		if err != nil {
			return nil, err
		}
		o.With = w
	}
	for i, p := range obj.Properties {
		n, err := analyzeProperty(p)
		if err != nil {
			return nil, err
		}
		o.Properties[i] = n
	}
	return o, nil
}
func analyzeArrayExpression(array *ast.ArrayExpression) (*ArrayExpression, error) {
	a := &ArrayExpression{
		Loc:      Loc(array.Location()),
		Elements: make([]Expression, len(array.Elements)),
	}
	for i, e := range array.Elements {
		n, err := analyzeExpression(e)
		if err != nil {
			return nil, err
		}
		a.Elements[i] = n
	}
	return a, nil
}

func analyzeIdentifier(ident *ast.Identifier) (*Identifier, error) {
	return &Identifier{
		Loc:  Loc(ident.Location()),
		Name: ident.Name,
	}, nil
}

func analyzeIdentifierExpression(ident *ast.Identifier) (*IdentifierExpression, error) {
	return &IdentifierExpression{
		Loc:  Loc(ident.Location()),
		Name: ident.Name,
	}, nil
}

func analyzeProperty(property *ast.Property) (*Property, error) {
	key, err := analyzePropertyKey(property.Key)
	if err != nil {
		return nil, err
	}
	if property.Value == nil {
		return &Property{
			Loc: Loc(property.Location()),
			Key: key,
			Value: &IdentifierExpression{
				Loc:  Loc(key.Location()),
				Name: key.Key(),
			},
		}, nil
	}
	value, err := analyzeExpression(property.Value)
	if err != nil {
		return nil, err
	}
	return &Property{
		Loc:   Loc(property.Location()),
		Key:   key,
		Value: value,
	}, nil
}

func analyzeDateTimeLiteral(lit *ast.DateTimeLiteral) (*DateTimeLiteral, error) {
	return &DateTimeLiteral{
		Loc:   Loc(lit.Location()),
		Value: lit.Value,
	}, nil
}
func analyzeDurationLiteral(lit *ast.DurationLiteral) (*DurationLiteral, error) {
	return &DurationLiteral{
		Loc:    Loc(lit.Location()),
		Values: lit.Values,
	}, nil
}
func analyzeFloatLiteral(lit *ast.FloatLiteral) (*FloatLiteral, error) {
	return &FloatLiteral{
		Loc:   Loc(lit.Location()),
		Value: lit.Value,
	}, nil
}
func analyzeIntegerLiteral(lit *ast.IntegerLiteral) (*IntegerLiteral, error) {
	return &IntegerLiteral{
		Loc:   Loc(lit.Location()),
		Value: lit.Value,
	}, nil
}
func analyzeUnsignedIntegerLiteral(lit *ast.UnsignedIntegerLiteral) (*UnsignedIntegerLiteral, error) {
	return &UnsignedIntegerLiteral{
		Loc:   Loc(lit.Location()),
		Value: lit.Value,
	}, nil
}
func analyzeStringLiteral(lit *ast.StringLiteral) (*StringLiteral, error) {
	return &StringLiteral{
		Loc:   Loc(lit.Location()),
		Value: lit.Value,
	}, nil
}
func analyzeBooleanLiteral(lit *ast.BooleanLiteral) (*BooleanLiteral, error) {
	return &BooleanLiteral{
		Loc:   Loc(lit.Location()),
		Value: lit.Value,
	}, nil
}
func analyzeRegexpLiteral(lit *ast.RegexpLiteral) (*RegexpLiteral, error) {
	return &RegexpLiteral{
		Loc:   Loc(lit.Location()),
		Value: lit.Value,
	}, nil
}
