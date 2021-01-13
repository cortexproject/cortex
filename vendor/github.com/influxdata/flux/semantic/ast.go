package semantic

import (
	"fmt"

	"github.com/influxdata/flux/ast"
)

// ToAST will construct an AST from the semantic graph.
// The AST will not be preserved when going from
// AST -> semantic -> AST, but going from
// semantic -> AST -> semantic should result in the same graph.
func ToAST(n Node) ast.Node {
	switch n := n.(type) {
	case *Package:
		p := &ast.Package{
			Package: n.Package,
			Files:   make([]*ast.File, len(n.Files)),
		}
		for i, f := range n.Files {
			p.Files[i] = ToAST(f).(*ast.File)
		}
		return p
	case *File:
		f := &ast.File{
			Package: ToAST(n.Package).(*ast.PackageClause),
			Body:    make([]ast.Statement, len(n.Body)),
		}
		if n.Imports != nil {
			f.Imports = make([]*ast.ImportDeclaration, len(n.Imports))
			for i, imp := range n.Imports {
				f.Imports[i] = ToAST(imp).(*ast.ImportDeclaration)
			}
		}
		for i, stmt := range n.Body {
			f.Body[i] = ToAST(stmt).(ast.Statement)
		}
		return f
	case *Block:
		b := &ast.Block{
			Body: make([]ast.Statement, len(n.Body)),
		}
		for i, stmt := range n.Body {
			b.Body[i] = ToAST(stmt).(ast.Statement)
		}
		return b
	case *PackageClause:
		return &ast.PackageClause{
			Name: &ast.Identifier{Name: n.Name.Name},
		}
	case *ImportDeclaration:
		decl := &ast.ImportDeclaration{
			Path: &ast.StringLiteral{Value: n.Path.Value},
		}
		if n.As != nil {
			decl.As = &ast.Identifier{Name: n.As.Name}
		}
		return decl
	case *OptionStatement:
		assign := ToAST(n.Assignment).(ast.Assignment)
		if m, ok := assign.(*ast.MemberAssignment); ok {
			// Option statements must use identifiers for the property.
			// This only occurs when using a member assignment which
			// defaults to using a string literal.
			if v, ok := m.Member.Property.(*ast.StringLiteral); ok {
				m.Member.Property = &ast.Identifier{Name: v.Value}
			}
		}
		return &ast.OptionStatement{
			Assignment: assign,
		}
	case *BuiltinStatement:
		return &ast.BuiltinStatement{
			ID: &ast.Identifier{Name: n.ID.Name},
		}
	case *TestStatement:
		return &ast.TestStatement{
			Assignment: ToAST(n.Assignment).(*ast.VariableAssignment),
		}
	case *ExpressionStatement:
		return &ast.ExpressionStatement{
			Expression: ToAST(n.Expression).(ast.Expression),
		}
	case *ReturnStatement:
		return &ast.ReturnStatement{
			Argument: ToAST(n.Argument).(ast.Expression),
		}
	case *MemberAssignment:
		return &ast.MemberAssignment{
			Init: ToAST(n.Init).(ast.Expression),
			Member: &ast.MemberExpression{
				Object:   ToAST(n.Member.Object).(ast.Expression),
				Property: &ast.StringLiteral{Value: n.Member.Property},
			},
		}
	case *NativeVariableAssignment:
		return &ast.VariableAssignment{
			ID:   &ast.Identifier{Name: n.Identifier.Name},
			Init: ToAST(n.Init).(ast.Expression),
		}
	case *StringExpression:
		se := &ast.StringExpression{
			Parts: make([]ast.StringExpressionPart, len(n.Parts)),
		}
		for i, p := range n.Parts {
			se.Parts[i] = ToAST(p).(ast.StringExpressionPart)
		}
		return se
	case *ArrayExpression:
		arr := &ast.ArrayExpression{
			Elements: make([]ast.Expression, len(n.Elements)),
		}
		for i, e := range n.Elements {
			arr.Elements[i] = ToAST(e).(ast.Expression)
		}
		return arr
	case *FunctionExpression:
		fn := &ast.FunctionExpression{
			Body: ToAST(n.Block),
		}
		if n.Parameters != nil {
			fn.Params = make([]*ast.Property, len(n.Parameters.List))
			for i, p := range n.Parameters.List {
				fn.Params[i] = &ast.Property{
					Key: &ast.Identifier{Name: p.Key.Name},
				}
				if n.Defaults != nil {
					for _, pn := range n.Defaults.Properties {
						if pn.Key.Key() == p.Key.Name {
							fn.Params[i].Value = ToAST(pn.Value).(ast.Expression)
							break
						}
					}
				}
			}
		}
		return fn
	case *BinaryExpression:
		return &ast.BinaryExpression{
			Operator: n.Operator,
			Left:     ToAST(n.Left).(ast.Expression),
			Right:    ToAST(n.Right).(ast.Expression),
		}
	case *CallExpression:
		call := &ast.CallExpression{
			Callee: ToAST(n.Callee).(ast.Expression),
		}
		if n.Arguments != nil {
			call.Arguments = []ast.Expression{
				ToAST(n.Arguments).(ast.Expression),
			}
		}
		return call
	case *ConditionalExpression:
		return &ast.ConditionalExpression{
			Test:       ToAST(n.Test).(ast.Expression),
			Consequent: ToAST(n.Consequent).(ast.Expression),
			Alternate:  ToAST(n.Alternate).(ast.Expression),
		}
	case *IdentifierExpression:
		return &ast.Identifier{Name: n.Name}
	case *LogicalExpression:
		return &ast.LogicalExpression{
			Operator: n.Operator,
			Left:     ToAST(n.Left).(ast.Expression),
			Right:    ToAST(n.Right).(ast.Expression),
		}
	case *MemberExpression:
		return &ast.MemberExpression{
			Object:   ToAST(n.Object).(ast.Expression),
			Property: &ast.StringLiteral{Value: n.Property},
		}
	case *IndexExpression:
		return &ast.IndexExpression{
			Array: ToAST(n.Array).(ast.Expression),
			Index: ToAST(n.Index).(ast.Expression),
		}
	case *ObjectExpression:
		obj := &ast.ObjectExpression{}
		if n.With != nil {
			obj.With = &ast.Identifier{Name: n.With.Name}
		}
		if len(n.Properties) > 0 {
			obj.Properties = make([]*ast.Property, len(n.Properties))
			for i, on := range n.Properties {
				obj.Properties[i] = ToAST(on).(*ast.Property)
			}
		}
		return obj
	case *UnaryExpression:
		return &ast.UnaryExpression{
			Operator: n.Operator,
			Argument: ToAST(n.Argument).(ast.Expression),
		}
	case *Identifier:
		return &ast.Identifier{Name: n.Name}
	case *Property:
		p := &ast.Property{}
		switch key := n.Key.(type) {
		case *Identifier:
			p.Key = &ast.Identifier{Name: key.Name}
		default:
			p.Key = &ast.StringLiteral{Value: key.Key()}
		}
		if n.Value != nil {
			p.Value = ToAST(n.Value).(ast.Expression)
		}
		return p
	case *TextPart:
		return &ast.TextPart{
			Value: n.Value,
		}
	case *InterpolatedPart:
		return &ast.InterpolatedPart{
			Expression: ToAST(n.Expression).(ast.Expression),
		}
	case *BooleanLiteral:
		return &ast.BooleanLiteral{Value: n.Value}
	case *DateTimeLiteral:
		return &ast.DateTimeLiteral{Value: n.Value}
	case *DurationLiteral:
		return &ast.DurationLiteral{Values: n.Values}
	case *FloatLiteral:
		return &ast.FloatLiteral{Value: n.Value}
	case *IntegerLiteral:
		return &ast.IntegerLiteral{Value: n.Value}
	case *StringLiteral:
		return &ast.StringLiteral{Value: n.Value}
	case *RegexpLiteral:
		return &ast.RegexpLiteral{Value: n.Value}
	case *UnsignedIntegerLiteral:
		return &ast.UnsignedIntegerLiteral{Value: n.Value}
	default:
		panic(fmt.Sprintf("cannot transform semantic node of type %T to an ast node", n))
	}
}
