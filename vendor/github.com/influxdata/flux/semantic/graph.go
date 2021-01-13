package semantic

import (
	"regexp"
	"time"

	"github.com/influxdata/flux/ast"
)

type Node interface {
	node()
	NodeType() string
	Copy() Node

	Location() ast.SourceLocation
}

type Loc ast.SourceLocation

func (l Loc) Location() ast.SourceLocation {
	return ast.SourceLocation(l)
}

func (*Package) node()           {}
func (*File) node()              {}
func (*Block) node()             {}
func (*PackageClause) node()     {}
func (*ImportDeclaration) node() {}

func (*OptionStatement) node()          {}
func (*BuiltinStatement) node()         {}
func (*TestStatement) node()            {}
func (*ExpressionStatement) node()      {}
func (*ReturnStatement) node()          {}
func (*MemberAssignment) node()         {}
func (*NativeVariableAssignment) node() {}

func (*StringExpression) node()      {}
func (*ArrayExpression) node()       {}
func (*DictExpression) node()        {}
func (*FunctionExpression) node()    {}
func (*BinaryExpression) node()      {}
func (*CallExpression) node()        {}
func (*ConditionalExpression) node() {}
func (*IdentifierExpression) node()  {}
func (*LogicalExpression) node()     {}
func (*MemberExpression) node()      {}
func (*IndexExpression) node()       {}
func (*ObjectExpression) node()      {}
func (*UnaryExpression) node()       {}

func (*Identifier) node() {}
func (*Property) node()   {}

func (*TextPart) node()         {}
func (*InterpolatedPart) node() {}

func (*FunctionParameters) node() {}
func (*FunctionParameter) node()  {}

func (*BooleanLiteral) node()         {}
func (*DateTimeLiteral) node()        {}
func (*DurationLiteral) node()        {}
func (*FloatLiteral) node()           {}
func (*IntegerLiteral) node()         {}
func (*StringLiteral) node()          {}
func (*RegexpLiteral) node()          {}
func (*UnsignedIntegerLiteral) node() {}

type Statement interface {
	Node
	stmt()
}

func (*OptionStatement) stmt()          {}
func (*BuiltinStatement) stmt()         {}
func (*TestStatement) stmt()            {}
func (*ExpressionStatement) stmt()      {}
func (*ReturnStatement) stmt()          {}
func (*NativeVariableAssignment) stmt() {}
func (*MemberAssignment) stmt()         {}

type Assignment interface {
	Statement
	assignment()
}

func (*MemberAssignment) assignment()         {}
func (*NativeVariableAssignment) assignment() {}

type Expression interface {
	Node
	expression()
	TypeOf() MonoType
}

func (*StringExpression) expression()       {}
func (*ArrayExpression) expression()        {}
func (*DictExpression) expression()         {}
func (*BinaryExpression) expression()       {}
func (*BooleanLiteral) expression()         {}
func (*CallExpression) expression()         {}
func (*ConditionalExpression) expression()  {}
func (*DateTimeLiteral) expression()        {}
func (*DurationLiteral) expression()        {}
func (*FloatLiteral) expression()           {}
func (*FunctionExpression) expression()     {}
func (*IdentifierExpression) expression()   {}
func (*IntegerLiteral) expression()         {}
func (*LogicalExpression) expression()      {}
func (*MemberExpression) expression()       {}
func (*IndexExpression) expression()        {}
func (*ObjectExpression) expression()       {}
func (*RegexpLiteral) expression()          {}
func (*StringLiteral) expression()          {}
func (*UnaryExpression) expression()        {}
func (*UnsignedIntegerLiteral) expression() {}

type Literal interface {
	Expression
	literal()
}

func (*BooleanLiteral) literal()         {}
func (*DateTimeLiteral) literal()        {}
func (*DurationLiteral) literal()        {}
func (*FloatLiteral) literal()           {}
func (*IntegerLiteral) literal()         {}
func (*RegexpLiteral) literal()          {}
func (*StringLiteral) literal()          {}
func (*UnsignedIntegerLiteral) literal() {}

type PropertyKey interface {
	Node
	Key() string
}

func (n *Identifier) Key() string {
	return n.Name
}
func (n *StringLiteral) Key() string {
	return n.Value
}

type Package struct {
	Loc

	Package string
	Files   []*File
}

func (*Package) NodeType() string { return "Package" }

func (p *Package) Copy() Node {
	if p == nil {
		return p
	}
	np := new(Package)
	*np = *p

	if len(p.Files) > 0 {
		np.Files = make([]*File, len(p.Files))
		for i, f := range p.Files {
			np.Files[i] = f.Copy().(*File)
		}
	}

	return np
}

type File struct {
	Loc

	Package *PackageClause
	Imports []*ImportDeclaration
	Body    []Statement
}

func (*File) NodeType() string { return "File" }

func (p *File) Copy() Node {
	if p == nil {
		return p
	}
	np := new(File)
	*np = *p

	if len(p.Body) > 0 {
		np.Body = make([]Statement, len(p.Body))
		for i, s := range p.Body {
			np.Body[i] = s.Copy().(Statement)
		}
	}

	return np
}

type PackageClause struct {
	Loc

	Name *Identifier
}

func (*PackageClause) NodeType() string { return "PackageClause" }

func (p *PackageClause) Copy() Node {
	if p == nil {
		return p
	}
	np := new(PackageClause)
	*np = *p

	np.Name = p.Name.Copy().(*Identifier)

	return np
}

type ImportDeclaration struct {
	Loc

	As   *Identifier
	Path *StringLiteral
}

func (*ImportDeclaration) NodeType() string { return "ImportDeclaration" }

func (d *ImportDeclaration) Copy() Node {
	if d == nil {
		return d
	}
	nd := new(ImportDeclaration)
	*nd = *d

	nd.As = d.As.Copy().(*Identifier)
	nd.Path = d.Path.Copy().(*StringLiteral)

	return nd
}

type Block struct {
	Loc

	Body []Statement
}

func (*Block) NodeType() string { return "Block" }

func (s *Block) ReturnStatement() *ReturnStatement {
	return s.Body[len(s.Body)-1].(*ReturnStatement)
}

func (s *Block) Copy() Node {
	if s == nil {
		return s
	}
	ns := new(Block)
	*ns = *s

	if len(s.Body) > 0 {
		ns.Body = make([]Statement, len(s.Body))
		for i, stmt := range s.Body {
			ns.Body[i] = stmt.Copy().(Statement)
		}
	}

	return ns
}

type OptionStatement struct {
	Loc

	Assignment Assignment
}

func (s *OptionStatement) NodeType() string { return "OptionStatement" }

func (s *OptionStatement) Copy() Node {
	if s == nil {
		return s
	}
	ns := new(OptionStatement)
	*ns = *s

	ns.Assignment = s.Assignment.Copy().(Assignment)

	return ns
}

type BuiltinStatement struct {
	Loc

	ID *Identifier
}

func (s *BuiltinStatement) NodeType() string { return "BuiltinStatement" }

func (s *BuiltinStatement) Copy() Node {
	if s == nil {
		return s
	}
	ns := new(BuiltinStatement)
	*ns = *s

	ns.ID = s.ID.Copy().(*Identifier)

	return ns
}

type TestStatement struct {
	Loc

	Assignment *NativeVariableAssignment
}

func (s *TestStatement) NodeType() string { return "TestStatement" }

func (s *TestStatement) Copy() Node {
	if s == nil {
		return s
	}
	ns := new(TestStatement)
	*ns = *s

	ns.Assignment = s.Assignment.Copy().(*NativeVariableAssignment)

	return ns
}

type ExpressionStatement struct {
	Loc

	Expression Expression
}

func (*ExpressionStatement) NodeType() string { return "ExpressionStatement" }

func (s *ExpressionStatement) Copy() Node {
	if s == nil {
		return s
	}
	ns := new(ExpressionStatement)
	*ns = *s

	ns.Expression = s.Expression.Copy().(Expression)

	return ns
}

type ReturnStatement struct {
	Loc

	Argument Expression
}

func (*ReturnStatement) NodeType() string { return "ReturnStatement" }

func (s *ReturnStatement) Copy() Node {
	if s == nil {
		return s
	}
	ns := new(ReturnStatement)
	*ns = *s

	ns.Argument = s.Argument.Copy().(Expression)

	return ns
}

type NativeVariableAssignment struct {
	Loc

	Identifier *Identifier
	Init       Expression

	Typ PolyType
}

func (*NativeVariableAssignment) NodeType() string { return "NativeVariableAssignment" }

func (s *NativeVariableAssignment) Copy() Node {
	if s == nil {
		return s
	}
	ns := new(NativeVariableAssignment)
	*ns = *s

	ns.Identifier = s.Identifier.Copy().(*Identifier)

	if s.Init != nil {
		ns.Init = s.Init.Copy().(Expression)
	}

	return ns
}

type MemberAssignment struct {
	Loc

	Member *MemberExpression
	Init   Expression
}

func (*MemberAssignment) NodeType() string { return "MemberAssignment" }

func (s *MemberAssignment) Copy() Node {
	if s == nil {
		return s
	}
	ns := new(MemberAssignment)
	*ns = *s

	if s.Member != nil {
		ns.Member = s.Member.Copy().(*MemberExpression)
	}
	if s.Init != nil {
		ns.Init = s.Init.Copy().(Expression)
	}

	return ns
}

type StringExpression struct {
	Loc
	Parts []StringExpressionPart
}

func (*StringExpression) NodeType() string {
	return "StringExpression"
}
func (e *StringExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(StringExpression)
	*ne = *e

	parts := make([]StringExpressionPart, len(e.Parts))
	for i, p := range e.Parts {
		parts[i] = p.Copy().(StringExpressionPart)
	}
	ne.Parts = parts
	return ne
}
func (e *StringExpression) TypeOf() MonoType {
	return BasicString
}

type StringExpressionPart interface {
	Node

	stringPart()
}

func (*TextPart) stringPart()         {}
func (*InterpolatedPart) stringPart() {}

type TextPart struct {
	Loc
	Value string
}

func (*TextPart) NodeType() string {
	return "TextPart"
}
func (p *TextPart) Copy() Node {
	if p == nil {
		return p
	}
	np := new(TextPart)
	*np = *p
	return np
}

type InterpolatedPart struct {
	Loc
	Expression Expression
}

func (*InterpolatedPart) NodeType() string {
	return "InterpolatedPart"
}
func (p *InterpolatedPart) Copy() Node {
	if p == nil {
		return p
	}
	np := new(InterpolatedPart)
	*np = *p

	if p.Expression != nil {
		np.Expression = p.Expression.Copy().(Expression)
	}
	return np
}

type ArrayExpression struct {
	Loc

	Elements []Expression

	Type MonoType
}

func (*ArrayExpression) NodeType() string { return "ArrayExpression" }

func (e *ArrayExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(ArrayExpression)
	*ne = *e

	if len(e.Elements) > 0 {
		ne.Elements = make([]Expression, len(e.Elements))
		for i, elem := range e.Elements {
			ne.Elements[i] = elem.Copy().(Expression)
		}
	}

	return ne
}
func (e *ArrayExpression) TypeOf() MonoType {
	return e.Type
}

type DictExpression struct {
	Loc

	Elements []struct {
		Key Expression
		Val Expression
	}

	Type MonoType
}

func (*DictExpression) NodeType() string { return "DictExpression" }

func (e *DictExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(DictExpression)
	*ne = *e

	if len(e.Elements) > 0 {
		ne.Elements = make([]struct {
			Key Expression
			Val Expression
		}, len(e.Elements))
		for i, elem := range e.Elements {
			ne.Elements[i] = struct {
				Key Expression
				Val Expression
			}{
				Key: elem.Key.Copy().(Expression),
				Val: elem.Val.Copy().(Expression),
			}
		}
	}

	return ne
}
func (e *DictExpression) TypeOf() MonoType {
	return e.Type
}

// FunctionExpression represents the definition of a function
type FunctionExpression struct {
	Loc

	Parameters *FunctionParameters
	Defaults   *ObjectExpression
	Block      *Block

	typ MonoType
}

func (*FunctionExpression) NodeType() string { return "FunctionExpression" }

func (e *FunctionExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(FunctionExpression)
	*ne = *e

	if e.Parameters != nil {
		ne.Parameters = e.Parameters.Copy().(*FunctionParameters)
	}
	if e.Defaults != nil {
		ne.Defaults = e.Defaults.Copy().(*ObjectExpression)
	}
	ne.Block = e.Block.Copy().(*Block)

	return ne
}
func (e *FunctionExpression) TypeOf() MonoType {
	return e.typ
}

// GetFunctionBodyExpression will return the return value expression from
// the function block. This will only return an expression if there
// is exactly one expression in the block. It will return false
// as the second argument if the statement is more complex.
func (e *FunctionExpression) GetFunctionBodyExpression() (Expression, bool) {
	if len(e.Block.Body) != 1 {
		return nil, false
	}

	returnExpr, ok := e.Block.Body[0].(*ReturnStatement)
	if !ok {
		return nil, false
	}
	return returnExpr.Argument, true
}

// FunctionParameters represents the list of function parameters and which if any parameter is the pipe parameter.
type FunctionParameters struct {
	Loc

	List []*FunctionParameter
	Pipe *Identifier
}

func (*FunctionParameters) NodeType() string { return "FunctionParameters" }

func (p *FunctionParameters) Copy() Node {
	if p == nil {
		return p
	}
	np := new(FunctionParameters)
	*np = *p

	if len(p.List) > 0 {
		np.List = make([]*FunctionParameter, len(p.List))
		for i, k := range p.List {
			np.List[i] = k.Copy().(*FunctionParameter)
		}
	}
	if p.Pipe != nil {
		np.Pipe = p.Pipe.Copy().(*Identifier)
	}

	return np
}

// FunctionParameter represents a function parameter.
type FunctionParameter struct {
	Loc

	Key *Identifier
}

func (*FunctionParameter) NodeType() string { return "FunctionParameter" }

func (p *FunctionParameter) Copy() Node {
	if p == nil {
		return p
	}
	np := new(FunctionParameter)
	*np = *p

	np.Key = p.Key.Copy().(*Identifier)

	return np
}

type BinaryExpression struct {
	Loc

	Operator ast.OperatorKind
	Left     Expression
	Right    Expression

	typ MonoType
}

func (*BinaryExpression) NodeType() string { return "BinaryExpression" }

func (e *BinaryExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(BinaryExpression)
	*ne = *e

	ne.Left = e.Left.Copy().(Expression)
	ne.Right = e.Right.Copy().(Expression)

	return ne
}
func (e *BinaryExpression) TypeOf() MonoType {
	return e.typ
}

type CallExpression struct {
	Loc

	Callee    Expression
	Arguments *ObjectExpression
	Pipe      Expression

	typ MonoType
}

func (*CallExpression) NodeType() string { return "CallExpression" }

func (e *CallExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(CallExpression)
	*ne = *e

	ne.Callee = e.Callee.Copy().(Expression)
	ne.Arguments = e.Arguments.Copy().(*ObjectExpression)
	if e.Pipe != nil {
		ne.Pipe = e.Pipe.Copy().(Expression)
	}

	return ne
}
func (e *CallExpression) TypeOf() MonoType {
	return e.typ
}

type ConditionalExpression struct {
	Loc

	Test       Expression
	Alternate  Expression
	Consequent Expression
}

func (*ConditionalExpression) NodeType() string { return "ConditionalExpression" }

func (e *ConditionalExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(ConditionalExpression)
	*ne = *e

	ne.Test = e.Test.Copy().(Expression)
	ne.Alternate = e.Alternate.Copy().(Expression)
	ne.Consequent = e.Consequent.Copy().(Expression)

	return ne
}
func (e *ConditionalExpression) TypeOf() MonoType {
	return e.Alternate.TypeOf()
}

type LogicalExpression struct {
	Loc

	Operator ast.LogicalOperatorKind
	Left     Expression
	Right    Expression
}

func (*LogicalExpression) NodeType() string { return "LogicalExpression" }

func (e *LogicalExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(LogicalExpression)
	*ne = *e

	ne.Left = e.Left.Copy().(Expression)
	ne.Right = e.Right.Copy().(Expression)

	return ne
}
func (e *LogicalExpression) TypeOf() MonoType {
	return BasicBool
}

type MemberExpression struct {
	Loc

	Object   Expression
	Property string

	typ MonoType
}

func (*MemberExpression) NodeType() string { return "MemberExpression" }

func (e *MemberExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(MemberExpression)
	*ne = *e

	ne.Object = e.Object.Copy().(Expression)

	return ne
}
func (e *MemberExpression) TypeOf() MonoType {
	return e.typ
}

type IndexExpression struct {
	Loc

	Array Expression
	Index Expression

	typ MonoType
}

func (*IndexExpression) NodeType() string { return "IndexExpression" }

func (e *IndexExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(IndexExpression)
	*ne = *e
	ne.Array = e.Array.Copy().(Expression)
	ne.Index = e.Index.Copy().(Expression)
	return ne
}
func (e *IndexExpression) TypeOf() MonoType {
	return e.typ
}

type ObjectExpression struct {
	Loc

	With       *IdentifierExpression
	Properties []*Property

	typ MonoType
}

func (*ObjectExpression) NodeType() string { return "ObjectExpression" }

func (e *ObjectExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(ObjectExpression)
	*ne = *e

	ne.With = e.With.Copy().(*IdentifierExpression)

	if len(e.Properties) > 0 {
		ne.Properties = make([]*Property, len(e.Properties))
		for i, prop := range e.Properties {
			ne.Properties[i] = prop.Copy().(*Property)
		}
	}

	return ne
}
func (e *ObjectExpression) TypeOf() MonoType {
	return e.typ
}

type UnaryExpression struct {
	Loc

	Operator ast.OperatorKind
	Argument Expression

	typ MonoType
}

func (*UnaryExpression) NodeType() string { return "UnaryExpression" }

func (e *UnaryExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(UnaryExpression)
	*ne = *e

	ne.Argument = e.Argument.Copy().(Expression)

	return ne
}
func (e *UnaryExpression) TypeOf() MonoType {
	return e.typ
}

type Property struct {
	Loc

	Key   PropertyKey
	Value Expression
}

func (*Property) NodeType() string { return "Property" }

func (p *Property) Copy() Node {
	if p == nil {
		return p
	}
	np := new(Property)
	*np = *p

	np.Value = p.Value.Copy().(Expression)

	return np
}

type IdentifierExpression struct {
	Loc

	Name string

	typ MonoType
}

func (*IdentifierExpression) NodeType() string { return "IdentifierExpression" }

func (e *IdentifierExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(IdentifierExpression)
	*ne = *e

	return ne
}
func (e *IdentifierExpression) TypeOf() MonoType {
	return e.typ
}

type Identifier struct {
	Loc

	Name string
}

func (*Identifier) NodeType() string { return "Identifier" }

func (i *Identifier) Copy() Node {
	if i == nil {
		return i
	}
	ni := new(Identifier)
	*ni = *i

	return ni
}

type BooleanLiteral struct {
	Loc
	Value bool
}

func (*BooleanLiteral) NodeType() string { return "BooleanLiteral" }

func (l *BooleanLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(BooleanLiteral)
	*nl = *l

	return nl
}
func (e *BooleanLiteral) TypeOf() MonoType {
	return BasicBool
}

type DateTimeLiteral struct {
	Loc
	Value time.Time
}

func (*DateTimeLiteral) NodeType() string { return "DateTimeLiteral" }

func (l *DateTimeLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(DateTimeLiteral)
	*nl = *l

	return nl
}
func (e *DateTimeLiteral) TypeOf() MonoType {
	return BasicTime
}

type DurationLiteral struct {
	Loc
	Values []ast.Duration
}

func (*DurationLiteral) NodeType() string { return "DurationLiteral" }

func (l *DurationLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(DurationLiteral)
	*nl = *l

	return nl
}
func (e *DurationLiteral) TypeOf() MonoType {
	return BasicDuration
}

type IntegerLiteral struct {
	Loc
	Value int64
}

func (*IntegerLiteral) NodeType() string { return "IntegerLiteral" }

func (l *IntegerLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(IntegerLiteral)
	*nl = *l

	return nl
}
func (e *IntegerLiteral) TypeOf() MonoType {
	return BasicInt
}

type FloatLiteral struct {
	Loc
	Value float64
}

func (*FloatLiteral) NodeType() string { return "FloatLiteral" }

func (l *FloatLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(FloatLiteral)
	*nl = *l

	return nl
}
func (e *FloatLiteral) TypeOf() MonoType {
	return BasicFloat
}

type RegexpLiteral struct {
	Loc
	Value *regexp.Regexp
}

func (*RegexpLiteral) NodeType() string { return "RegexpLiteral" }

func (l *RegexpLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(RegexpLiteral)
	*nl = *l

	nl.Value = l.Value

	return nl
}
func (e *RegexpLiteral) TypeOf() MonoType {
	return BasicRegexp
}

type StringLiteral struct {
	Loc
	Value string
}

func (*StringLiteral) NodeType() string { return "StringLiteral" }

func (l *StringLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(StringLiteral)
	*nl = *l

	return nl
}
func (e *StringLiteral) TypeOf() MonoType {
	return BasicString
}

type UnsignedIntegerLiteral struct {
	Loc
	Value uint64
}

func (*UnsignedIntegerLiteral) NodeType() string { return "UnsignedIntegerLiteral" }

func (l *UnsignedIntegerLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(UnsignedIntegerLiteral)
	*nl = *l

	return nl
}
func (e *UnsignedIntegerLiteral) TypeOf() MonoType {
	return BasicUint
}
