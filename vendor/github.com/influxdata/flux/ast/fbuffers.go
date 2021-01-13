package ast

import (
	"fmt"
	"regexp"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/influxdata/flux/ast/internal/fbast"
)

// DeserializeFromFlatBuffer takes the given byte slice
// that is an AST flatbuffer and deserializes it to an AST package.
func DeserializeFromFlatBuffer(buf []byte) *Package {
	pkg := Package{}
	return pkg.FromBuf(buf)
}

func (p *Position) FromBuf(buf *fbast.Position) {
	p.Line = int(buf.Line())
	p.Column = int(buf.Column())
}

func (l SourceLocation) FromBuf(buf *fbast.SourceLocation) *SourceLocation {
	l.File = string(buf.File())
	l.Start.FromBuf(buf.Start(nil))
	l.End.FromBuf(buf.End(nil))
	l.Source = string(buf.Source())
	return &l
}

func (b *BaseNode) FromBuf(buf *fbast.BaseNode) {
	if buf == nil {
		return
	}
	b.Loc = SourceLocation{}.FromBuf(buf.Loc(nil))
	if !b.Loc.IsValid() {
		b.Loc = nil
	}
	if buf.ErrorsLength() != 0 {
		b.Errors = make([]Error, buf.ErrorsLength())
		for i := 0; i < buf.ErrorsLength(); i++ {
			b.Errors[i] = Error{string(buf.Errors(i))}
		}
	}
}

func (t TypeExpression) FromBuf(buf *fbast.TypeExpression) *TypeExpression {
	t.BaseNode.FromBuf(buf.BaseNode(nil))
	t.Ty = DecodeMonoType(newFBTable(buf.Monotype, &t.BaseNode), buf.MonotypeType())
	for i := 0; i < buf.ConstraintsLength(); i++ {
		if c := new(fbast.TypeConstraint); !buf.Constraints(c, i) {
			t.BaseNode.Errors = append(t.BaseNode.Errors, Error{
				fmt.Sprintf("Encountered error in deserializing TypeExpression.Constraints[%d]", i),
			})
		} else {
			t.Constraints = append(t.Constraints, TypeConstraint{}.FromBuf(c))
		}
	}
	return &t
}

func (c TypeConstraint) FromBuf(buf *fbast.TypeConstraint) *TypeConstraint {
	c.BaseNode.FromBuf(buf.BaseNode(nil))
	c.Tvar = Identifier{}.FromBuf(buf.Tvar(nil))
	for i := 0; i < buf.KindsLength(); i++ {
		if id := new(fbast.Identifier); !buf.Kinds(id, i) {
			c.BaseNode.Errors = append(c.BaseNode.Errors, Error{
				fmt.Sprintf("Encountered error in deserializing TypeConstraint.Kinds[%d]", i),
			})
		} else {
			c.Kinds = append(c.Kinds, Identifier{}.FromBuf(id))
		}
	}
	return &c
}

func (t NamedType) FromBuf(buf *fbast.NamedType) *NamedType {
	t.BaseNode.FromBuf(buf.BaseNode(nil))
	t.ID = Identifier{}.FromBuf(buf.Id(nil))
	return &t
}

func (t TvarType) FromBuf(buf *fbast.TvarType) *TvarType {
	t.BaseNode.FromBuf(buf.BaseNode(nil))
	t.ID = Identifier{}.FromBuf(buf.Id(nil))
	return &t
}

func (t ArrayType) FromBuf(buf *fbast.ArrayType) *ArrayType {
	t.BaseNode.FromBuf(buf.BaseNode(nil))
	t.ElementType = DecodeMonoType(newFBTable(buf.Element, &t.BaseNode), buf.ElementType())
	return &t
}

func (t DictType) FromBuf(buf *fbast.DictType) *DictType {
	t.BaseNode.FromBuf(buf.BaseNode(nil))
	t.KeyType = DecodeMonoType(newFBTable(buf.Key, &t.BaseNode), buf.KeyType())
	t.ValueType = DecodeMonoType(newFBTable(buf.Val, &t.BaseNode), buf.ValType())
	return &t
}

func (t RecordType) FromBuf(buf *fbast.RecordType) *RecordType {
	t.BaseNode.FromBuf(buf.BaseNode(nil))
	t.Tvar = Identifier{}.FromBuf(buf.Tvar(nil))
	for i := 0; i < buf.PropertiesLength(); i++ {
		if p := new(fbast.PropertyType); !buf.Properties(p, i) {
			t.BaseNode.Errors = append(t.BaseNode.Errors, Error{
				fmt.Sprintf("Encountered error in deserializing RecordType.Properties[%d]", i),
			})
		} else {
			t.Properties = append(t.Properties, PropertyType{}.FromBuf(p))
		}
	}
	return &t
}

func (p PropertyType) FromBuf(buf *fbast.PropertyType) *PropertyType {
	p.BaseNode.FromBuf(buf.BaseNode(nil))
	p.Name = Identifier{}.FromBuf(buf.Id(nil))
	p.Ty = DecodeMonoType(newFBTable(buf.Monotype, &p.BaseNode), buf.MonotypeType())
	return &p
}

func (t FunctionType) FromBuf(buf *fbast.FunctionType) *FunctionType {
	t.BaseNode.FromBuf(buf.BaseNode(nil))
	t.Return = DecodeMonoType(newFBTable(buf.Monotype, &t.BaseNode), buf.MonotypeType())
	for i := 0; i < buf.ParametersLength(); i++ {
		if p := new(fbast.ParameterType); !buf.Parameters(p, i) {
			t.BaseNode.Errors = append(t.BaseNode.Errors, Error{
				fmt.Sprintf("Encountered error in deserializing FunctionType.Parameters[%d]", i),
			})
		} else {
			t.Parameters = append(t.Parameters, ParameterType{}.FromBuf(p))
		}
	}
	return &t
}

func (p ParameterType) FromBuf(buf *fbast.ParameterType) *ParameterType {
	p.BaseNode.FromBuf(buf.BaseNode(nil))
	p.Name = Identifier{}.FromBuf(buf.Id(nil))
	p.Ty = DecodeMonoType(newFBTable(buf.Monotype, &p.BaseNode), buf.MonotypeType())
	p.Kind = paramKindMap[buf.Kind()]
	return &p
}

func newFBTable(f unionTableWriterFn, base *BaseNode) *flatbuffers.Table {
	t := new(flatbuffers.Table)
	if !f(t) {
		base.Errors = append(base.Errors, Error{fmt.Sprint("serialization error")})
	}
	return t
}

func DecodeMonoType(t *flatbuffers.Table, ty byte) MonoType {
	switch ty {
	case fbast.MonoTypeNamedType:
		b := new(fbast.NamedType)
		b.Init(t.Bytes, t.Pos)
		return NamedType{}.FromBuf(b)
	case fbast.MonoTypeTvarType:
		b := new(fbast.TvarType)
		b.Init(t.Bytes, t.Pos)
		return TvarType{}.FromBuf(b)
	case fbast.MonoTypeArrayType:
		b := new(fbast.ArrayType)
		b.Init(t.Bytes, t.Pos)
		return ArrayType{}.FromBuf(b)
	case fbast.MonoTypeDictType:
		b := new(fbast.DictType)
		b.Init(t.Bytes, t.Pos)
		return DictType{}.FromBuf(b)
	case fbast.MonoTypeRecordType:
		b := new(fbast.RecordType)
		b.Init(t.Bytes, t.Pos)
		return RecordType{}.FromBuf(b)
	case fbast.MonoTypeFunctionType:
		b := new(fbast.FunctionType)
		b.Init(t.Bytes, t.Pos)
		return FunctionType{}.FromBuf(b)
	default:
		// TODO: Use bad monotype to store errors.
		return nil
	}
}

func (p Package) FromBuf(buf []byte) *Package {
	fbp := fbast.GetRootAsPackage(buf, 0)
	p.BaseNode.FromBuf(fbp.BaseNode(nil))
	p.Path = string(fbp.Path())
	p.Package = string(fbp.Package())
	p.Files = make([]*File, fbp.FilesLength())
	for i := 0; i < fbp.FilesLength(); i++ {
		fbf := new(fbast.File)
		if !fbp.Files(fbf, i) {
			p.BaseNode.Errors = append(p.BaseNode.Errors,
				Error{fmt.Sprintf("Encountered error in deserializing Package.Files[%d]", i)})
		} else {
			p.Files[i] = File{}.FromBuf(fbf)
		}
	}
	return &p
}

func (f File) FromBuf(buf *fbast.File) *File {
	f.BaseNode.FromBuf(buf.BaseNode(nil))
	f.Name = string(buf.Name())
	f.Metadata = string(buf.Metadata())
	f.Package = PackageClause{}.FromBuf(buf.Package(nil))
	if buf.ImportsLength() > 0 {
		f.Imports = make([]*ImportDeclaration, buf.ImportsLength())
		for i := 0; i < buf.ImportsLength(); i++ {
			fbd := new(fbast.ImportDeclaration)
			if !buf.Imports(fbd, i) {
				f.BaseNode.Errors = append(f.BaseNode.Errors,
					Error{fmt.Sprintf("Encountered error in deserializing File.Imports[%d]", i)})
			} else {
				f.Imports[i] = ImportDeclaration{}.FromBuf(fbd)
			}
		}
	}
	if buf.BodyLength() > 0 {
		var err []Error
		f.Body, err = statementArrayFromBuf(buf.BodyLength(), buf.Body, "File.Body")
		if len(err) > 0 {
			f.BaseNode.Errors = append(f.BaseNode.Errors, err...)
		}
	}
	return &f
}

func (c PackageClause) FromBuf(buf *fbast.PackageClause) *PackageClause {
	if buf == nil {
		return nil
	}
	c.BaseNode.FromBuf(buf.BaseNode(nil))
	c.Name = Identifier{}.FromBuf(buf.Name(nil))
	return &c
}

func (d ImportDeclaration) FromBuf(buf *fbast.ImportDeclaration) *ImportDeclaration {
	d.BaseNode.FromBuf(buf.BaseNode(nil))
	d.As = Identifier{}.FromBuf(buf.As(nil))
	d.Path = StringLiteral{}.FromBuf(buf.Path(nil))
	return &d
}

func (s Block) FromBuf(buf *fbast.Block) *Block {
	s.BaseNode.FromBuf(buf.BaseNode(nil))
	var err []Error
	s.Body, err = statementArrayFromBuf(buf.BodyLength(), buf.Body, "Block.Body")
	if len(err) > 0 {
		s.BaseNode.Errors = append(s.BaseNode.Errors, err...)
	}
	return &s
}

func (s BadStatement) FromBuf(buf *fbast.BadStatement) *BadStatement {
	s.BaseNode.FromBuf(buf.BaseNode(nil))
	s.Text = string(buf.Text())
	return &s
}

func (s ExpressionStatement) FromBuf(buf *fbast.ExpressionStatement) *ExpressionStatement {
	s.BaseNode.FromBuf(buf.BaseNode(nil))
	s.Expression = exprFromBuf("ExpressionStatement.Expression", s.BaseNode, buf.Expression, buf.ExpressionType())
	return &s
}

func (s ReturnStatement) FromBuf(buf *fbast.ReturnStatement) *ReturnStatement {
	s.BaseNode.FromBuf(buf.BaseNode(nil))
	s.Argument = exprFromBuf("ReturnStatement.Argument", s.BaseNode, buf.Argument, buf.ArgumentType())
	return &s
}

func (s OptionStatement) FromBuf(buf *fbast.OptionStatement) *OptionStatement {
	s.BaseNode.FromBuf(buf.BaseNode(nil))
	s.Assignment = assignmentFromBuf("OptionStatement.Assignment", s.BaseNode, buf.Assignment, buf.AssignmentType())
	return &s
}

func (s BuiltinStatement) FromBuf(buf *fbast.BuiltinStatement) *BuiltinStatement {
	s.BaseNode.FromBuf(buf.BaseNode(nil))
	s.ID = Identifier{}.FromBuf(buf.Id(nil))
	return &s
}

func (s TestStatement) FromBuf(buf *fbast.TestStatement) *TestStatement {
	s.BaseNode.FromBuf(buf.BaseNode(nil))
	s.Assignment = assignmentFromBuf("TestStatement.Assignment",
		s.BaseNode, buf.Assignment, buf.AssignmentType()).(*VariableAssignment)
	return &s
}

func (s TestCaseStatement) FromBuf(buf *fbast.TestCaseStatement) *TestCaseStatement {
	s.BaseNode.FromBuf(buf.BaseNode(nil))
	s.ID = Identifier{}.FromBuf(buf.Id(nil))
	s.Block = Block{}.FromBuf(buf.Block(nil))
	return &s
}

func (d VariableAssignment) FromBuf(buf *fbast.VariableAssignment) *VariableAssignment {
	d.BaseNode.FromBuf(buf.BaseNode(nil))
	d.ID = Identifier{}.FromBuf(buf.Id(nil))
	d.Init = exprFromBuf("VariableAssignment.Init", d.BaseNode, buf.Init_, buf.Init_type())
	return &d
}

func (a MemberAssignment) FromBuf(buf *fbast.MemberAssignment) *MemberAssignment {
	a.BaseNode.FromBuf(buf.BaseNode(nil))
	a.Member = MemberExpression{}.FromBuf(buf.Member(nil))
	a.Init = exprFromBuf("MemberAssignment.Init", a.BaseNode, buf.Init_, buf.Init_type())
	return &a
}

func (e StringExpression) FromBuf(buf *fbast.StringExpression) *StringExpression {
	e.BaseNode.FromBuf(buf.BaseNode(nil))
	if buf.PartsLength() > 0 {
		e.Parts = make([]StringExpressionPart, buf.PartsLength())
		for i := 0; i < buf.PartsLength(); i++ {
			fbp := new(fbast.StringExpressionPart)
			if !buf.Parts(fbp, i) {
				e.BaseNode.Errors = append(e.BaseNode.Errors,
					Error{fmt.Sprintf("Encountered error in deserializing StringExpression.Parts[%d]", i)})
			} else if fbp.TextValue() != nil {
				e.Parts[i] = TextPart{}.FromBuf(fbp)
			} else {
				e.Parts[i] = InterpolatedPart{}.FromBuf(fbp)
			}
		}
	}
	return &e
}

func (p TextPart) FromBuf(buf *fbast.StringExpressionPart) *TextPart {
	p.BaseNode.FromBuf(buf.BaseNode(nil))
	p.Value = string(buf.TextValue())
	return &p
}

func (p InterpolatedPart) FromBuf(buf *fbast.StringExpressionPart) *InterpolatedPart {
	p.BaseNode.FromBuf(buf.BaseNode(nil))
	p.Expression = exprFromBuf("InterpolatedPart.Expression", p.BaseNode,
		buf.InterpolatedExpression, buf.InterpolatedExpressionType())
	return &p
}

func (e ParenExpression) FromBuf(buf *fbast.ParenExpression) *ParenExpression {
	e.BaseNode.FromBuf(buf.BaseNode(nil))
	e.Expression = exprFromBuf("ParenExpression.Expression", e.BaseNode, buf.Expression, buf.ExpressionType())
	return &e
}

func (e CallExpression) FromBuf(buf *fbast.CallExpression) *CallExpression {
	e.BaseNode.FromBuf(buf.BaseNode(nil))
	e.Callee = exprFromBuf("CallExpression.Callee", e.BaseNode, buf.Callee, buf.CalleeType())
	arg := buf.Arguments(nil)
	if arg != nil {
		e.Arguments = []Expression{ObjectExpression{}.FromBuf(arg)}
	}
	return &e
}

func (e PipeExpression) FromBuf(buf *fbast.PipeExpression) *PipeExpression {
	e.BaseNode.FromBuf(buf.BaseNode(nil))
	e.Argument = exprFromBuf("PipeExpression.Argument", e.BaseNode, buf.Argument, buf.ArgumentType())
	e.Call = CallExpression{}.FromBuf(buf.Call(nil))
	return &e
}

func (e MemberExpression) FromBuf(buf *fbast.MemberExpression) *MemberExpression {
	e.BaseNode.FromBuf(buf.BaseNode(nil))
	e.Object = exprFromBuf("MemberExpression.Object", e.BaseNode, buf.Object, buf.ObjectType())
	e.Property = propertyKeyFromBuf("MemberExpression.Property", e.BaseNode, buf.Property, buf.PropertyType())
	return &e
}

func (e IndexExpression) FromBuf(buf *fbast.IndexExpression) *IndexExpression {
	e.BaseNode.FromBuf(buf.BaseNode(nil))
	e.Array = exprFromBuf("IndexExpression.Array", e.BaseNode, buf.Array, buf.ArrayType())
	e.Index = exprFromBuf("IndexExpression.Index", e.BaseNode, buf.Index, buf.IndexType())
	return &e
}

func (e FunctionExpression) FromBuf(buf *fbast.FunctionExpression) *FunctionExpression {
	e.BaseNode.FromBuf(buf.BaseNode(nil))
	e.Params = make([]*Property, 0, buf.ParamsLength())
	for i := 0; i < buf.ParamsLength(); i++ {
		fbp := new(fbast.Property)
		if !buf.Params(fbp, i) {
			e.BaseNode.Errors = append(e.BaseNode.Errors,
				Error{fmt.Sprintf("Encountered error in deserializing FunctionExpression.Params[%d]", i)})
		} else {
			e.Params = append(e.Params, Property{}.FromBuf(fbp))
		}
	}
	t := new(flatbuffers.Table)
	if buf.Body(t) {
		switch buf.BodyType() {
		case fbast.ExpressionOrBlockBlock:
			b := new(fbast.Block)
			b.Init(t.Bytes, t.Pos)
			e.Body = Block{}.FromBuf(b)
		case fbast.ExpressionOrBlockWrappedExpression:
			we := new(fbast.WrappedExpression)
			we.Init(t.Bytes, t.Pos)
			e.Body = exprFromBuf("FunctionExpression.Body", e.BaseNode, we.Expr, we.ExprType())
		default:
			e.BaseNode.Errors = append(e.BaseNode.Errors,
				Error{"Encountered error in deserializing FunctionExpression.Body"})
		}
	}
	return &e
}

func (e BinaryExpression) FromBuf(buf *fbast.BinaryExpression) *BinaryExpression {
	e.BaseNode.FromBuf(buf.BaseNode(nil))
	e.Operator = opMap[buf.Operator()]
	e.Left = exprFromBuf("BinaryExpression.Left", e.BaseNode, buf.Left, buf.LeftType())
	e.Right = exprFromBuf("BinaryExpression.Right", e.BaseNode, buf.Right, buf.RightType())
	return &e
}

func (e UnaryExpression) FromBuf(buf *fbast.UnaryExpression) *UnaryExpression {
	e.BaseNode.FromBuf(buf.BaseNode(nil))
	e.Operator = opMap[buf.Operator()]
	e.Argument = exprFromBuf("UnaryExpression.Argument", e.BaseNode, buf.Argument, buf.ArgumentType())
	return &e
}

func (e LogicalExpression) FromBuf(buf *fbast.LogicalExpression) *LogicalExpression {
	e.BaseNode.FromBuf(buf.BaseNode(nil))
	e.Operator = logOpMap[buf.Operator()]
	e.Left = exprFromBuf("LogicalExpression.Left", e.BaseNode, buf.Left, buf.LeftType())
	e.Right = exprFromBuf("LogicalExpression.Right", e.BaseNode, buf.Right, buf.RightType())
	return &e
}

func (e ArrayExpression) FromBuf(buf *fbast.ArrayExpression) *ArrayExpression {
	e.BaseNode.FromBuf(buf.BaseNode(nil))
	var err []Error
	e.Elements, err = exprArrayFromBuf(buf.ElementsLength(), buf.Elements, "ArrayExpression.Elements")
	if len(err) > 0 {
		e.BaseNode.Errors = append(e.BaseNode.Errors, err...)
	}
	return &e
}

func (e ObjectExpression) FromBuf(buf *fbast.ObjectExpression) *ObjectExpression {
	e.BaseNode.FromBuf(buf.BaseNode(nil))
	e.With = Identifier{}.FromBuf(buf.With(nil))
	e.Properties = make([]*Property, buf.PropertiesLength())
	for i := 0; i < buf.PropertiesLength(); i++ {
		fbp := new(fbast.Property)
		if !buf.Properties(fbp, i) {
			e.BaseNode.Errors = append(e.BaseNode.Errors,
				Error{fmt.Sprintf("Encountered error in deserializing ObjectExpression.Properties[%d]", i)})
		} else {
			e.Properties[i] = Property{}.FromBuf(fbp)
		}
	}
	return &e
}

func (e ConditionalExpression) FromBuf(buf *fbast.ConditionalExpression) *ConditionalExpression {
	e.BaseNode.FromBuf(buf.BaseNode(nil))
	e.Test = exprFromBuf("ConditionalExpression.Test", e.BaseNode, buf.Test, buf.TestType())
	e.Consequent = exprFromBuf("ConditionalExpression.Consequent", e.BaseNode, buf.Consequent, buf.ConsequentType())
	e.Alternate = exprFromBuf("ConditionalExpression.Alternate", e.BaseNode, buf.Alternate, buf.AlternateType())
	return &e
}

func (p Property) FromBuf(buf *fbast.Property) *Property {
	p.BaseNode.FromBuf(buf.BaseNode(nil))
	// deserialize key
	p.Key = propertyKeyFromBuf("Property.Key", p.BaseNode, buf.Key, buf.KeyType())
	// deserialize value
	p.Value = exprFromBuf("Property.Value", p.BaseNode, buf.Value, buf.ValueType())
	return &p
}

func (i Identifier) FromBuf(buf *fbast.Identifier) *Identifier {
	if buf == nil {
		return nil
	}
	i.BaseNode.FromBuf(buf.BaseNode(nil))
	i.Name = string(buf.Name())
	return &i
}

func (p PipeLiteral) FromBuf(buf *fbast.PipeLiteral) *PipeLiteral {
	p.BaseNode.FromBuf(buf.BaseNode(nil))
	return &p
}

func (l StringLiteral) FromBuf(buf *fbast.StringLiteral) *StringLiteral {
	l.BaseNode.FromBuf(buf.BaseNode(nil))
	l.Value = string(buf.Value())
	return &l
}

func (l BooleanLiteral) FromBuf(buf *fbast.BooleanLiteral) *BooleanLiteral {
	l.BaseNode.FromBuf(buf.BaseNode(nil))
	l.Value = buf.Value()
	return &l
}

func (l FloatLiteral) FromBuf(buf *fbast.FloatLiteral) *FloatLiteral {
	l.BaseNode.FromBuf(buf.BaseNode(nil))
	l.Value = buf.Value()
	return &l
}

func (l IntegerLiteral) FromBuf(buf *fbast.IntegerLiteral) *IntegerLiteral {
	l.BaseNode.FromBuf(buf.BaseNode(nil))
	l.Value = buf.Value()
	return &l
}

func (l UnsignedIntegerLiteral) FromBuf(buf *fbast.UnsignedIntegerLiteral) *UnsignedIntegerLiteral {
	l.BaseNode.FromBuf(buf.BaseNode(nil))
	l.Value = buf.Value()
	return &l
}

func (l RegexpLiteral) FromBuf(buf *fbast.RegexpLiteral) *RegexpLiteral {
	l.BaseNode.FromBuf(buf.BaseNode(nil))
	var err error
	if l.Value, err = regexp.Compile(string(buf.Value())); err != nil {
		l.BaseNode.Errors = append(l.BaseNode.Errors,
			Error{fmt.Sprintf("Encountered error in deserializing RegexpLiteral.Values: %s", err.Error())})
	}
	return &l
}

func (d Duration) FromBuf(buf *fbast.Duration) Duration {
	d.Magnitude = buf.Magnitude()
	d.Unit = fbast.EnumNamesTimeUnit[buf.Unit()]
	return d
}

func (l DurationLiteral) FromBuf(buf *fbast.DurationLiteral) *DurationLiteral {
	l.BaseNode.FromBuf(buf.BaseNode(nil))
	l.Values = make([]Duration, buf.ValuesLength())
	for i := 0; i < buf.ValuesLength(); i++ {
		d := new(fbast.Duration)
		if !buf.Values(d, i) {
			l.BaseNode.Errors = append(l.BaseNode.Errors,
				Error{fmt.Sprintf("Encountered error in deserializing DurationLiteral.Values[%d]", i)})
		} else {
			l.Values[i] = Duration{}.FromBuf(d)
		}
	}
	return &l
}

func (l DateTimeLiteral) FromBuf(buf *fbast.DateTimeLiteral) *DateTimeLiteral {
	l.BaseNode.FromBuf(buf.BaseNode(nil))
	l.Value = time.Unix(buf.Secs(), int64(buf.Nsecs())).In(time.FixedZone("DateTimeLiteral offset", int(buf.Offset())))
	return &l
}

type stmtGetterFn func(obj *fbast.WrappedStatement, j int) bool

func statementArrayFromBuf(len int, g stmtGetterFn, arrName string) ([]Statement, []Error) {
	s := make([]Statement, len)
	err := make([]Error, 0)
	for i := 0; i < len; i++ {
		fbs := new(fbast.WrappedStatement)
		t := new(flatbuffers.Table)
		if !g(fbs, i) || !fbs.Statement(t) || fbs.StatementType() == fbast.StatementNONE {
			err = append(err, Error{fmt.Sprintf("Encountered error in deserializing %s[%d]", arrName, i)})
		} else {
			s[i] = statementFromBuf(t, fbs.StatementType())
		}
	}
	return s, err
}

func statementFromBuf(t *flatbuffers.Table, stype fbast.Statement) Statement {
	switch stype {
	case fbast.StatementBadStatement:
		b := new(fbast.BadStatement)
		b.Init(t.Bytes, t.Pos)
		return BadStatement{}.FromBuf(b)
	case fbast.StatementVariableAssignment:
		a := new(fbast.VariableAssignment)
		a.Init(t.Bytes, t.Pos)
		return VariableAssignment{}.FromBuf(a)
	case fbast.StatementMemberAssignment:
		a := new(fbast.MemberAssignment)
		a.Init(t.Bytes, t.Pos)
		return MemberAssignment{}.FromBuf(a)
	case fbast.StatementExpressionStatement:
		e := new(fbast.ExpressionStatement)
		e.Init(t.Bytes, t.Pos)
		return ExpressionStatement{}.FromBuf(e)
	case fbast.StatementReturnStatement:
		r := new(fbast.ReturnStatement)
		r.Init(t.Bytes, t.Pos)
		return ReturnStatement{}.FromBuf(r)
	case fbast.StatementOptionStatement:
		o := new(fbast.OptionStatement)
		o.Init(t.Bytes, t.Pos)
		return OptionStatement{}.FromBuf(o)
	case fbast.StatementBuiltinStatement:
		b := new(fbast.BuiltinStatement)
		b.Init(t.Bytes, t.Pos)
		return BuiltinStatement{}.FromBuf(b)
	case fbast.StatementTestStatement:
		s := new(fbast.TestStatement)
		s.Init(t.Bytes, t.Pos)
		return TestStatement{}.FromBuf(s)
	case fbast.StatementTestCaseStatement:
		s := new(fbast.TestCaseStatement)
		s.Init(t.Bytes, t.Pos)
		return TestCaseStatement{}.FromBuf(s)
	default:
		// Ultimately we want to use bad statement/expression to store errors?
		return nil
	}
}

type exprGetterFn func(obj *fbast.WrappedExpression, j int) bool

func exprArrayFromBuf(len int, g exprGetterFn, arrName string) ([]Expression, []Error) {
	s := make([]Expression, len)
	err := make([]Error, 0)
	for i := 0; i < len; i++ {
		e := new(fbast.WrappedExpression)
		t := new(flatbuffers.Table)
		if !g(e, i) || !e.Expr(t) || e.ExprType() == fbast.ExpressionNONE {
			err = append(err, Error{fmt.Sprintf("Encountered error in deserializing %s[%d]", arrName, i)})
		} else {
			s[i] = exprFromBufTable(t, e.ExprType())
		}
	}
	return s, err
}

type unionTableWriterFn func(t *flatbuffers.Table) bool

func exprFromBuf(label string, baseNode BaseNode, f unionTableWriterFn, etype fbast.Expression) Expression {
	t := new(flatbuffers.Table)
	if !f(t) || etype == fbast.ExpressionNONE {
		baseNode.Errors = append(baseNode.Errors,
			Error{fmt.Sprintf("Encountered error in deserializing %s", label)})
		return nil
	}
	return exprFromBufTable(t, etype)
}

func exprFromBufTable(t *flatbuffers.Table, etype fbast.Expression) Expression {
	switch etype {
	case fbast.ExpressionStringExpression:
		s := new(fbast.StringExpression)
		s.Init(t.Bytes, t.Pos)
		return StringExpression{}.FromBuf(s)
	case fbast.ExpressionParenExpression:
		p := new(fbast.ParenExpression)
		p.Init(t.Bytes, t.Pos)
		return ParenExpression{}.FromBuf(p)
	case fbast.ExpressionArrayExpression:
		a := new(fbast.ArrayExpression)
		a.Init(t.Bytes, t.Pos)
		return ArrayExpression{}.FromBuf(a)
	case fbast.ExpressionFunctionExpression:
		f := new(fbast.FunctionExpression)
		f.Init(t.Bytes, t.Pos)
		return FunctionExpression{}.FromBuf(f)
	case fbast.ExpressionBinaryExpression:
		b := new(fbast.BinaryExpression)
		b.Init(t.Bytes, t.Pos)
		return BinaryExpression{}.FromBuf(b)
	case fbast.ExpressionBooleanLiteral:
		b := new(fbast.BooleanLiteral)
		b.Init(t.Bytes, t.Pos)
		return BooleanLiteral{}.FromBuf(b)
	case fbast.ExpressionCallExpression:
		c := new(fbast.CallExpression)
		c.Init(t.Bytes, t.Pos)
		return CallExpression{}.FromBuf(c)
	case fbast.ExpressionConditionalExpression:
		c := new(fbast.ConditionalExpression)
		c.Init(t.Bytes, t.Pos)
		return ConditionalExpression{}.FromBuf(c)
	case fbast.ExpressionDateTimeLiteral:
		d := new(fbast.DateTimeLiteral)
		d.Init(t.Bytes, t.Pos)
		return DateTimeLiteral{}.FromBuf(d)
	case fbast.ExpressionDurationLiteral:
		d := new(fbast.DurationLiteral)
		d.Init(t.Bytes, t.Pos)
		return DurationLiteral{}.FromBuf(d)
	case fbast.ExpressionFloatLiteral:
		f := new(fbast.FloatLiteral)
		f.Init(t.Bytes, t.Pos)
		return FloatLiteral{}.FromBuf(f)
	case fbast.ExpressionIdentifier:
		i := new(fbast.Identifier)
		i.Init(t.Bytes, t.Pos)
		return Identifier{}.FromBuf(i)
	case fbast.ExpressionIntegerLiteral:
		i := new(fbast.IntegerLiteral)
		i.Init(t.Bytes, t.Pos)
		return IntegerLiteral{}.FromBuf(i)
	case fbast.ExpressionLogicalExpression:
		l := new(fbast.LogicalExpression)
		l.Init(t.Bytes, t.Pos)
		return LogicalExpression{}.FromBuf(l)
	case fbast.ExpressionMemberExpression:
		m := new(fbast.MemberExpression)
		m.Init(t.Bytes, t.Pos)
		return MemberExpression{}.FromBuf(m)
	case fbast.ExpressionIndexExpression:
		m := new(fbast.IndexExpression)
		m.Init(t.Bytes, t.Pos)
		return IndexExpression{}.FromBuf(m)
	case fbast.ExpressionObjectExpression:
		m := new(fbast.ObjectExpression)
		m.Init(t.Bytes, t.Pos)
		return ObjectExpression{}.FromBuf(m)
	case fbast.ExpressionPipeExpression:
		p := new(fbast.PipeExpression)
		p.Init(t.Bytes, t.Pos)
		return PipeExpression{}.FromBuf(p)
	case fbast.ExpressionPipeLiteral:
		p := new(fbast.PipeLiteral)
		p.Init(t.Bytes, t.Pos)
		return PipeLiteral{}.FromBuf(p)
	case fbast.ExpressionRegexpLiteral:
		r := new(fbast.RegexpLiteral)
		r.Init(t.Bytes, t.Pos)
		return RegexpLiteral{}.FromBuf(r)
	case fbast.ExpressionStringLiteral:
		r := new(fbast.StringLiteral)
		r.Init(t.Bytes, t.Pos)
		return StringLiteral{}.FromBuf(r)
	case fbast.ExpressionUnaryExpression:
		u := new(fbast.UnaryExpression)
		u.Init(t.Bytes, t.Pos)
		return UnaryExpression{}.FromBuf(u)
	case fbast.ExpressionUnsignedIntegerLiteral:
		u := new(fbast.UnsignedIntegerLiteral)
		u.Init(t.Bytes, t.Pos)
		return UnsignedIntegerLiteral{}.FromBuf(u)
	case fbast.ExpressionBadExpression:
		fallthrough
	default:
		return nil
	}
}

func assignmentFromBuf(label string, baseNode BaseNode, f unionTableWriterFn, atype fbast.Assignment) Assignment {
	t := new(flatbuffers.Table)
	if !f(t) || atype == fbast.AssignmentNONE {
		baseNode.Errors = append(baseNode.Errors,
			Error{fmt.Sprintf("Encountered error in deserializing %s", label)})
		return nil
	}
	switch atype {
	case fbast.AssignmentMemberAssignment:
		fba := new(fbast.MemberAssignment)
		fba.Init(t.Bytes, t.Pos)
		return MemberAssignment{}.FromBuf(fba)
	case fbast.AssignmentVariableAssignment:
		fba := new(fbast.VariableAssignment)
		fba.Init(t.Bytes, t.Pos)
		return VariableAssignment{}.FromBuf(fba)
	default:
		return nil
	}
}

func propertyKeyFromBuf(label string, baseNode BaseNode, f unionTableWriterFn, atype fbast.PropertyKey) PropertyKey {
	t := new(flatbuffers.Table)
	if !f(t) || atype == fbast.PropertyKeyNONE {
		baseNode.Errors = append(baseNode.Errors,
			Error{fmt.Sprintf("Encountered error in deserializing %s", label)})
		return nil
	}
	switch atype {
	case fbast.PropertyKeyIdentifier:
		fbk := new(fbast.Identifier)
		fbk.Init(t.Bytes, t.Pos)
		return Identifier{}.FromBuf(fbk)
	case fbast.PropertyKeyStringLiteral:
		fbs := new(fbast.StringLiteral)
		fbs.Init(t.Bytes, t.Pos)
		return StringLiteral{}.FromBuf(fbs)
	default:
		return nil
	}
}

var opMap = map[fbast.Operator]OperatorKind{
	fbast.OperatorMultiplicationOperator:   MultiplicationOperator,
	fbast.OperatorDivisionOperator:         DivisionOperator,
	fbast.OperatorModuloOperator:           ModuloOperator,
	fbast.OperatorPowerOperator:            PowerOperator,
	fbast.OperatorAdditionOperator:         AdditionOperator,
	fbast.OperatorSubtractionOperator:      SubtractionOperator,
	fbast.OperatorLessThanEqualOperator:    LessThanEqualOperator,
	fbast.OperatorLessThanOperator:         LessThanOperator,
	fbast.OperatorGreaterThanEqualOperator: GreaterThanEqualOperator,
	fbast.OperatorGreaterThanOperator:      GreaterThanOperator,
	fbast.OperatorStartsWithOperator:       StartsWithOperator,
	fbast.OperatorInOperator:               InOperator,
	fbast.OperatorNotOperator:              NotOperator,
	fbast.OperatorExistsOperator:           ExistsOperator,
	fbast.OperatorNotEmptyOperator:         NotEmptyOperator,
	fbast.OperatorEmptyOperator:            EmptyOperator,
	fbast.OperatorEqualOperator:            EqualOperator,
	fbast.OperatorNotEqualOperator:         NotEqualOperator,
	fbast.OperatorRegexpMatchOperator:      RegexpMatchOperator,
	fbast.OperatorNotRegexpMatchOperator:   NotRegexpMatchOperator,
}

var logOpMap = map[fbast.LogicalOperator]LogicalOperatorKind{
	fbast.LogicalOperatorAndOperator: AndOperator,
	fbast.LogicalOperatorOrOperator:  OrOperator,
}

var paramKindMap = map[fbast.ParameterKind]ParameterKind{
	fbast.ParameterKindRequired: Required,
	fbast.ParameterKindOptional: Optional,
	fbast.ParameterKindPipe:     Pipe,
}
