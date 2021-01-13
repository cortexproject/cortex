package ast

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
)

func (p *Package) MarshalJSON() ([]byte, error) {
	type Alias Package
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  p.Type(),
		Alias: (*Alias)(p),
	}
	return json.Marshal(raw)
}
func (f *File) MarshalJSON() ([]byte, error) {
	type Alias File
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  f.Type(),
		Alias: (*Alias)(f),
	}
	return json.Marshal(raw)
}
func (f *File) UnmarshalJSON(data []byte) error {
	type Alias File
	raw := struct {
		*Alias
		Body []json.RawMessage `json:"body"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*f = *(*File)(raw.Alias)
	}

	f.Body = make([]Statement, len(raw.Body))
	for i, r := range raw.Body {
		s, err := unmarshalStatement(r)
		if err != nil {
			return err
		}
		f.Body[i] = s
	}
	return nil
}
func (c *PackageClause) MarshalJSON() ([]byte, error) {
	type Alias PackageClause
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  c.Type(),
		Alias: (*Alias)(c),
	}
	return json.Marshal(raw)
}
func (d *ImportDeclaration) MarshalJSON() ([]byte, error) {
	type Alias ImportDeclaration
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  d.Type(),
		Alias: (*Alias)(d),
	}
	return json.Marshal(raw)
}
func (s *BadStatement) MarshalJSON() ([]byte, error) {
	type Alias BadStatement
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  s.Type(),
		Alias: (*Alias)(s),
	}
	return json.Marshal(raw)
}
func (s *Block) MarshalJSON() ([]byte, error) {
	type Alias Block
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  s.Type(),
		Alias: (*Alias)(s),
	}
	return json.Marshal(raw)
}
func (s *Block) UnmarshalJSON(data []byte) error {
	type Alias Block
	raw := struct {
		*Alias
		Body []json.RawMessage `json:"body"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*s = *(*Block)(raw.Alias)
	}

	s.Body = make([]Statement, len(raw.Body))
	for i, r := range raw.Body {
		stmt, err := unmarshalStatement(r)
		if err != nil {
			return err
		}
		s.Body[i] = stmt
	}
	return nil
}
func (s *ExpressionStatement) MarshalJSON() ([]byte, error) {
	type Alias ExpressionStatement
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  s.Type(),
		Alias: (*Alias)(s),
	}
	return json.Marshal(raw)
}
func (s *ExpressionStatement) UnmarshalJSON(data []byte) error {
	type Alias ExpressionStatement
	raw := struct {
		*Alias
		Expression json.RawMessage `json:"expression"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*s = *(*ExpressionStatement)(raw.Alias)
	}

	e, err := unmarshalExpression(raw.Expression)
	if err != nil {
		return err
	}
	s.Expression = e
	return nil
}
func (s *ReturnStatement) MarshalJSON() ([]byte, error) {
	type Alias ReturnStatement
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  s.Type(),
		Alias: (*Alias)(s),
	}
	return json.Marshal(raw)
}
func (s *ReturnStatement) UnmarshalJSON(data []byte) error {
	type Alias ReturnStatement
	raw := struct {
		*Alias
		Argument json.RawMessage `json:"argument"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*s = *(*ReturnStatement)(raw.Alias)
	}

	e, err := unmarshalExpression(raw.Argument)
	if err != nil {
		return err
	}
	s.Argument = e
	return nil
}
func (s *OptionStatement) MarshalJSON() ([]byte, error) {
	type Alias OptionStatement
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  s.Type(),
		Alias: (*Alias)(s),
	}
	return json.Marshal(raw)
}
func (s *OptionStatement) UnmarshalJSON(data []byte) error {
	type Alias OptionStatement
	raw := struct {
		*Alias
		Assignment json.RawMessage `json:"assignment"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*s = *(*OptionStatement)(raw.Alias)
	}

	a, err := unmarshalAssignment(raw.Assignment)
	if err != nil {
		return err
	}
	s.Assignment = a
	return nil
}
func (s *BuiltinStatement) MarshalJSON() ([]byte, error) {
	type Alias BuiltinStatement
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  s.Type(),
		Alias: (*Alias)(s),
	}
	return json.Marshal(raw)
}

func (d *BuiltinStatement) UnmarshalJSON(data []byte) error {
	type Alias BuiltinStatement
	raw := struct {
		*Alias
		Init json.RawMessage `json:"ty"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*d = *(*BuiltinStatement)(raw.Alias)
	}

	e, err := unmarshalTypeExpression(raw.Init)
	if err != nil {
		return err
	}
	d.Ty = *e
	return nil
}
func (s *TestStatement) MarshalJSON() ([]byte, error) {
	type Alias TestStatement
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  s.Type(),
		Alias: (*Alias)(s),
	}
	return json.Marshal(raw)
}
func (s *TestCaseStatement) MarshalJSON() ([]byte, error) {
	type Alias TestCaseStatement
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  s.Type(),
		Alias: (*Alias)(s),
	}
	return json.Marshal(raw)
}
func (d *VariableAssignment) MarshalJSON() ([]byte, error) {
	type Alias VariableAssignment
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  d.Type(),
		Alias: (*Alias)(d),
	}
	return json.Marshal(raw)
}
func (d *VariableAssignment) UnmarshalJSON(data []byte) error {
	type Alias VariableAssignment
	raw := struct {
		*Alias
		Init json.RawMessage `json:"init"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*d = *(*VariableAssignment)(raw.Alias)
	}

	e, err := unmarshalExpression(raw.Init)
	if err != nil {
		return err
	}
	d.Init = e
	return nil
}
func (a *MemberAssignment) MarshalJSON() ([]byte, error) {
	type Alias MemberAssignment
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  a.Type(),
		Alias: (*Alias)(a),
	}
	return json.Marshal(raw)
}
func (a *MemberAssignment) UnmarshalJSON(data []byte) error {
	type Alias MemberAssignment
	raw := struct {
		*Alias
		Init json.RawMessage `json:"init"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*a = *(*MemberAssignment)(raw.Alias)
	}

	e, err := unmarshalExpression(raw.Init)
	if err != nil {
		return err
	}
	a.Init = e
	return nil
}
func (e *CallExpression) MarshalJSON() ([]byte, error) {
	type Alias CallExpression
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  e.Type(),
		Alias: (*Alias)(e),
	}
	return json.Marshal(raw)
}
func (e *CallExpression) UnmarshalJSON(data []byte) error {
	type Alias CallExpression
	raw := struct {
		*Alias
		Callee    json.RawMessage   `json:"callee"`
		Arguments []json.RawMessage `json:"arguments"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*e = *(*CallExpression)(raw.Alias)
	}

	callee, err := unmarshalExpression(raw.Callee)
	if err != nil {
		return err
	}
	e.Callee = callee

	if len(raw.Arguments) > 0 {
		e.Arguments = make([]Expression, len(raw.Arguments))
		for i, r := range raw.Arguments {
			expr, err := unmarshalExpression(r)
			if err != nil {
				return err
			}
			e.Arguments[i] = expr
		}
	}
	return nil
}
func (e *PipeExpression) MarshalJSON() ([]byte, error) {
	type Alias PipeExpression
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  e.Type(),
		Alias: (*Alias)(e),
	}
	return json.Marshal(raw)
}
func (e *PipeExpression) UnmarshalJSON(data []byte) error {
	type Alias PipeExpression
	raw := struct {
		*Alias
		Argument json.RawMessage `json:"argument"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*e = *(*PipeExpression)(raw.Alias)
	}

	arg, err := unmarshalExpression(raw.Argument)
	if err != nil {
		return err
	}
	e.Argument = arg

	return nil
}
func (e *MemberExpression) MarshalJSON() ([]byte, error) {
	type Alias MemberExpression
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  e.Type(),
		Alias: (*Alias)(e),
	}
	return json.Marshal(raw)
}
func (e *MemberExpression) UnmarshalJSON(data []byte) error {
	type Alias MemberExpression
	raw := struct {
		*Alias
		Object   json.RawMessage `json:"object"`
		Property json.RawMessage `json:"property"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*e = *(*MemberExpression)(raw.Alias)
	}

	object, err := unmarshalExpression(raw.Object)
	if err != nil {
		return err
	}
	e.Object = object

	property, err := unmarshalPropertyKey(raw.Property)
	if err != nil {
		return err
	}
	e.Property = property

	return nil
}
func (e *IndexExpression) MarshalJSON() ([]byte, error) {
	type Alias IndexExpression
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  e.Type(),
		Alias: (*Alias)(e),
	}
	return json.Marshal(raw)
}
func (e *IndexExpression) UnmarshalJSON(data []byte) error {
	type Alias IndexExpression
	raw := struct {
		*Alias
		Array json.RawMessage `json:"array"`
		Index json.RawMessage `json:"index"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*e = *(*IndexExpression)(raw.Alias)
	}

	array, err := unmarshalExpression(raw.Array)
	if err != nil {
		return err
	}
	e.Array = array

	index, err := unmarshalExpression(raw.Index)
	if err != nil {
		return err
	}
	e.Index = index

	return nil
}
func (e *FunctionExpression) MarshalJSON() ([]byte, error) {
	type Alias FunctionExpression
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  e.Type(),
		Alias: (*Alias)(e),
	}
	return json.Marshal(raw)
}
func (e *FunctionExpression) UnmarshalJSON(data []byte) error {
	type Alias FunctionExpression
	raw := struct {
		*Alias
		Body json.RawMessage `json:"body"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*e = *(*FunctionExpression)(raw.Alias)
	}

	body, err := unmarshalNode(raw.Body)
	if err != nil {
		return err
	}
	e.Body = body
	return nil
}
func (e *BinaryExpression) MarshalJSON() ([]byte, error) {
	type Alias BinaryExpression
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  e.Type(),
		Alias: (*Alias)(e),
	}
	return json.Marshal(raw)
}
func (e *BinaryExpression) UnmarshalJSON(data []byte) error {
	type Alias BinaryExpression
	raw := struct {
		*Alias
		Left  json.RawMessage `json:"left"`
		Right json.RawMessage `json:"right"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*e = *(*BinaryExpression)(raw.Alias)
	}

	l, err := unmarshalExpression(raw.Left)
	if err != nil {
		return err
	}
	e.Left = l

	r, err := unmarshalExpression(raw.Right)
	if err != nil {
		return err
	}
	e.Right = r
	return nil
}
func (e *UnaryExpression) MarshalJSON() ([]byte, error) {
	type Alias UnaryExpression
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  e.Type(),
		Alias: (*Alias)(e),
	}
	return json.Marshal(raw)
}
func (e *UnaryExpression) UnmarshalJSON(data []byte) error {
	type Alias UnaryExpression
	raw := struct {
		*Alias
		Argument json.RawMessage `json:"argument"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*e = *(*UnaryExpression)(raw.Alias)
	}

	argument, err := unmarshalExpression(raw.Argument)
	if err != nil {
		return err
	}
	e.Argument = argument

	return nil
}
func (e *LogicalExpression) MarshalJSON() ([]byte, error) {
	type Alias LogicalExpression
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  e.Type(),
		Alias: (*Alias)(e),
	}
	return json.Marshal(raw)
}
func (e *LogicalExpression) UnmarshalJSON(data []byte) error {
	type Alias LogicalExpression
	raw := struct {
		*Alias
		Left  json.RawMessage `json:"left"`
		Right json.RawMessage `json:"right"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*e = *(*LogicalExpression)(raw.Alias)
	}

	l, err := unmarshalExpression(raw.Left)
	if err != nil {
		return err
	}
	e.Left = l

	r, err := unmarshalExpression(raw.Right)
	if err != nil {
		return err
	}
	e.Right = r
	return nil
}
func (e *StringExpression) MarshalJSON() ([]byte, error) {
	type Alias StringExpression
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  e.Type(),
		Alias: (*Alias)(e),
	}
	return json.Marshal(raw)
}
func (e *StringExpression) UnmarshalJSON(data []byte) error {
	type Alias StringExpression
	raw := struct {
		*Alias
		Parts []json.RawMessage `json:"parts"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*e = *(*StringExpression)(raw.Alias)
	}

	e.Parts = make([]StringExpressionPart, len(raw.Parts))
	for i, r := range raw.Parts {
		part, err := unmarshalStringPart(r)
		if err != nil {
			return err
		}
		e.Parts[i] = part
	}
	return nil
}
func (p *TextPart) MarshalJSON() ([]byte, error) {
	type Alias TextPart
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  p.Type(),
		Alias: (*Alias)(p),
	}
	return json.Marshal(raw)
}
func (p *TextPart) UnmarshalJSON(data []byte) error {
	type Alias TextPart
	raw := struct {
		*Alias
		Value string `json:"value"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*p = *(*TextPart)(raw.Alias)
	}
	p.Value = raw.Value
	return nil
}
func (p *InterpolatedPart) MarshalJSON() ([]byte, error) {
	type Alias InterpolatedPart
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  p.Type(),
		Alias: (*Alias)(p),
	}
	return json.Marshal(raw)
}
func (p *InterpolatedPart) UnmarshalJSON(data []byte) error {
	type Alias InterpolatedPart
	raw := struct {
		*Alias
		Expression json.RawMessage `json:"expression"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*p = *(*InterpolatedPart)(raw.Alias)
	}

	expr, err := unmarshalExpression(raw.Expression)
	if err != nil {
		return err
	}
	p.Expression = expr
	return nil
}
func (e *ParenExpression) MarshalJSON() ([]byte, error) {
	type Alias ParenExpression
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  e.Type(),
		Alias: (*Alias)(e),
	}
	return json.Marshal(raw)
}
func (e *ParenExpression) UnmarshalJSON(data []byte) error {
	type Alias ParenExpression
	raw := struct {
		*Alias
		Expression json.RawMessage `json:"expression"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*e = *(*ParenExpression)(raw.Alias)
	}

	expr, err := unmarshalExpression(raw.Expression)
	if err != nil {
		return err
	}
	e.Expression = expr
	return nil
}
func (e *ArrayExpression) MarshalJSON() ([]byte, error) {
	type Alias ArrayExpression
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  e.Type(),
		Alias: (*Alias)(e),
	}
	return json.Marshal(raw)
}
func (e *ArrayExpression) UnmarshalJSON(data []byte) error {
	type Alias ArrayExpression
	raw := struct {
		*Alias
		Elements []json.RawMessage `json:"elements"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*e = *(*ArrayExpression)(raw.Alias)
	}

	e.Elements = make([]Expression, len(raw.Elements))
	for i, r := range raw.Elements {
		expr, err := unmarshalExpression(r)
		if err != nil {
			return err
		}
		e.Elements[i] = expr
	}
	return nil
}
func (e *DictExpression) MarshalJSON() ([]byte, error) {
	type Alias DictExpression
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  e.Type(),
		Alias: (*Alias)(e),
	}
	return json.Marshal(raw)
}
func (e *DictExpression) UnmarshalJSON(data []byte) error {
	type Alias DictExpression
	raw := struct {
		*Alias
		Elements []json.RawMessage `json:"elements"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*e = *(*DictExpression)(raw.Alias)
	}

	e.Elements = make([]*DictItem, len(raw.Elements))
	for i, r := range raw.Elements {
		item := new(DictItem)
		if err := item.UnmarshalJSON(r); err != nil {
			return err
		}
		e.Elements[i] = item
	}
	return nil
}
func (item *DictItem) MarshalJSON() ([]byte, error) {
	type Alias DictItem
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  item.Type(),
		Alias: (*Alias)(item),
	}
	return json.Marshal(raw)
}
func (item *DictItem) UnmarshalJSON(data []byte) error {
	type Alias DictItem
	raw := struct {
		*Alias
		Key json.RawMessage `json:"key"`
		Val json.RawMessage `json:"val"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	key, err := unmarshalExpression(raw.Key)
	if err != nil {
		return err
	}
	item.Key = key

	val, err := unmarshalExpression(raw.Val)
	if err != nil {
		return err
	}
	item.Val = val

	return nil
}
func (e *ObjectExpression) MarshalJSON() ([]byte, error) {
	type Alias ObjectExpression
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  e.Type(),
		Alias: (*Alias)(e),
	}
	return json.Marshal(raw)
}
func (e *ConditionalExpression) MarshalJSON() ([]byte, error) {
	type Alias ConditionalExpression
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  e.Type(),
		Alias: (*Alias)(e),
	}
	return json.Marshal(raw)
}
func (e *ConditionalExpression) UnmarshalJSON(data []byte) error {
	type Alias ConditionalExpression
	raw := struct {
		*Alias
		Test       json.RawMessage `json:"test"`
		Alternate  json.RawMessage `json:"alternate"`
		Consequent json.RawMessage `json:"consequent"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*e = *(*ConditionalExpression)(raw.Alias)
	}

	test, err := unmarshalExpression(raw.Test)
	if err != nil {
		return err
	}
	e.Test = test

	alternate, err := unmarshalExpression(raw.Alternate)
	if err != nil {
		return err
	}
	e.Alternate = alternate

	consequent, err := unmarshalExpression(raw.Consequent)
	if err != nil {
		return err
	}
	e.Consequent = consequent
	return nil
}
func (p *Property) MarshalJSON() ([]byte, error) {
	type Alias Property
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  p.Type(),
		Alias: (*Alias)(p),
	}
	return json.Marshal(raw)
}
func (p *Property) UnmarshalJSON(data []byte) error {
	type Alias Property
	raw := struct {
		*Alias
		Key   json.RawMessage `json:"key"`
		Value json.RawMessage `json:"value"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*p = *(*Property)(raw.Alias)
	}

	key, err := unmarshalPropertyKey(raw.Key)
	if err != nil {
		return err
	}
	p.Key = key

	if raw.Value != nil {
		value, err := unmarshalExpression(raw.Value)
		if err != nil {
			return err
		}
		p.Value = value
	}
	return nil
}
func (i *Identifier) MarshalJSON() ([]byte, error) {
	type Alias Identifier
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  i.Type(),
		Alias: (*Alias)(i),
	}
	return json.Marshal(raw)
}
func (i *Identifier) UnmarshalJSON(data []byte) error {
	type Alias Identifier
	raw := struct {
		*Alias
		Name string `json:"name"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*i = *(*Identifier)(raw.Alias)
	}
	i.Name = raw.Name
	return nil
}
func (p *PipeLiteral) MarshalJSON() ([]byte, error) {
	type Alias PipeLiteral
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  p.Type(),
		Alias: (*Alias)(p),
	}
	return json.Marshal(raw)
}
func (l *StringLiteral) MarshalJSON() ([]byte, error) {
	type Alias StringLiteral
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  l.Type(),
		Alias: (*Alias)(l),
	}
	return json.Marshal(raw)
}
func (l *BooleanLiteral) MarshalJSON() ([]byte, error) {
	type Alias BooleanLiteral
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  l.Type(),
		Alias: (*Alias)(l),
	}
	return json.Marshal(raw)
}
func (l *FloatLiteral) MarshalJSON() ([]byte, error) {
	type Alias FloatLiteral
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  l.Type(),
		Alias: (*Alias)(l),
	}
	return json.Marshal(raw)
}
func (l *IntegerLiteral) MarshalJSON() ([]byte, error) {
	type Alias IntegerLiteral
	raw := struct {
		Type string `json:"type"`
		*Alias
		Value string `json:"value"`
	}{
		Type:  l.Type(),
		Alias: (*Alias)(l),
		Value: strconv.FormatInt(l.Value, 10),
	}
	return json.Marshal(raw)
}
func (l *IntegerLiteral) UnmarshalJSON(data []byte) error {
	type Alias IntegerLiteral
	raw := struct {
		*Alias
		Value string `json:"value"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if raw.Alias != nil {
		*l = *(*IntegerLiteral)(raw.Alias)
	}

	value, err := strconv.ParseInt(raw.Value, 10, 64)
	if err != nil {
		return err
	}
	l.Value = value
	return nil
}
func (l *UnsignedIntegerLiteral) MarshalJSON() ([]byte, error) {
	type Alias UnsignedIntegerLiteral
	raw := struct {
		Type string `json:"type"`
		*Alias
		Value string `json:"value"`
	}{
		Type:  l.Type(),
		Alias: (*Alias)(l),
		Value: strconv.FormatUint(l.Value, 10),
	}
	return json.Marshal(raw)
}
func (l *UnsignedIntegerLiteral) UnmarshalJSON(data []byte) error {
	type Alias UnsignedIntegerLiteral
	raw := struct {
		*Alias
		Value string `json:"value"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if raw.Alias != nil {
		*l = *(*UnsignedIntegerLiteral)(raw.Alias)
	}

	value, err := strconv.ParseUint(raw.Value, 10, 64)
	if err != nil {
		return err
	}
	l.Value = value
	return nil
}
func (l *RegexpLiteral) MarshalJSON() ([]byte, error) {
	type Alias RegexpLiteral
	raw := struct {
		Type string `json:"type"`
		*Alias
		Value string `json:"value"`
	}{
		Type:  l.Type(),
		Alias: (*Alias)(l),
		Value: l.Value.String(),
	}
	return json.Marshal(raw)
}
func (l *RegexpLiteral) UnmarshalJSON(data []byte) error {
	type Alias RegexpLiteral
	raw := struct {
		*Alias
		Value string `json:"value"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if raw.Alias != nil {
		*l = *(*RegexpLiteral)(raw.Alias)
	}

	value, err := regexp.Compile(raw.Value)
	if err != nil {
		return err
	}
	l.Value = value
	return nil
}
func (l *DurationLiteral) MarshalJSON() ([]byte, error) {
	type Alias DurationLiteral
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  l.Type(),
		Alias: (*Alias)(l),
	}
	return json.Marshal(raw)
}
func (l *DateTimeLiteral) MarshalJSON() ([]byte, error) {
	type Alias DateTimeLiteral
	raw := struct {
		Type string `json:"type"`
		*Alias
	}{
		Type:  l.Type(),
		Alias: (*Alias)(l),
	}
	return json.Marshal(raw)
}

// new types for builtin package
func (nt NamedType) MarshalJSON() ([]byte, error) {
	type Alias NamedType
	raw := struct {
		Type string `json:"type"`
		Alias
	}{
		Type:  nt.Type(),
		Alias: (Alias)(nt),
	}
	ret, err := json.Marshal(raw)
	return ret, err
}
func (nt *NamedType) UnmarshalJSON(data []byte) error {
	type Alias NamedType
	raw := struct {
		*Alias
		ID *Identifier `json:"name"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*nt = *(*NamedType)(raw.Alias)
	}
	nt.ID = raw.ID
	return nil
}

func (tv TvarType) MarshalJSON() ([]byte, error) {
	type Alias TvarType
	raw := struct {
		Type string `json:"type"`
		Alias
	}{
		Type:  tv.Type(),
		Alias: (Alias)(tv),
	}
	return json.Marshal(raw)
}

func (arr ArrayType) MarshalJSON() ([]byte, error) {
	type Alias ArrayType
	raw := struct {
		Type string `json:"type"`
		Alias
	}{
		Type:  arr.Type(),
		Alias: (Alias)(arr),
	}
	return json.Marshal(raw)
}
func (nt *ArrayType) UnmarshalJSON(data []byte) error {
	type Alias ArrayType
	raw := struct {
		*Alias
		ElementType json.RawMessage `json:"element"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*nt = *(*ArrayType)(raw.Alias)
	}
	et, err := unmarshalMonotype(raw.ElementType)
	if err != nil {
		return err
	}
	nt.ElementType = et
	return nil
}

func (c DictType) MarshalJSON() ([]byte, error) {
	type Alias DictType
	raw := struct {
		Type string `json:"type"`
		Alias
	}{
		Type:  c.Type(),
		Alias: (Alias)(c),
	}
	return json.Marshal(raw)
}
func (c *DictType) UnmarshalJSON(data []byte) error {
	type Alias DictType
	raw := struct {
		*Alias
		KeyType   json.RawMessage `json:"key"`
		ValueType json.RawMessage `json:"val"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*c = *(*DictType)(raw.Alias)
	}
	kt, err := unmarshalMonotype(raw.KeyType)
	if err != nil {
		return err
	}
	c.KeyType = kt
	vt, err := unmarshalMonotype(raw.ValueType)
	if err != nil {
		return err
	}
	c.ValueType = vt
	return nil
}

func (rec RecordType) MarshalJSON() ([]byte, error) {
	type Alias RecordType
	raw := struct {
		Type string `json:"type"`
		Alias
	}{
		Type:  rec.Type(),
		Alias: (Alias)(rec),
	}
	return json.Marshal(raw)
}

func (fun FunctionType) MarshalJSON() ([]byte, error) {
	type Alias FunctionType
	raw := struct {
		Type string `json:"type"`
		Alias
	}{
		Type:  fun.Type(),
		Alias: (Alias)(fun),
	}
	return json.Marshal(raw)
}
func (param *FunctionType) UnmarshalJSON(data []byte) error {
	type Alias FunctionType
	raw := struct {
		*Alias
		Return json.RawMessage `json:"monotype"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*param = *(*FunctionType)(raw.Alias)
	}
	ty, err := unmarshalMonotype(raw.Return)
	if err != nil {
		return err
	}
	param.Return = ty
	return nil
}

func (prop PropertyType) MarshalJSON() ([]byte, error) {
	type Alias PropertyType
	raw := struct {
		Type string `json:"type"`
		Alias
	}{
		Type:  prop.Type(),
		Alias: (Alias)(prop),
	}
	return json.Marshal(raw)
}
func (nt *PropertyType) UnmarshalJSON(data []byte) error {
	type Alias PropertyType
	raw := struct {
		*Alias
		Ty json.RawMessage `json:"monotype"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*nt = *(*PropertyType)(raw.Alias)
	}
	ty, err := unmarshalMonotype(raw.Ty)
	if err != nil {
		return err
	}
	nt.Ty = ty
	return nil
}

func (param ParameterType) MarshalJSON() ([]byte, error) {
	type Alias ParameterType
	raw := struct {
		Type string `json:"type"`
		Alias
	}{
		Type:  param.Type(),
		Alias: (Alias)(param),
	}
	return json.Marshal(raw)
}
func (param *ParameterType) UnmarshalJSON(data []byte) error {
	type Alias ParameterType
	raw := struct {
		*Alias
		Type string          `json:"type"` // add a Type field here
		Ty   json.RawMessage `json:"monotype"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*param = *(*ParameterType)(raw.Alias)
	}
	ty, err := unmarshalMonotype(raw.Ty)
	if err != nil {
		return err
	}
	param.Ty = ty
	param.Kind = ParameterKind(raw.Type) // Type field is really the parameter kind
	return nil
}

func (typ_con TypeConstraint) MarshalJSON() ([]byte, error) {
	type Alias TypeConstraint
	raw := struct {
		Type string `json:"type"`
		Alias
	}{
		Type:  typ_con.Type(),
		Alias: (Alias)(typ_con),
	}
	return json.Marshal(raw)
}
func (typ_expr *TypeConstraint) UnmarshalJSON(data []byte) error {
	type Alias TypeConstraint
	raw := struct {
		*Alias
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*typ_expr = *(*TypeConstraint)(raw.Alias)
	}

	return nil
}

func (typ_expr TypeExpression) MarshalJSON() ([]byte, error) {
	type Alias TypeExpression
	raw := struct {
		Type string `json:"type"`
		Alias
	}{
		Type:  typ_expr.Type(),
		Alias: (Alias)(typ_expr),
	}
	return json.Marshal(raw)
}
func (typ_expr *TypeExpression) UnmarshalJSON(data []byte) error {
	type Alias TypeExpression
	raw := struct {
		*Alias
		Ty json.RawMessage `json:"monotype"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Alias != nil {
		*typ_expr = *(*TypeExpression)(raw.Alias)
	}
	t, err := unmarshalMonotype(raw.Ty)
	if err != nil {
		return err
	}
	typ_expr.Ty = t
	return nil
}

func checkNullMsg(msg json.RawMessage) bool {
	switch len(msg) {
	case 0:
		return true
	case 4:
		return string(msg) == "null"
	default:
		return false
	}
}
func unmarshalMonotype(msg json.RawMessage) (MonoType, error) {
	if checkNullMsg(msg) {
		return nil, nil
	}
	n, err := unmarshalNode(msg)
	if err != nil {
		return nil, err
	}
	s, ok := n.(MonoType)
	if !ok {
		return nil, fmt.Errorf("node %q is not a monotype", n.Type())
	}
	return s, nil
}
func unmarshalTypeExpression(msg json.RawMessage) (*TypeExpression, error) {
	if checkNullMsg(msg) {
		return nil, nil
	}
	n, err := unmarshalNode(msg)
	if err != nil {
		return nil, err
	}
	s, ok := n.(*TypeExpression)
	if !ok {
		return nil, fmt.Errorf("node %q is not a TypeExpression", n.Type())
	}
	return s, nil
}
func unmarshalStatement(msg json.RawMessage) (Statement, error) {
	if checkNullMsg(msg) {
		return nil, nil
	}
	n, err := unmarshalNode(msg)
	if err != nil {
		return nil, err
	}
	s, ok := n.(Statement)
	if !ok {
		return nil, fmt.Errorf("node %q is not a statement", n.Type())
	}
	return s, nil
}
func unmarshalExpression(msg json.RawMessage) (Expression, error) {
	if checkNullMsg(msg) {
		return nil, nil
	}
	n, err := unmarshalNode(msg)
	if err != nil {
		return nil, err
	}
	if n == nil {
		// If both n and err are nil, then this is a BadExpression, just return nil.
		return nil, nil
	}
	e, ok := n.(Expression)
	if !ok {
		return nil, fmt.Errorf("node %q is not an expression", n.Type())
	}
	return e, nil
}
func unmarshalAssignment(msg json.RawMessage) (Assignment, error) {
	if checkNullMsg(msg) {
		return nil, nil
	}
	n, err := unmarshalNode(msg)
	if err != nil {
		return nil, err
	}
	a, ok := n.(Assignment)
	if !ok {
		return nil, fmt.Errorf("node %q is not an assignment", n.Type())
	}
	return a, nil
}
func unmarshalPropertyKey(msg json.RawMessage) (PropertyKey, error) {
	if checkNullMsg(msg) {
		return nil, nil
	}
	n, err := unmarshalNode(msg)
	if err != nil {
		return nil, err
	}
	k, ok := n.(PropertyKey)
	if !ok {
		return nil, fmt.Errorf("node %q is not a property key", n.Type())
	}
	return k, nil
}
func unmarshalStringPart(msg json.RawMessage) (StringExpressionPart, error) {
	if checkNullMsg(msg) {
		return nil, nil
	}
	n, err := unmarshalNode(msg)
	if err != nil {
		return nil, err
	}
	p, ok := n.(StringExpressionPart)
	if !ok {
		return nil, fmt.Errorf("node %q is not a string expression part", n.Type())
	}
	return p, nil
}
func unmarshalNode(msg json.RawMessage) (Node, error) {
	if checkNullMsg(msg) {
		return nil, nil
	}

	type typeRawMessage struct {
		Type string `json:"type"`
	}
	typ := typeRawMessage{}
	if err := json.Unmarshal(msg, &typ); err != nil {
		return nil, err
	}
	var node Node
	switch typ.Type {
	case "TypeExpression":
		node = new(TypeExpression)
	case "TypeConstraint":
		node = new(TypeConstraint)
	case "ParameterType":
		node = new(ParameterType)
	case "PropertyType":
		node = new(PropertyType)
	case "FunctionType":
		node = new(FunctionType)
	case "RecordType":
		node = new(RecordType)
	case "ArrayType":
		node = new(ArrayType)
	case "DictType":
		node = new(DictType)
	case "TvarType":
		node = new(TvarType)
	case "NamedType":
		node = new(NamedType)
	case "Package":
		node = new(Package)
	case "File":
		node = new(File)
	case "PackageClause":
		node = new(PackageClause)
	case "ImportDeclaration":
		node = new(ImportDeclaration)
	case "BadStatement":
		node = new(BadStatement)
	case "Block":
		node = new(Block)
	case "OptionStatement":
		node = new(OptionStatement)
	case "BuiltinStatement":
		node = new(BuiltinStatement)
	case "TestStatement":
		node = new(TestStatement)
	case "TestCaseStatement":
		node = new(TestCaseStatement)
	case "ExpressionStatement":
		node = new(ExpressionStatement)
	case "ReturnStatement":
		node = new(ReturnStatement)
	case "VariableAssignment":
		node = new(VariableAssignment)
	case "MemberAssignment":
		node = new(MemberAssignment)
	case "CallExpression":
		node = new(CallExpression)
	case "PipeExpression":
		node = new(PipeExpression)
	case "MemberExpression":
		node = new(MemberExpression)
	case "IndexExpression":
		node = new(IndexExpression)
	case "BinaryExpression":
		node = new(BinaryExpression)
	case "UnaryExpression":
		node = new(UnaryExpression)
	case "LogicalExpression":
		node = new(LogicalExpression)
	case "ObjectExpression":
		node = new(ObjectExpression)
	case "ConditionalExpression":
		node = new(ConditionalExpression)
	case "ArrayExpression":
		node = new(ArrayExpression)
	case "DictExpression":
		node = new(DictExpression)
	case "Identifier":
		node = new(Identifier)
	case "PipeLiteral":
		node = new(PipeLiteral)
	case "StringExpression":
		node = new(StringExpression)
	case "TextPart":
		node = new(TextPart)
	case "InterpolatedPart":
		node = new(InterpolatedPart)
	case "ParenExpression":
		node = new(ParenExpression)
	case "StringLiteral":
		node = new(StringLiteral)
	case "BooleanLiteral":
		node = new(BooleanLiteral)
	case "FloatLiteral":
		node = new(FloatLiteral)
	case "IntegerLiteral":
		node = new(IntegerLiteral)
	case "UnsignedIntegerLiteral":
		node = new(UnsignedIntegerLiteral)
	case "RegexpLiteral":
		node = new(RegexpLiteral)
	case "DurationLiteral":
		node = new(DurationLiteral)
	case "DateTimeLiteral":
		node = new(DateTimeLiteral)
	case "FunctionExpression":
		node = new(FunctionExpression)
	case "Property":
		node = new(Property)
	case "BadExpression":
		// Rust does not support plain nil if not using Options.
		// The places where we use BadExpressions in the Rust parser
		// are all just nil nodes in the Go parser.
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown type %q", typ.Type)
	}

	if err := json.Unmarshal(msg, node); err != nil {
		return nil, err
	}
	return node, nil
}
func UnmarshalNode(data []byte) (Node, error) {
	return unmarshalNode((json.RawMessage)(data))
}
