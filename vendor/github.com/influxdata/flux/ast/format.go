package ast

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

//Returns a valid script for a given AST rooted at node `n`.
//Formatting rules:
// - In a list of statements, if two statements are of a different type
//	(e.g. an `OptionStatement` followed by an `ExpressionStatement`), they are separated by a double newline.
// - In a function call (or object definition), if the arguments (or properties) are more than 3,
//	they are split into multiple lines.
func Format(n Node) string {
	f := &formatter{new(strings.Builder), 0}
	f.formatNode(n)
	return f.get()
}

type formatter struct {
	*strings.Builder
	indentation int
}

func (f *formatter) get() string {
	return f.String()
}

func (f *formatter) writeString(s string) {
	// `strings.Builder`'s methods never return a non-nil error,
	// so it is safe to ignore it.
	f.WriteString(s)
}

func (f *formatter) writeRune(r rune) {
	f.WriteRune(r)
}

func (f *formatter) writeIndent() {
	for i := 0; i < f.indentation; i++ {
		f.writeRune('\t')
	}
}

func (f *formatter) indent() {
	f.indentation++
}

func (f *formatter) unIndent() {
	f.indentation--
}

func (f *formatter) setIndent(i int) {
	f.indentation = i
}
func (f *formatter) writeComment(comment string) {
	f.writeString("// ")
	f.writeString(comment)
	f.writeRune('\n')
}

// Logic for handling operator precedence and parenthesis formatting.

const (
	functionCall = 1
	member       = 2
	index        = 3

	// Use offsets for operators and logical operators to ensure they are unique keys
	// in the map of operators precedence.
	opOffset  = 100
	lopOffset = 1000
)

func getIntForOp(op OperatorKind) int {
	return int(op) + opOffset
}

func getIntForLOp(op LogicalOperatorKind) int {
	return int(op) + lopOffset
}

func getPrecedence(key int) int {
	return opPrecedence[key]
}

func getPrecedenceForOp(op OperatorKind) int {
	return getPrecedence(getIntForOp(op))
}

func getPrecedenceForLOp(op LogicalOperatorKind) int {
	return getPrecedence(getIntForLOp(op))
}

// this matches the SPEC
var opPrecedence = map[int]int{
	functionCall: 1,
	member:       1,
	index:        1,
	// these are OperatorKinds
	getIntForOp(PowerOperator):            2,
	getIntForOp(MultiplicationOperator):   3,
	getIntForOp(DivisionOperator):         3,
	getIntForOp(ModuloOperator):           3,
	getIntForOp(AdditionOperator):         4,
	getIntForOp(SubtractionOperator):      4,
	getIntForOp(LessThanEqualOperator):    5,
	getIntForOp(LessThanOperator):         5,
	getIntForOp(GreaterThanEqualOperator): 5,
	getIntForOp(GreaterThanOperator):      5,
	getIntForOp(StartsWithOperator):       5,
	getIntForOp(InOperator):               5,
	getIntForOp(NotEmptyOperator):         5,
	getIntForOp(EmptyOperator):            5,
	getIntForOp(EqualOperator):            5,
	getIntForOp(NotEqualOperator):         5,
	getIntForOp(RegexpMatchOperator):      5,
	getIntForOp(NotRegexpMatchOperator):   5,
	getIntForOp(NotOperator):              6,
	getIntForOp(ExistsOperator):           6,
	// theses are LogicalOperatorKinds:
	getIntForLOp(AndOperator): 7,
	getIntForLOp(OrOperator):  8,
}

// formatChildWithParens applies the generic rule for parenthesis (not for binary expressions).
func (f *formatter) formatChildWithParens(parent, child Node) {
	f.formatLeftChildWithParens(parent, child)
}

// formatLeftChildWithParens applies the generic rule for parenthesis to the left child of a binary expression.
func (f *formatter) formatLeftChildWithParens(parent, child Node) {
	pvp, pvc := getPrecedences(parent, child)
	if needsParenthesis(pvp, pvc, false) {
		f.formatNodeWithParens(child)
	} else {
		f.formatNode(child)
	}
}

// formatRightChildWithParens applies the generic rule for parenthesis to the right child of a binary expression.
func (f *formatter) formatRightChildWithParens(parent, child Node) {
	pvp, pvc := getPrecedences(parent, child)
	if needsParenthesis(pvp, pvc, true) {
		f.formatNodeWithParens(child)
	} else {
		f.formatNode(child)
	}
}

func getPrecedences(parent, child Node) (int, int) {
	var pvp, pvc int
	switch parent := parent.(type) {
	case *BinaryExpression:
		pvp = getPrecedenceForOp(parent.Operator)
	case *LogicalExpression:
		pvp = getPrecedenceForLOp(parent.Operator)
	case *UnaryExpression:
		pvp = getPrecedenceForOp(parent.Operator)
	case *CallExpression:
		pvp = getPrecedence(functionCall)
	case *MemberExpression:
		pvp = getPrecedence(member)
	case *IndexExpression:
		pvp = getPrecedence(index)
	case *ParenExpression:
		return getPrecedences(parent.Expression, child)
	}

	switch child := child.(type) {
	case *BinaryExpression:
		pvc = getPrecedenceForOp(child.Operator)
	case *LogicalExpression:
		pvc = getPrecedenceForLOp(child.Operator)
	case *UnaryExpression:
		pvc = getPrecedenceForOp(child.Operator)
	case *CallExpression:
		pvc = getPrecedence(functionCall)
	case *MemberExpression:
		pvc = getPrecedence(member)
	case *IndexExpression:
		pvc = getPrecedence(index)
	case *ParenExpression:
		return getPrecedences(parent, child.Expression)
	}

	return pvp, pvc
}

// About parenthesis:
// We need parenthesis if a child node has lower precedence (bigger value) than its parent node.
// The same stands for the left child of a binary expression; while, for the right child, we need parenthesis if its
// precedence is lower or equal then its parent's.
//
// To explain parenthesis logic, we must to understand how the parser generates the AST.
// (A) - The parser always puts lower precedence operators at the root of the AST.
// (B) - When there are multiple operators with the same precedence, the right-most expression is at root.
// (C) - When there are parenthesis, instead, the parser recursively generates a AST for the expression contained
// in the parenthesis, and makes it the right child.
// So, when formatting:
//  - if we encounter a child with lower precedence on the left, this means it requires parenthesis, because, for sure,
//    the parser detected parenthesis to break (A);
//  - if we encounter a child with higher or equal precedence on the left, it doesn't need parenthesis, because
//    that was the natural parsing order of elements (see (B));
//  - if we encounter a child with lower or equal precedence on the right, it requires parenthesis, otherwise, it
//    would have been at root (see (C)).
func needsParenthesis(pvp, pvc int, isRight bool) bool {
	// If one of the precedence values is invalid, then we shouldn't apply any parenthesis.
	par := !(pvc == 0 || pvp == 0)
	par = par && ((!isRight && pvc > pvp) || (isRight && pvc >= pvp))
	return par
}

func (f *formatter) formatNodeWithParens(node Node) {
	f.writeRune('(')
	f.formatNode(node)
	f.writeRune(')')
}

func (f *formatter) formatPackage(n *Package) {
	f.formatPackageClause(&PackageClause{
		Name: &Identifier{Name: n.Package},
	})
	for i, file := range n.Files {
		if i != 0 {
			f.writeRune('\n')
			f.writeRune('\n')
		}
		if len(file.Name) > 0 {
			f.writeComment(file.Name)
		}
		f.formatFile(file, false)
	}
}

func (f *formatter) formatFile(n *File, includePkg bool) {
	sep := '\n'

	if includePkg && n.Package != nil && n.Package.Name != nil && n.Package.Name.Name != "" {
		f.writeIndent()
		f.formatNode(n.Package)

		if len(n.Imports) > 0 || len(n.Body) > 0 {
			f.writeRune(sep)
			f.writeRune(sep)
		}
	}

	for i, imp := range n.Imports {
		if i != 0 {
			f.writeRune(sep)
		}

		f.writeIndent()
		f.formatNode(imp)
	}

	if len(n.Imports) > 0 && len(n.Body) > 0 {
		f.writeRune(sep)
		f.writeRune(sep)
	}

	for i, c := range n.Body {
		if i != 0 {
			f.writeRune(sep)

			// separate different statements with double newline
			if n.Body[i-1].Type() != n.Body[i].Type() {
				f.writeRune(sep)
			}
		}

		f.writeIndent()
		f.formatNode(c)
	}
}

func (f *formatter) formatBlock(n *Block) {
	f.writeRune('{')

	sep := '\n'
	if len(n.Body) > 0 {
		f.indent()
	}

	for i, c := range n.Body {
		f.writeRune(sep)

		if i != 0 {
			// separate different statements with double newline
			if n.Body[i-1].Type() != n.Body[i].Type() {
				f.writeRune(sep)
			}
		}

		f.writeIndent()
		f.formatNode(c)
	}

	if len(n.Body) > 0 {
		f.writeRune(sep)
		f.unIndent()
		f.writeIndent()
	}

	f.writeRune('}')
}

func (f *formatter) formatPackageClause(n *PackageClause) {
	f.writeString("package ")
	f.formatNode(n.Name)
	f.writeRune('\n')
}

func (f *formatter) formatImportDeclaration(n *ImportDeclaration) {
	f.writeString("import ")

	if n.As != nil && len(n.As.Name) > 0 {
		f.formatNode(n.As)
		f.writeRune(' ')
	}

	f.formatNode(n.Path)
}

func (f *formatter) formatExpressionStatement(n *ExpressionStatement) {
	f.formatNode(n.Expression)
}

func (f *formatter) formatReturnStatement(n *ReturnStatement) {
	f.writeString("return ")
	f.formatNode(n.Argument)
}

func (f *formatter) formatOptionStatement(n *OptionStatement) {
	f.writeString("option ")
	f.formatNode(n.Assignment)
}

func (f *formatter) formatTestStatement(n *TestStatement) {
	f.writeString("test ")
	f.formatNode(n.Assignment)
}

func (f *formatter) formatTestCaseStatement(n *TestCaseStatement) {
	f.writeString("testcase ")
	f.formatNode(n.ID)
	f.formatNode(n.Block)
}

func (f *formatter) formatVariableAssignment(n *VariableAssignment) {
	f.formatNode(n.ID)
	f.writeString(" = ")
	f.formatNode(n.Init)
}

func (f *formatter) formatMemberAssignment(n *MemberAssignment) {
	f.formatNode(n.Member)
	f.writeString(" = ")
	f.formatNode(n.Init)
}

func (f *formatter) formatArrayExpression(n *ArrayExpression) {
	f.writeRune('[')

	sep := ", "
	for i, c := range n.Elements {
		if i != 0 {
			f.writeString(sep)
		}

		f.formatNode(c)
	}

	f.writeRune(']')
}

func (f *formatter) formatFunctionExpression(n *FunctionExpression) {
	f.writeRune('(')

	sep := ", "
	for i, c := range n.Params {
		if i != 0 {
			f.writeString(sep)
		}

		// treat properties differently than in general case
		f.formatFunctionArgument(c)
	}

	f.writeString(") =>")

	// must wrap body with parenthesis in order to discriminate between:
	//  - returning an object: (x) => ({foo: x})
	//  - and block statements:
	//		(x) => {
	//			return x + 1
	//		}
	_, block := n.Body.(*Block)
	if !block {
		f.writeRune('\n')
		f.indent()
		f.writeIndent()
		f.writeRune('(')
	} else {
		f.writeRune(' ')
	}

	f.formatNode(n.Body)
	if !block {
		f.writeRune(')')
	}
}

func (f *formatter) formatUnaryExpression(n *UnaryExpression) {
	f.writeString(n.Operator.String())
	if n.Operator != SubtractionOperator &&
		n.Operator != AdditionOperator {
		f.WriteRune(' ')
	}
	f.formatChildWithParens(n, n.Argument)
}

func (f *formatter) formatBinaryExpression(n *BinaryExpression) {
	f.formatBinary(n.Operator.String(), n, n.Left, n.Right)
}

func (f *formatter) formatLogicalExpression(n *LogicalExpression) {
	f.formatBinary(n.Operator.String(), n, n.Left, n.Right)
}

func (f *formatter) formatBinary(op string, parent, left, right Node) {
	f.formatLeftChildWithParens(parent, left)
	f.writeRune(' ')
	f.writeString(op)
	f.writeRune(' ')
	f.formatRightChildWithParens(parent, right)
}

func (f *formatter) formatCallExpression(n *CallExpression) {
	f.formatChildWithParens(n, n.Callee)
	f.writeRune('(')

	sep := ", "
	for i, c := range n.Arguments {
		if i != 0 {
			f.writeString(sep)
		}

		// treat ObjectExpression as argument in a special way
		// (an object as argument doesn't need braces)
		if oe, ok := c.(*ObjectExpression); ok {
			f.formatObjectExpressionAsFunctionArgument(oe)
		} else {
			f.formatNode(c)
		}
	}

	f.writeRune(')')
}

func (f *formatter) formatPipeExpression(n *PipeExpression) {
	f.formatNode(n.Argument)
	f.writeRune('\n')
	f.indent()
	f.writeIndent()
	f.writeString("|> ")
	f.formatNode(n.Call)
}

func (f *formatter) formatConditionalExpression(n *ConditionalExpression) {
	f.writeString("if ")
	f.formatNode(n.Test)
	f.writeString(" then ")
	f.formatNode(n.Consequent)
	f.writeString(" else ")
	f.formatNode(n.Alternate)
}

func (f *formatter) formatMemberExpression(n *MemberExpression) {
	f.formatChildWithParens(n, n.Object)

	if _, ok := n.Property.(*StringLiteral); ok {
		f.writeRune('[')
		f.formatNode(n.Property)
		f.writeRune(']')
	} else {
		f.writeRune('.')
		f.formatNode(n.Property)
	}
}

func (f *formatter) formatIndexExpression(n *IndexExpression) {
	f.formatChildWithParens(n, n.Array)
	f.writeRune('[')
	f.formatNode(n.Index)
	f.writeRune(']')
}

func (f *formatter) formatObjectExpression(n *ObjectExpression) {
	f.formatObjectExpressionBraces(n, true)
}

func (f *formatter) formatObjectExpressionAsFunctionArgument(n *ObjectExpression) {
	// not called from formatNode, need to save indentation
	i := f.indentation
	f.formatObjectExpressionBraces(n, false)
	f.setIndent(i)
}

func (f *formatter) formatObjectExpressionBraces(n *ObjectExpression, braces bool) {
	multiline := len(n.Properties) > 3

	if braces {
		f.writeRune('{')
	}

	if n.With != nil {
		f.formatIdentifier(n.With)
		f.writeString(" with ")
	}

	if multiline {
		f.writeRune('\n')
		f.indent()
		f.writeIndent()
	}

	var sep string
	if multiline {
		sep = ",\n"
	} else {
		sep = ", "
	}

	for i, c := range n.Properties {
		if i != 0 {
			f.writeString(sep)

			if multiline {
				f.writeIndent()
			}
		}

		f.formatNode(c)
	}

	if multiline {
		f.writeString(sep)
		f.unIndent()
		f.writeIndent()
	}

	if braces {
		f.writeRune('}')
	}
}

func (f *formatter) formatProperty(n *Property) {
	f.formatNode(n.Key)
	if n.Value != nil {
		f.writeString(": ")
		f.formatNode(n.Value)
	}
}

func (f *formatter) formatFunctionArgument(n *Property) {
	if n.Value == nil {
		f.formatNode(n.Key)
		return
	}

	f.formatNode(n.Key)
	f.writeRune('=')
	f.formatNode(n.Value)
}

func (f *formatter) formatIdentifier(n *Identifier) {
	f.writeString(n.Name)
}

func (f *formatter) formatStringExpression(n *StringExpression) {
	f.writeRune('"')
	for _, p := range n.Parts {
		f.formatStringExpressionPart(p)
	}
	f.writeRune('"')
}

func (f *formatter) formatStringExpressionPart(n StringExpressionPart) {
	switch p := n.(type) {
	case *TextPart:
		f.formatTextPart(p)
	case *InterpolatedPart:
		f.formatInterpolatedPart(p)
	}
}

func (f *formatter) formatTextPart(n *TextPart) {
	f.writeString(escapeStr(n.Value))
}

func (f *formatter) formatInterpolatedPart(n *InterpolatedPart) {
	f.writeString("${")
	f.formatNode(n.Expression)
	f.writeString("}")
}

func (f *formatter) formatParenExpression(n *ParenExpression) {
	f.formatNode(n.Expression)
}

func (f *formatter) formatStringLiteral(n *StringLiteral) {
	if n.Loc != nil && n.Loc.Source != "" {
		// Preserve the exact literal if we have it
		f.writeString(n.Loc.Source)
		return
	}
	// Write out escaped string value
	f.writeRune('"')
	f.writeString(escapeStr(n.Value))
	f.writeRune('"')
}

func escapeStr(s string) string {
	if !strings.ContainsAny(s, `"\`) {
		return s
	}
	var builder strings.Builder
	// Allocate for worst case where every rune needs to be escaped.
	builder.Grow(len(s) * 2)
	for _, r := range s {
		switch r {
		case '"', '\\':
			builder.WriteRune('\\')
		}
		builder.WriteRune(r)
	}
	return builder.String()
}

func (f *formatter) formatBooleanLiteral(n *BooleanLiteral) {
	f.writeString(strconv.FormatBool(n.Value))
}

func (f *formatter) formatDateTimeLiteral(n *DateTimeLiteral) {
	f.writeString(n.Value.Format(time.RFC3339Nano))
}

func (f *formatter) formatDurationLiteral(n *DurationLiteral) {
	formatDuration := func(d Duration) {
		f.writeString(strconv.FormatInt(d.Magnitude, 10))
		f.writeString(d.Unit)
	}

	for _, d := range n.Values {
		formatDuration(d)
	}
}

func (f *formatter) formatFloatLiteral(n *FloatLiteral) {
	sf := strconv.FormatFloat(n.Value, 'f', -1, 64)

	if !strings.Contains(sf, ".") {
		sf += ".0" // force to make it a float
	}

	f.writeString(sf)
}

func (f *formatter) formatIntegerLiteral(n *IntegerLiteral) {
	f.writeString(strconv.FormatInt(n.Value, 10))
}

func (f *formatter) formatUnsignedIntegerLiteral(n *UnsignedIntegerLiteral) {
	f.writeString(strconv.FormatUint(n.Value, 10))
}

func (f *formatter) formatPipeLiteral(_ *PipeLiteral) {
	f.writeString("<-")
}

func (f *formatter) formatRegexpLiteral(n *RegexpLiteral) {
	f.writeRune('/')
	f.writeString(strings.Replace(n.Value.String(), "/", "\\/", -1))
	f.writeRune('/')
}

func (f *formatter) formatNode(n Node) {
	//save current indentation
	currInd := f.indentation

	switch n := n.(type) {
	case *Package:
		f.formatPackage(n)
	case *File:
		f.formatFile(n, true)
	case *Block:
		f.formatBlock(n)
	case *PackageClause:
		f.formatPackageClause(n)
	case *ImportDeclaration:
		f.formatImportDeclaration(n)
	case *OptionStatement:
		f.formatOptionStatement(n)
	case *TestStatement:
		f.formatTestStatement(n)
	case *TestCaseStatement:
		f.formatTestCaseStatement(n)
	case *ExpressionStatement:
		f.formatExpressionStatement(n)
	case *ReturnStatement:
		f.formatReturnStatement(n)
	case *VariableAssignment:
		f.formatVariableAssignment(n)
	case *MemberAssignment:
		f.formatMemberAssignment(n)
	case *CallExpression:
		f.formatCallExpression(n)
	case *PipeExpression:
		f.formatPipeExpression(n)
	case *MemberExpression:
		f.formatMemberExpression(n)
	case *IndexExpression:
		f.formatIndexExpression(n)
	case *BinaryExpression:
		f.formatBinaryExpression(n)
	case *UnaryExpression:
		f.formatUnaryExpression(n)
	case *LogicalExpression:
		f.formatLogicalExpression(n)
	case *ObjectExpression:
		f.formatObjectExpression(n)
	case *ConditionalExpression:
		f.formatConditionalExpression(n)
	case *ArrayExpression:
		f.formatArrayExpression(n)
	case *Identifier:
		f.formatIdentifier(n)
	case *PipeLiteral:
		f.formatPipeLiteral(n)
	case *StringExpression:
		f.formatStringExpression(n)
	case *TextPart:
		f.formatTextPart(n)
	case *InterpolatedPart:
		f.formatInterpolatedPart(n)
	case *ParenExpression:
		f.formatParenExpression(n)
	case *StringLiteral:
		f.formatStringLiteral(n)
	case *BooleanLiteral:
		f.formatBooleanLiteral(n)
	case *FloatLiteral:
		f.formatFloatLiteral(n)
	case *IntegerLiteral:
		f.formatIntegerLiteral(n)
	case *UnsignedIntegerLiteral:
		f.formatUnsignedIntegerLiteral(n)
	case *RegexpLiteral:
		f.formatRegexpLiteral(n)
	case *DurationLiteral:
		f.formatDurationLiteral(n)
	case *DateTimeLiteral:
		f.formatDateTimeLiteral(n)
	case *FunctionExpression:
		f.formatFunctionExpression(n)
	case *Property:
		f.formatProperty(n)
	default:
		// If we were able not to find the type, than this switch is wrong
		panic(fmt.Errorf("unknown type %q", n.Type()))
	}

	// reset indentation
	f.setIndent(currInd)
}
