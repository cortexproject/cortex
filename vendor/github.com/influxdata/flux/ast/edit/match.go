package edit

import (
	"fmt"

	"github.com/influxdata/flux/ast"
)

// Match takes an AST and a pattern and returns the nodes of the AST that
// match the given pattern.
// The pattern is an AST in turn, but it is partially specified, i.e. nil nodes are ignored
// while matching.
// In the case of slices (e.g. ast.File.Body, ast.CallExpression.Arguments) the matching
// mode can be "exact" or "fuzzy".
// Let ps be a slice of nodes in the pattern and ns in node.
// In "exact" mode, slices match iff len(ps) == len(ns) and every non-nil node ps[i] matches ns[i].
// In "fuzzy" mode, slices match iff ns is a superset of ps; e.g. if ps is empty, it matches every slice.
func Match(node ast.Node, pattern ast.Node, matchSlicesFuzzy bool) []ast.Node {
	var sms sliceMatchingStrategy
	if matchSlicesFuzzy {
		sms = &fuzzyMatchingStrategy{}
	} else {
		sms = &exactMatchingStrategy{}
	}
	mv := &matchVisitor{
		matched: make([]ast.Node, 0),
		pattern: pattern,
		sms:     sms,
	}
	ast.Walk(mv, node)
	return mv.matched
}

type matchVisitor struct {
	matched []ast.Node
	pattern ast.Node
	sms     sliceMatchingStrategy
}

func (mv *matchVisitor) Visit(node ast.Node) ast.Visitor {
	if match(mv.pattern, node, mv.sms) {
		mv.matched = append(mv.matched, node)
	}
	return mv
}

func (mv *matchVisitor) Done(node ast.Node) {}

type sliceMatchingStrategy interface {
	matchFiles(p, n []*ast.File) bool
	matchImportDeclarations(p, n []*ast.ImportDeclaration) bool
	matchStatements(p, n []ast.Statement) bool
	matchProperties(p, n []*ast.Property) bool
	matchExpressions(p, n []ast.Expression) bool
}

type fuzzyMatchingStrategy struct{}

func (fms *fuzzyMatchingStrategy) matchFiles(p, n []*ast.File) bool {
	// if the pattern slice is bigger than the node slice it can't be a subset
	if len(p) > len(n) {
		return false
	}
	// check if every node in p is also in n, but do not use the same
	// node in n twice
	used := make(map[int]bool)
	for i := 0; i < len(p); i++ {
		var matched bool
		for ii := 0; ii < len(n) && !matched; ii++ {
			if !used[ii] && match(p[i], n[ii], fms) {
				used[ii] = true
				matched = true
			}
		}
		if !matched {
			// found at least one node in the p that is not in n
			return false
		}
	}
	return true
}

func (fms *fuzzyMatchingStrategy) matchImportDeclarations(p, n []*ast.ImportDeclaration) bool {
	if len(p) > len(n) {
		return false
	}
	used := make(map[int]bool)
	for i := 0; i < len(p); i++ {
		var matched bool
		for ii := 0; ii < len(n) && !matched; ii++ {
			if !used[ii] && match(p[i], n[ii], fms) {
				used[ii] = true
				matched = true
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func (fms *fuzzyMatchingStrategy) matchStatements(p, n []ast.Statement) bool {
	if len(p) > len(n) {
		return false
	}
	used := make(map[int]bool)
	for i := 0; i < len(p); i++ {
		var matched bool
		for ii := 0; ii < len(n) && !matched; ii++ {
			if !used[ii] && match(p[i], n[ii], fms) {
				used[ii] = true
				matched = true
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func (fms *fuzzyMatchingStrategy) matchProperties(p, n []*ast.Property) bool {
	if len(p) > len(n) {
		return false
	}
	used := make(map[int]bool)
	for i := 0; i < len(p); i++ {
		var matched bool
		for ii := 0; ii < len(n) && !matched; ii++ {
			if !used[ii] && match(p[i], n[ii], fms) {
				used[ii] = true
				matched = true
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func (fms *fuzzyMatchingStrategy) matchExpressions(p, n []ast.Expression) bool {
	if len(p) > len(n) {
		return false
	}
	used := make(map[int]bool)
	for i := 0; i < len(p); i++ {
		var matched bool
		for ii := 0; ii < len(n) && !matched; ii++ {
			if !used[ii] && match(p[i], n[ii], fms) {
				used[ii] = true
				matched = true
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

type exactMatchingStrategy struct{}

func (ems *exactMatchingStrategy) matchFiles(p, n []*ast.File) bool {
	if len(p) != len(n) {
		return false
	}
	for i := 0; i < len(p); i++ {
		if !match(p[i], n[i], ems) {
			return false
		}
	}
	return true
}

func (ems *exactMatchingStrategy) matchImportDeclarations(p, n []*ast.ImportDeclaration) bool {
	if len(p) != len(n) {
		return false
	}
	for i := 0; i < len(p); i++ {
		if !match(p[i], n[i], ems) {
			return false
		}
	}
	return true
}

func (ems *exactMatchingStrategy) matchStatements(p, n []ast.Statement) bool {
	if len(p) != len(n) {
		return false
	}
	for i := 0; i < len(p); i++ {
		if !match(p[i], n[i], ems) {
			return false
		}
	}
	return true
}

func (ems *exactMatchingStrategy) matchProperties(p, n []*ast.Property) bool {
	if len(p) != len(n) {
		return false
	}
	for i := 0; i < len(p); i++ {
		if !match(p[i], n[i], ems) {
			return false
		}
	}
	return true
}

func (ems *exactMatchingStrategy) matchExpressions(p, n []ast.Expression) bool {
	if len(p) != len(n) {
		return false
	}
	for i := 0; i < len(p); i++ {
		if !match(p[i], n[i], ems) {
			return false
		}
	}
	return true
}

func match(pattern, node ast.Node, ms sliceMatchingStrategy) bool {
	if pattern == node {
		return true
	}
	// beware of untyped nils
	if pattern == nil {
		return true
	}
	if node == nil {
		return false
	}

	switch p := pattern.(type) {
	case *ast.Package:
		n, ok := node.(*ast.Package)
		if !ok {
			// nodes are of different type, they don't match.
			// cannot use p.Type() != n.Type() because, if p or n is a typed nil,
			// it would cause a nil dereference panic. I should do it below.
			// However, at this point, I just know they are of different type.
			return false
		}
		// beware of typed nils
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchPackage(p, n, ms)
	case *ast.File:
		n, ok := node.(*ast.File)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchFile(p, n, ms)
	case *ast.Block:
		n, ok := node.(*ast.Block)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchBlock(p, n, ms)
	case *ast.PackageClause:
		n, ok := node.(*ast.PackageClause)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchPackageClause(p, n, ms)
	case *ast.ImportDeclaration:
		n, ok := node.(*ast.ImportDeclaration)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchImportDeclaration(p, n, ms)
	case *ast.OptionStatement:
		n, ok := node.(*ast.OptionStatement)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchOptionStatement(p, n, ms)
	case *ast.ExpressionStatement:
		n, ok := node.(*ast.ExpressionStatement)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchExpressionStatement(p, n, ms)
	case *ast.ReturnStatement:
		n, ok := node.(*ast.ReturnStatement)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchReturnStatement(p, n, ms)
	case *ast.TestStatement:
		n, ok := node.(*ast.TestStatement)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchTestStatement(p, n, ms)
	case *ast.TestCaseStatement:
		n, ok := node.(*ast.TestCaseStatement)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchTestCaseStatement(p, n, ms)
	case *ast.VariableAssignment:
		n, ok := node.(*ast.VariableAssignment)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchVariableAssignment(p, n, ms)
	case *ast.MemberAssignment:
		n, ok := node.(*ast.MemberAssignment)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchMemberAssignment(p, n, ms)
	case *ast.CallExpression:
		n, ok := node.(*ast.CallExpression)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchCallExpression(p, n, ms)
	case *ast.PipeExpression:
		n, ok := node.(*ast.PipeExpression)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchPipeExpression(p, n, ms)
	case *ast.MemberExpression:
		n, ok := node.(*ast.MemberExpression)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchMemberExpression(p, n, ms)
	case *ast.IndexExpression:
		n, ok := node.(*ast.IndexExpression)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchIndexExpression(p, n, ms)
	case *ast.BinaryExpression:
		n, ok := node.(*ast.BinaryExpression)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchBinaryExpression(p, n, ms)
	case *ast.UnaryExpression:
		n, ok := node.(*ast.UnaryExpression)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchUnaryExpression(p, n, ms)
	case *ast.LogicalExpression:
		n, ok := node.(*ast.LogicalExpression)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchLogicalExpression(p, n, ms)
	case *ast.ObjectExpression:
		n, ok := node.(*ast.ObjectExpression)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchObjectExpression(p, n, ms)
	case *ast.ConditionalExpression:
		n, ok := node.(*ast.ConditionalExpression)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchConditionalExpression(p, n, ms)
	case *ast.ArrayExpression:
		n, ok := node.(*ast.ArrayExpression)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchArrayExpression(p, n, ms)
	case *ast.Identifier:
		n, ok := node.(*ast.Identifier)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchIdentifier(p, n, ms)
	case *ast.PipeLiteral:
		n, ok := node.(*ast.PipeLiteral)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchPipeLiteral(p, n, ms)
	case *ast.StringLiteral:
		n, ok := node.(*ast.StringLiteral)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchStringLiteral(p, n, ms)
	case *ast.BooleanLiteral:
		n, ok := node.(*ast.BooleanLiteral)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchBooleanLiteral(p, n, ms)
	case *ast.FloatLiteral:
		n, ok := node.(*ast.FloatLiteral)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchFloatLiteral(p, n, ms)
	case *ast.IntegerLiteral:
		n, ok := node.(*ast.IntegerLiteral)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchIntegerLiteral(p, n, ms)
	case *ast.UnsignedIntegerLiteral:
		n, ok := node.(*ast.UnsignedIntegerLiteral)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchUnsignedIntegerLiteral(p, n, ms)
	case *ast.RegexpLiteral:
		n, ok := node.(*ast.RegexpLiteral)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchRegexpLiteral(p, n, ms)
	case *ast.DurationLiteral:
		n, ok := node.(*ast.DurationLiteral)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchDurationLiteral(p, n, ms)
	case *ast.DateTimeLiteral:
		n, ok := node.(*ast.DateTimeLiteral)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchDateTimeLiteral(p, n, ms)
	case *ast.FunctionExpression:
		n, ok := node.(*ast.FunctionExpression)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchFunctionExpression(p, n, ms)
	case *ast.Property:
		n, ok := node.(*ast.Property)
		if !ok {
			return false
		}
		if p == nil {
			return true
		}
		if n == nil {
			return false
		}
		return matchProperty(p, n, ms)
	default:
		// If we were able not to find the type, than this switch is wrong
		panic(fmt.Errorf("unknown type %q", p.Type()))
	}
}

// empty strings are ignored
func matchString(p, n string) bool {
	if len(p) > 0 && p != n {
		return false
	}
	return true
}

// negative operators are ignored (invalid value for enumeration)
func matchOperator(p, n ast.OperatorKind) bool {
	if p >= 0 && p != n {
		return false
	}
	return true
}

func matchLogicalOperator(p, n ast.LogicalOperatorKind) bool {
	if p >= 0 && p != n {
		return false
	}
	return true
}

func matchPackage(p *ast.Package, n *ast.Package, ms sliceMatchingStrategy) bool {
	if !matchString(p.Path, n.Path) ||
		!matchString(p.Package, n.Package) {
		return false
	}
	return ms.matchFiles(p.Files, n.Files)
}

func matchFile(p *ast.File, n *ast.File, ms sliceMatchingStrategy) bool {
	if !matchString(p.Name, n.Name) {
		return false
	}
	if !match(p.Package, n.Package, ms) {
		return false
	}
	return ms.matchImportDeclarations(p.Imports, n.Imports) &&
		ms.matchStatements(p.Body, n.Body)
}

func matchBlock(p *ast.Block, n *ast.Block, ms sliceMatchingStrategy) bool {
	return ms.matchStatements(p.Body, n.Body)
}

func matchPackageClause(p *ast.PackageClause, n *ast.PackageClause, ms sliceMatchingStrategy) bool {
	return match(p.Name, n.Name, ms)
}

func matchImportDeclaration(p *ast.ImportDeclaration, n *ast.ImportDeclaration, ms sliceMatchingStrategy) bool {
	return match(p.As, n.As, ms) && match(p.Path, n.Path, ms)
}

func matchOptionStatement(p *ast.OptionStatement, n *ast.OptionStatement, ms sliceMatchingStrategy) bool {
	return match(p.Assignment, n.Assignment, ms)
}

func matchExpressionStatement(p *ast.ExpressionStatement, n *ast.ExpressionStatement, ms sliceMatchingStrategy) bool {
	return match(p.Expression, n.Expression, ms)
}

func matchReturnStatement(p *ast.ReturnStatement, n *ast.ReturnStatement, ms sliceMatchingStrategy) bool {
	return match(p.Argument, n.Argument, ms)
}

func matchTestStatement(p *ast.TestStatement, n *ast.TestStatement, ms sliceMatchingStrategy) bool {
	return match(p.Assignment, n.Assignment, ms)
}

func matchTestCaseStatement(p *ast.TestCaseStatement, n *ast.TestCaseStatement, ms sliceMatchingStrategy) bool {
	return match(p.ID, n.ID, ms) && match(p.Block, n.Block, ms)
}

func matchVariableAssignment(p *ast.VariableAssignment, n *ast.VariableAssignment, ms sliceMatchingStrategy) bool {
	return match(p.ID, n.ID, ms) && match(p.Init, n.Init, ms)
}

func matchMemberAssignment(p *ast.MemberAssignment, n *ast.MemberAssignment, ms sliceMatchingStrategy) bool {
	return match(p.Member, n.Member, ms) && match(p.Init, n.Init, ms)
}

func matchCallExpression(p *ast.CallExpression, n *ast.CallExpression, ms sliceMatchingStrategy) bool {
	if !match(p.Callee, n.Callee, ms) {
		return false
	}
	return ms.matchExpressions(p.Arguments, n.Arguments)
}

func matchPipeExpression(p *ast.PipeExpression, n *ast.PipeExpression, ms sliceMatchingStrategy) bool {
	return match(p.Argument, n.Argument, ms) && match(p.Call, n.Call, ms)
}

func matchMemberExpression(p *ast.MemberExpression, n *ast.MemberExpression, ms sliceMatchingStrategy) bool {
	return match(p.Object, n.Object, ms) && match(p.Property, n.Property, ms)
}

func matchIndexExpression(p *ast.IndexExpression, n *ast.IndexExpression, ms sliceMatchingStrategy) bool {
	return match(p.Array, n.Array, ms) && match(p.Index, n.Index, ms)
}

func matchBinaryExpression(p *ast.BinaryExpression, n *ast.BinaryExpression, ms sliceMatchingStrategy) bool {
	return matchOperator(p.Operator, n.Operator) && match(p.Left, n.Left, ms) && match(p.Right, n.Right, ms)
}

func matchUnaryExpression(p *ast.UnaryExpression, n *ast.UnaryExpression, ms sliceMatchingStrategy) bool {
	return matchOperator(p.Operator, n.Operator) && match(p.Argument, n.Argument, ms)
}

func matchLogicalExpression(p *ast.LogicalExpression, n *ast.LogicalExpression, ms sliceMatchingStrategy) bool {
	return matchLogicalOperator(p.Operator, n.Operator) && match(p.Left, n.Left, ms) && match(p.Right, n.Right, ms)
}

func matchObjectExpression(p *ast.ObjectExpression, n *ast.ObjectExpression, ms sliceMatchingStrategy) bool {
	return ms.matchProperties(p.Properties, n.Properties)
}

func matchConditionalExpression(p *ast.ConditionalExpression, n *ast.ConditionalExpression, ms sliceMatchingStrategy) bool {
	return match(p.Test, n.Test, ms) && match(p.Alternate, n.Alternate, ms) && match(p.Consequent, n.Consequent, ms)
}

func matchArrayExpression(p *ast.ArrayExpression, n *ast.ArrayExpression, ms sliceMatchingStrategy) bool {
	return ms.matchExpressions(p.Elements, n.Elements)
}

func matchIdentifier(p *ast.Identifier, n *ast.Identifier, ms sliceMatchingStrategy) bool {
	return matchString(p.Name, n.Name)
}

func matchPipeLiteral(p *ast.PipeLiteral, n *ast.PipeLiteral, ms sliceMatchingStrategy) bool {
	return true
}

// If one has specified a literal, the value must match as it is.
// In order to ignore a literal, don't specify it.
func matchStringLiteral(p *ast.StringLiteral, n *ast.StringLiteral, ms sliceMatchingStrategy) bool {
	return p.Value == n.Value
}

func matchBooleanLiteral(p *ast.BooleanLiteral, n *ast.BooleanLiteral, ms sliceMatchingStrategy) bool {
	return p.Value == n.Value
}

func matchFloatLiteral(p *ast.FloatLiteral, n *ast.FloatLiteral, ms sliceMatchingStrategy) bool {
	return p.Value == n.Value
}

func matchIntegerLiteral(p *ast.IntegerLiteral, n *ast.IntegerLiteral, ms sliceMatchingStrategy) bool {
	return p.Value == n.Value
}

func matchUnsignedIntegerLiteral(p *ast.UnsignedIntegerLiteral, n *ast.UnsignedIntegerLiteral, ms sliceMatchingStrategy) bool {
	return p.Value == n.Value
}

func matchRegexpLiteral(p *ast.RegexpLiteral, n *ast.RegexpLiteral, ms sliceMatchingStrategy) bool {
	return p.Value == n.Value
}

func matchDurationLiteral(p *ast.DurationLiteral, n *ast.DurationLiteral, ms sliceMatchingStrategy) bool {
	if len(p.Values) > len(n.Values) {
		return false
	}
	for i, el := range p.Values {
		if el != n.Values[i] {
			return false
		}
	}
	return true
}

func matchDateTimeLiteral(p *ast.DateTimeLiteral, n *ast.DateTimeLiteral, ms sliceMatchingStrategy) bool {
	return p.Value == n.Value
}

func matchFunctionExpression(p *ast.FunctionExpression, n *ast.FunctionExpression, ms sliceMatchingStrategy) bool {
	return ms.matchProperties(p.Params, n.Params)
}

func matchProperty(p *ast.Property, n *ast.Property, ms sliceMatchingStrategy) bool {
	return match(p.Key, n.Key, ms) && match(p.Value, n.Value, ms)
}
