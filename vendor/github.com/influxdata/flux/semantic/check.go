package semantic

import (
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
)

func runChecks(n Node, vars, opts map[string]bool) error {
	// Check for options declared below package block.
	// Returns a list of option statements visited.
	stmts, err := optionStatements(n)
	if err != nil {
		return err
	}
	// Check for option reassignments in package block.
	if err := optionReAssignments(stmts, vars, opts); err != nil {
		return err
	}
	// Check for variable reassignments.
	if err := varReAssignments(n, vars, opts); err != nil {
		return err
	}
	// Check for dependencies among options.
	if err := optionDependencies(stmts, opts); err != nil {
		return err
	}
	return nil
}

func optionStatements(n Node) ([]*OptionStatement, error) {
	var stmts []*OptionStatement
	var errStmt *OptionStatement
	errFn := func(n *OptionStatement) {
		errStmt = n
	}
	optFn := func(n *OptionStatement) {
		stmts = append(stmts, n)
	}
	visitor := optionStmtVisitor{
		optFn: optFn,
		errFn: errFn,
	}
	Walk(NewScopedVisitor(visitor), n)
	if errStmt != nil {
		name, err := optionName(errStmt)
		if err != nil {
			return nil, err
		}
		return nil, errors.Newf(codes.Invalid, "option %q declared below package block at %v", name, errStmt.Location())
	}
	return stmts, nil
}

func optionName(opt *OptionStatement) (string, error) {
	switch n := opt.Assignment.(type) {
	case *NativeVariableAssignment:
		return n.Identifier.Name, nil
	case *MemberAssignment:
		obj := n.Member.Object
		id, ok := obj.(*IdentifierExpression)
		if !ok {
			return "", errors.Newf(codes.Invalid, "unsupported option qualifier %T", obj)
		}
		return id.Name + "." + n.Member.Property, nil
	default:
		return "", errors.Newf(codes.Internal, "unsupported assignment %T", n)
	}
}

// option statement visitor.
// option statments nested within a package block are passed to errFn.
type optionStmtVisitor struct {
	optFn  func(*OptionStatement)
	errFn  func(*OptionStatement)
	nested bool
}

func (v optionStmtVisitor) Visit(node Node) Visitor {
	if n, ok := node.(*OptionStatement); ok {
		if v.nested {
			v.errFn(n)
		} else {
			v.optFn(n)
		}
		return nil
	}
	return v
}

func (v optionStmtVisitor) Nest() NestingVisitor {
	v.nested = true
	return v
}

func (v optionStmtVisitor) Done(node Node) {}

func optionReAssignments(stmts []*OptionStatement, vars, options map[string]bool) error {
	for _, stmt := range stmts {
		name, err := optionName(stmt)
		if err != nil {
			return err
		}
		if options[name] {
			return errors.Newf(codes.Invalid, "option %q redeclared at %v", name, stmt.Location())
		}
		if vars[name] {
			return errors.Newf(codes.Invalid, "cannot declare option %q at %v; variable with same name already declared", name, stmt.Location())
		}
		options[name] = true
	}
	return nil
}

func varReAssignments(n Node, vars, opts map[string]bool) error {
	var varDec, optDec *NativeVariableAssignment
	varFn := func(n *NativeVariableAssignment) {
		varDec = n
	}
	optFn := func(n *NativeVariableAssignment) {
		optDec = n
	}
	visitor := varStmtVisitor{
		vars:  vars,
		opts:  opts,
		varFn: varFn,
		optFn: optFn,
	}
	Walk(NewScopedVisitor(visitor), n)
	if varDec != nil {
		name := varDec.Identifier.Name
		return errors.Newf(codes.Invalid, "var %q redeclared at %v", name, varDec.Location())
	}
	if optDec != nil {
		name := optDec.Identifier.Name
		return errors.Newf(codes.Invalid, "cannot declare variable %q at %v; option with same name already declared", name, optDec.Location())
	}
	return nil
}

// variable assignment visitor.
// variable reassignments are passed to errFn.
type varStmtVisitor struct {
	vars, opts   map[string]bool
	varFn, optFn func(*NativeVariableAssignment)
	option       bool
}

func (v varStmtVisitor) Visit(node Node) Visitor {
	switch n := node.(type) {
	case *OptionStatement:
		v.option = true
	case *NativeVariableAssignment:
		name := n.Identifier.Name
		if v.option {
			v.option = false
			return v
		} else if v.vars[name] {
			v.varFn(n)
			return nil
		} else if v.opts[name] {
			v.optFn(n)
			return nil
		}
		v.vars[name] = true
	case *FunctionParameter:
		v.vars[n.Key.Name] = true
	}
	return v
}

func (v varStmtVisitor) Nest() NestingVisitor {
	v.vars = make(map[string]bool)
	v.opts = make(map[string]bool)
	return v
}

func (v varStmtVisitor) Done(node Node) {}

func optionDependencies(stmts []*OptionStatement, options map[string]bool) error {
	var dep *IdentifierExpression
	errFn := func(n *IdentifierExpression) {
		dep = n
	}
	visitor := optionExprVisitor{
		option: options,
		shadow: make(map[string]bool, len(options)),
		errFn:  errFn,
	}
	for _, stmt := range stmts {
		// Externally declared options may have dependencies
		// on other options.
		n, ok := stmt.Assignment.(*NativeVariableAssignment)
		if !ok {
			continue
		}
		name := n.Identifier.Name
		Walk(NewScopedVisitor(visitor), n.Init)
		if dep != nil {
			return errors.Newf(codes.Invalid, "option dependency: option %q depends on option %q defined in the same package at %v", name, dep.Name, dep.Location())
		}
	}
	return nil
}

// option expression visitor.
// references to non-qualified options are passed to errFn.
type optionExprVisitor struct {
	option map[string]bool
	shadow map[string]bool
	errFn  func(*IdentifierExpression)
}

func (v optionExprVisitor) Visit(node Node) Visitor {
	switch n := node.(type) {
	case *NativeVariableAssignment:
		// var declarations shadow options
		v.shadow[n.Identifier.Name] = true
	case *FunctionParameter:
		// function params shadow options
		v.shadow[n.Key.Name] = true
	case *IdentifierExpression:
		if v.option[n.Name] && !v.shadow[n.Name] {
			v.errFn(n)
			return nil
		}
	}
	return v
}

func (v optionExprVisitor) Nest() NestingVisitor {
	shadows := make(map[string]bool, len(v.shadow))
	for k, v := range v.shadow {
		shadows[k] = v
	}
	v.shadow = shadows
	return v
}

func (v optionExprVisitor) Done(node Node) {}
