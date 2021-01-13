package ast

import (
	"bytes"
	"fmt"
	"io"
	"strconv"

	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
)

// Check will inspect each node and annotate it with any AST errors.
// It will return the number of errors that were found.
func Check(root Node) int {
	v := errorVisitor{}
	Walk(&v, root)
	return v.count
}

// check will inspect a single node and annotate it with any AST errors.
// For convenience, it returns the number of errors that were identified.
func check(n Node) int {
	// TODO(jsternberg): Fill in the details for how we retrieve errors.
	switch n := n.(type) {
	case *BadStatement:
		loc := n.Location()
		// TODO(nathanielc): Remove the location information from the error message once we have a way to report the location information as part of the errors.
		n.Errors = append(n.Errors, Error{
			Msg: fmt.Sprintf("invalid statement %s@%d:%d-%d:%d: %s", loc.File, loc.Start.Line, loc.Start.Column, loc.End.Line, loc.End.Column, n.Text),
		})
	case *ObjectExpression:
		hasImplicit := false
		hasExplicit := false
		for _, p := range n.Properties {
			if p.BaseNode.Errors == nil {
				if p.Value == nil {
					hasImplicit = true
					if s, ok := p.Key.(*StringLiteral); ok {
						p.Errors = append(p.Errors, Error{
							Msg: fmt.Sprintf("string literal key %q must have a value", s.Value),
						})
					}
				} else {
					hasExplicit = true
				}
			} else {
				break
			}
		}
		if hasImplicit && hasExplicit {
			n.Errors = append(n.Errors, Error{
				Msg: fmt.Sprintf("cannot mix implicit and explicit properties"),
			})
		}
	case *PipeExpression:
		if n.Call == nil {
			n.Errors = append(n.Errors, Error{
				Msg: "pipe destination is missing",
			})
		}
	case *BinaryExpression:
		if n.Left == nil {
			n.Errors = append(n.Errors, Error{
				Msg: "missing left hand side of expression",
			})
		}
		if n.Right == nil {
			n.Errors = append(n.Errors, Error{
				Msg: "missing right hand side of expression",
			})
		}
		if n.Operator == 0 {
			n.Errors = append(n.Errors, Error{
				Msg: "expected an operator between two expressions",
			})
		}
	}

	return len(n.Errs())
}

// GetError will return the first error within an AST.
func GetError(n Node) error {
	errs := GetErrors(n)
	if len(errs) == 0 {
		return nil
	}
	return errs[0]
}

// GetErrors will return each of the errors within an AST.
func GetErrors(n Node) (errs []error) {
	Walk(CreateVisitor(func(node Node) {
		if nerrs := node.Errs(); len(nerrs) > 0 {
			for _, err := range nerrs {
				// Errors in the AST are a result of invalid Flux, so the error code should be codes.Invalid.
				errs = append(errs, errors.Wrapf(err, codes.Invalid, "loc %v", node.Location()))
			}
		}
	}), n)
	return errs
}

// PrintErrors will format the errors within the AST and output them
// to the writer.
func PrintErrors(w io.Writer, root Node) {
	var buf bytes.Buffer
	Walk(CreateVisitor(func(node Node) {
		if errs := node.Errs(); len(errs) > 0 {
			loc := node.Location()
			for _, err := range errs {
				buf.WriteString("error")
				if loc.Start.Line > 0 {
					buf.WriteByte(':')
					buf.WriteString(strconv.FormatInt(int64(loc.Start.Line), 10))
				}
				if loc.Start.Column > 0 {
					if buf.Len() > 0 {
						buf.WriteByte(':')
					}
					buf.WriteString(strconv.FormatInt(int64(loc.Start.Column), 10))
				}
				buf.WriteString(": ")
				buf.WriteString(err.Msg)
				buf.WriteByte('\n')
				_, _ = w.Write(buf.Bytes())
				buf.Reset()
			}
		}
	}), root)
}

type errorVisitor struct {
	count int
}

func (ev *errorVisitor) Visit(n Node) Visitor {
	ev.count += check(n)
	return ev
}

func (ev *errorVisitor) Done(n Node) {}
