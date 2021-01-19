// Package builder implements a simple way to generate arbitrary Jsonnet
// directly from Go, using a functional API inspired by how Jsonnet is written
// itself.
package builder

import (
	"fmt"
	"strings"
)

type named string

func (n named) Name() string {
	return string(n)
}

type Doc struct {
	Locals []LocalType
	Root   Type
}

func (d Doc) String() string {
	s := ""
	for _, l := range d.Locals {
		s += fmt.Sprintf("local %s = %s;\n", l.Name(), l.String())
	}

	s += d.Root.String()
	return s
}

type Type interface {
	String() string
	Name() string
}

func indent(s string) string {
	split := strings.Split(s, "\n")
	for i := range split {
		split[i] = "  " + split[i]
	}
	return strings.Join(split, "\n")
}

func dedent(s string) string {
	split := strings.Split(s, "\n")
	for i := range split {
		split[i] = strings.TrimPrefix(split[i], "  ")
	}
	return strings.Join(split, "\n")
}
