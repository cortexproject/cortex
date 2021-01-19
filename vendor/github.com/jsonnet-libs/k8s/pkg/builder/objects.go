package builder

import (
	"fmt"
	"strings"
)

const (
	SeparatorLong    = ",\n"
	SeparatorConcise = ", "
)

// Objects (dicts)
type ObjectType struct {
	named
	order    []string
	children map[string]Type
	concise  bool
}

func Object(name string, children ...Type) ObjectType {
	c := make(map[string]Type)
	order := make([]string, len(children))
	i := 0
	for _, child := range children {
		if v, ok := c[child.Name()]; ok {
			panic(fmt.Sprintf("key clash: trying to add `%v` as `%s`, but `%v` already uses this key", child, child.Name(), v))
		}

		key := escapeKey(child.Name())
		c[key] = child
		order[i] = key
		i++
	}

	return ObjectType{
		named:    named(name),
		children: c,
		order:    order,
	}
}

func escapeKey(s string) string {
	switch s {
	case "local", "error":
		return fmt.Sprintf(`'%s'`, s)
	default:
		if strings.HasPrefix(s, "#") {
			return fmt.Sprintf(`'%s'`, s)
		}
		return s
	}
}

func ConciseObject(name string, children ...Type) ObjectType {
	o := Object(name, children...)
	o.concise = true
	return o
}

func (o ObjectType) String() string {
	if len(o.children) == 0 {
		return "{}"
	}
	if o.concise {
		return o.ConciseString()
	}

	s := printChildren(o.children, o.order, SeparatorLong)
	return fmt.Sprintf("{\n%s\n}", indent(s))
}

func (o ObjectType) ConciseString() string {
	s := printChildren(o.children, o.order, SeparatorConcise)
	return fmt.Sprintf("{ %s }", strings.TrimSuffix(s, ","))
}

func printChildren(children map[string]Type, order []string, s string) string {
	j := ""
	for _, name := range order {
		c := children[name]
		colon := ":"
		value := c.String()

		if cmt, ok := c.(CommentType); ok {
			c = cmt.value
			if s == SeparatorLong {
				j += cmt.Comment() + "\n"
			}
		}

		switch t := c.(type) {
		case FuncType:
			name = fmt.Sprintf("%s(%s)", name, t.Args())
		case HiddenType:
			colon = "::"
			switch h := t.value.(type) {
			case MergeType:
				colon = "+::"
			case FuncType:
				name = fmt.Sprintf("%s(%s)", name, h.Args())
			}
		case LocalType:
			colon = " ="
			// using t.Name() here (unescaped), cause we define an identifier
			name = "local " + t.Name()
		case MergeType:
			colon = "+:"
		}

		j += fmt.Sprintf("%s%s %s"+s, name, colon, value)

	}
	j = strings.TrimSuffix(j, s)
	return j
}
