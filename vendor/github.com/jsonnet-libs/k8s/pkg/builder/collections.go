package builder

import (
	"fmt"
	"strings"
)

// Lists (arrays)
type ListType struct {
	named
	items []Type
}

func List(name string, items ...Type) ListType {
	return ListType{named: named(name), items: items}
}

func (t ListType) String() string {
	s := ""
	for _, l := range t.items {
		s += fmt.Sprintf(", %s", l.String())
	}
	s = strings.TrimPrefix(s, ", ")
	return fmt.Sprintf("[%s]", s)
}
