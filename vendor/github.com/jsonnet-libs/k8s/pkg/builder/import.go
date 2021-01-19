package builder

import "fmt"

type ImportType struct {
	named
	pkg string
	raw bool
}

func (i ImportType) String() string {
	op := "import"
	if i.raw {
		op += "str"
	}

	return fmt.Sprintf(`(%s '%s')`, op, i.pkg)
}

func Import(name, pkg string) ImportType {
	return ImportType{named: named(name), pkg: pkg, raw: false}
}

func ImportStr(name, pkg string) ImportType {
	return ImportType{named: named(name), pkg: pkg, raw: true}
}
