package interpreter

// Importer produces a package given an import path
type Importer interface {
	ImportPackageObject(path string) (*Package, error)
}
