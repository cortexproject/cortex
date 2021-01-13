package values

type Package interface {
	Object

	// Name returns the package name.
	Name() string

	// Path returns the canonical import path for this package.
	Path() string
}

// SetOption will set an option on the package and return
// the value representing the option.
func SetOption(p Package, name string, v Value) (Value, bool) {
	// TODO(jsternberg): Setting an invalid option on a package wasn't previously
	// an error so it continues to not be an error. We should probably find a way
	// to make it so setting an invalid option is an error.
	opt, ok := p.Get(name)
	if ok {
		if opt, ok := opt.(*Option); ok {
			opt.Value = v
			return opt, ok
		}
	}
	return opt, false
}
