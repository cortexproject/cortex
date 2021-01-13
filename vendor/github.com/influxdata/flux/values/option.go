package values

// Option is a value that has been declared as an option within the
// package. An option can be modified by another package from outside
// of the scope that the option was originally defined in, but it will
// affect the original scope it was defined in.
type Option struct {
	Value
}
