package codec

type Clonable interface {
	// Clone should return a deep copy of the state.
	Clone() interface{}
}
