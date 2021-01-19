package builder

type LocalType struct {
	value Type
}

func Local(value Type) LocalType {
	return LocalType{value}
}

func (t LocalType) String() string {
	return t.value.String()
}

func (t LocalType) Name() string {
	return t.value.Name()
}
