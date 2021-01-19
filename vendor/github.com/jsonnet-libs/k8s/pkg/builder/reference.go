package builder

type RefType struct {
	named
	to string
}

func Ref(name, to string) RefType {
	return RefType{named(name), to}
}

func (r RefType) String() string {
	return r.to
}
