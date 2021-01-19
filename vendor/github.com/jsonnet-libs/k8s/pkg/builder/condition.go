package builder

import "fmt"

type ConditionType struct {
	name string
	If   Type
	Then Type
	Else Type
}

func (c ConditionType) Name() string {
	return c.name
}

func (c ConditionType) String() string {
	return fmt.Sprintf("if %s then %s else %s",
		c.If.String(),
		c.Then.String(),
		c.Else.String(),
	)
}

func IfThenElse(name string, If, Then, Else Type) ConditionType {
	return ConditionType{
		name: name,
		If:   If,
		Then: Then,
		Else: Else,
	}
}
