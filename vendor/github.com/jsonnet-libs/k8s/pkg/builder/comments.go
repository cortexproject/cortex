package builder

import "strings"

type CommentType struct {
	comment string
	value   Type
}

func Comment(value Type, comment string) CommentType {
	return CommentType{
		comment: comment,
		value:   value,
	}
}

func (t CommentType) String() string {
	return t.value.String()
}

func (t CommentType) Name() string {
	return t.value.Name()
}

func (t CommentType) Comment() string {
	lines := strings.Split(t.comment, "\n")
	for i := range lines {
		lines[i] = "// " + lines[i]
	}
	return strings.Join(lines, "\n")
}
