package errors

import (
	"testing"

	"github.com/pkg/errors"
)

var (
	errToTest = errors.New("err")
)

func TestErrorIs(t *testing.T) {
	type args struct {
		err error
		f   func(err error) bool
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "should unwrap error",
			want: true,
			args: args{
				err: errors.Wrap(errors.Wrap(errToTest, "outer1"), "outer2"),
				f: func(err error) bool {
					return err == errToTest
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ErrorIs(tt.args.err, tt.args.f); got != tt.want {
				t.Errorf("ErrorIs() = %v, want %v", got, tt.want)
			}
		})
	}
}
