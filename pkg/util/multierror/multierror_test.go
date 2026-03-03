package multierror

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiError_Add(t *testing.T) {
	tests := []struct {
		name     string
		errs     []error
		expected int
	}{
		{
			name:     "nil error is not added",
			errs:     []error{nil},
			expected: 0,
		},
		{
			name:     "non-nil error is added",
			errs:     []error{errors.New("err1")},
			expected: 1,
		},
		{
			name:     "multiple errors are added",
			errs:     []error{errors.New("err1"), errors.New("err2"), errors.New("err3")},
			expected: 3,
		},
		{
			name:     "nil errors are skipped among non-nil",
			errs:     []error{errors.New("err1"), nil, errors.New("err2")},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var me MultiError
			for _, err := range tt.errs {
				me.Add(err)
			}
			assert.Len(t, me, tt.expected)
		})
	}
}

func TestMultiError_Add_flattensNestedMultiError(t *testing.T) {
	inner := MultiError{}
	inner.Add(errors.New("inner1"))
	inner.Add(errors.New("inner2"))

	var outer MultiError
	outer.Add(inner.Err())

	// Inner errors should be flattened into outer, not nested as a single error.
	assert.Len(t, outer, 2)
	assert.EqualError(t, outer[0], "inner1")
	assert.EqualError(t, outer[1], "inner2")
}

func TestMultiError_Err(t *testing.T) {
	tests := []struct {
		name      string
		errs      []error
		expectNil bool
	}{
		{
			name:      "empty returns nil",
			errs:      nil,
			expectNil: true,
		},
		{
			name:      "with errors returns non-nil",
			errs:      []error{errors.New("err1")},
			expectNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			me := New(tt.errs...)
			result := me.Err()
			if tt.expectNil {
				assert.NoError(t, result)
			} else {
				assert.Error(t, result)
			}
		})
	}
}

func TestNonNilMultiError_Error(t *testing.T) {
	tests := []struct {
		name     string
		errs     []error
		expected string
	}{
		{
			name:     "single error has no prefix",
			errs:     []error{errors.New("something failed")},
			expected: "something failed",
		},
		{
			name:     "multiple errors have count prefix",
			errs:     []error{errors.New("err1"), errors.New("err2")},
			expected: "2 errors: err1; err2",
		},
		{
			name:     "three errors",
			errs:     []error{errors.New("a"), errors.New("b"), errors.New("c")},
			expected: "3 errors: a; b; c",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			me := New(tt.errs...)
			err := me.Err()
			require.Error(t, err)
			assert.Equal(t, tt.expected, err.Error())
		})
	}
}

func TestNew(t *testing.T) {
	tests := []struct {
		name     string
		errs     []error
		expected int
	}{
		{
			name:     "no arguments",
			errs:     nil,
			expected: 0,
		},
		{
			name:     "nil errors are filtered out",
			errs:     []error{nil, nil},
			expected: 0,
		},
		{
			name:     "mixed nil and non-nil",
			errs:     []error{nil, errors.New("err1"), nil, errors.New("err2")},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			me := New(tt.errs...)
			assert.Len(t, me, tt.expected)
		})
	}
}
