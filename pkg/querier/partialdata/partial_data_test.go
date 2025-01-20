package partialdata

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPartialData_Context(t *testing.T) {
	assert.False(t, FromContext(context.Background()))

	ctx := ContextWithPartialData(context.Background(), false)
	assert.False(t, FromContext(ctx))

	ctx = ContextWithPartialData(context.Background(), true)
	assert.True(t, FromContext(ctx))
}

func TestPartialData_ReturnPartialData(t *testing.T) {
	assert.False(t, ReturnPartialData(fmt.Errorf(""), false))
	assert.False(t, ReturnPartialData(fmt.Errorf(""), true))
	assert.False(t, ReturnPartialData(Error{}, false))
	assert.True(t, ReturnPartialData(Error{}, true))
}
