package partialdata

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPartialData_ReturnPartialData(t *testing.T) {
	assert.False(t, ReturnPartialData(fmt.Errorf(""), false))
	assert.False(t, ReturnPartialData(fmt.Errorf(""), true))
	assert.False(t, ReturnPartialData(Error{}, false))
	assert.True(t, ReturnPartialData(Error{}, true))
}
