package partialdata

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPartialData_ReturnPartialData(t *testing.T) {
	assert.False(t, IsPartialDataError(fmt.Errorf("")))
	assert.True(t, IsPartialDataError(ErrPartialData))
}
