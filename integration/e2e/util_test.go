package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeToMilliseconds(t *testing.T) {
	input := time.Unix(0, 1614091978491118000)
	assert.Equal(t, int64(1614091978491), TimeToMilliseconds(input))

	input = time.Unix(0, 1614091978505374000)
	assert.Equal(t, int64(1614091978505), TimeToMilliseconds(input))

	input = time.Unix(0, 1614091921796500000)
	assert.Equal(t, int64(1614091921796), TimeToMilliseconds(input))

	input = time.Unix(0, 1614092667692610000)
	assert.Equal(t, int64(1614092667693), TimeToMilliseconds(input))
}
