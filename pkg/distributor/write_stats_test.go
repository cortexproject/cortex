package distributor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SetAndLoad(t *testing.T) {
	ws := &WriteStats{}

	t.Run("Samples", func(t *testing.T) {
		ws.SetSamples(3)
		assert.Equal(t, int64(3), ws.LoadSamples())
	})
	t.Run("Histograms", func(t *testing.T) {
		ws.SetHistograms(10)
		assert.Equal(t, int64(10), ws.LoadHistogram())
	})
	t.Run("Exemplars", func(t *testing.T) {
		ws.SetExemplars(2)
		assert.Equal(t, int64(2), ws.LoadExemplars())
	})
}

func Test_NilReceiver(t *testing.T) {
	var ws *WriteStats

	t.Run("Samples", func(t *testing.T) {
		ws.SetSamples(3)
		assert.Equal(t, int64(0), ws.LoadSamples())
	})
	t.Run("Histograms", func(t *testing.T) {
		ws.SetHistograms(10)
		assert.Equal(t, int64(0), ws.LoadHistogram())
	})
	t.Run("Exemplars", func(t *testing.T) {
		ws.SetExemplars(2)
		assert.Equal(t, int64(0), ws.LoadExemplars())
	})
}
