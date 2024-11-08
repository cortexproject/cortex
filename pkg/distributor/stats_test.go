package distributor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SetAndLoad(t *testing.T) {
	s := &WriteStats{}

	t.Run("Samples", func(t *testing.T) {
		s.SetSamples(3)
		assert.Equal(t, int64(3), s.LoadSamples())
	})
	t.Run("Histograms", func(t *testing.T) {
		s.SetHistograms(10)
		assert.Equal(t, int64(10), s.LoadHistogram())
	})
	t.Run("Exemplars", func(t *testing.T) {
		s.SetExemplars(2)
		assert.Equal(t, int64(2), s.LoadExemplars())
	})
}

func Test_NilReceiver(t *testing.T) {
	var s *WriteStats

	t.Run("Samples", func(t *testing.T) {
		s.SetSamples(3)
		assert.Equal(t, int64(0), s.LoadSamples())
	})
	t.Run("Histograms", func(t *testing.T) {
		s.SetHistograms(10)
		assert.Equal(t, int64(0), s.LoadHistogram())
	})
	t.Run("Exemplars", func(t *testing.T) {
		s.SetExemplars(2)
		assert.Equal(t, int64(0), s.LoadExemplars())
	})
}
