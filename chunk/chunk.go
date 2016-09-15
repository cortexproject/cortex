// Copyright 2016 The Prometheus Authors

package chunk

import (
	"github.com/prometheus/common/model"
)

// Chunk contains encoded timeseries data
type Chunk struct {
	ID      string       `json:"-"`
	From    model.Time   `json:"from"`
	Through model.Time   `json:"through"`
	Metric  model.Metric `json:"metric"`
	Data    []byte       `json:"-"`
}

// ByID allow you to sort chunks by ID
type ByID []Chunk

func (cs ByID) Len() int           { return len(cs) }
func (cs ByID) Swap(i, j int)      { cs[i], cs[j] = cs[j], cs[i] }
func (cs ByID) Less(i, j int) bool { return cs[i].ID < cs[j].ID }
