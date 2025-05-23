// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schema

import (
	"fmt"
	"strconv"

	"github.com/efficientgo/core/errors"

	"github.com/parquet-go/parquet-go"
)

type Builder struct {
	g parquet.Group

	metadata          map[string]string
	dataColDurationMs int64
	mint, maxt        int64
}

func NewBuilder(mint, maxt, colDuration int64) *Builder {
	b := &Builder{
		g:                 make(parquet.Group),
		dataColDurationMs: colDuration,
		metadata: map[string]string{
			DataColSizeMd: strconv.FormatInt(colDuration, 10),
			MaxTMd:        strconv.FormatInt(maxt, 10),
			MinTMd:        strconv.FormatInt(mint, 10),
		},
		mint: mint,
		maxt: maxt,
	}

	return b
}

func FromLabelsFile(lf *parquet.File) (*TSDBSchema, error) {
	md := MetadataToMap(lf.Metadata().KeyValueMetadata)
	mint, err := strconv.ParseInt(md[MinTMd], 0, 64)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert mint to int")
	}

	maxt, err := strconv.ParseInt(md[MaxTMd], 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert max to int")
	}

	dataColDurationMs, err := strconv.ParseInt(md[DataColSizeMd], 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert dataColDurationMs to int")
	}
	g := make(parquet.Group)

	b := &Builder{
		g:                 g,
		metadata:          md,
		mint:              mint,
		maxt:              maxt,
		dataColDurationMs: dataColDurationMs,
	}

	for _, c := range lf.Schema().Columns() {
		lbl, ok := ExtractLabelFromColumn(c[0])
		if !ok {
			continue
		}

		b.AddLabelNameColumn(lbl)
	}

	return b.Build()
}

func (b *Builder) AddLabelNameColumn(lbls ...string) {
	for _, lbl := range lbls {
		b.g[LabelToColumn(lbl)] = parquet.Optional(parquet.Encoded(parquet.String(), &parquet.RLEDictionary))
	}
}

func (b *Builder) Build() (*TSDBSchema, error) {
	colIdx := 0

	b.g[ColIndexes] = parquet.Encoded(parquet.Leaf(parquet.ByteArrayType), &parquet.DeltaByteArray)
	for i := b.mint; i <= b.maxt; i += b.dataColDurationMs {
		b.g[DataColumn(colIdx)] = parquet.Encoded(parquet.Leaf(parquet.ByteArrayType), &parquet.DeltaLengthByteArray)
		colIdx++
	}

	s := parquet.NewSchema("tsdb", b.g)

	dc := make([]int, colIdx)
	for i := range dc {
		lc, ok := s.Lookup(DataColumn(i))
		if !ok {
			return nil, fmt.Errorf("data column %v not found", DataColumn(i))
		}
		dc[i] = lc.ColumnIndex
	}

	return &TSDBSchema{
		Schema:            s,
		Metadata:          b.metadata,
		DataColDurationMs: b.dataColDurationMs,
		DataColsIndexes:   dc,
		MinTs:             b.mint,
		MaxTs:             b.maxt,
	}, nil
}

type TSDBSchema struct {
	Schema   *parquet.Schema
	Metadata map[string]string

	DataColsIndexes   []int
	MinTs, MaxTs      int64
	DataColDurationMs int64
}

type TSDBProjection struct {
	Schema       *parquet.Schema
	ExtraOptions []parquet.WriterOption
}

func (s *TSDBSchema) DataColumIdx(t int64) int {
	colIdx := 0

	for i := s.MinTs + s.DataColDurationMs; i <= t; i += s.DataColDurationMs {
		colIdx++
	}

	return colIdx
}

func (s *TSDBSchema) LabelsProjection() (*TSDBProjection, error) {
	g := make(parquet.Group)

	lc, ok := s.Schema.Lookup(ColIndexes)
	if !ok {
		return nil, fmt.Errorf("column %v not found", ColIndexes)
	}
	g[ColIndexes] = lc.Node

	for _, c := range s.Schema.Columns() {
		if _, ok := ExtractLabelFromColumn(c[0]); !ok {
			continue
		}
		lc, ok := s.Schema.Lookup(c...)
		if !ok {
			return nil, fmt.Errorf("column %v not found", c)
		}
		g[c[0]] = lc.Node
	}
	return &TSDBProjection{
		Schema: WithCompression(parquet.NewSchema("labels-projection", g)),
	}, nil
}

func (s *TSDBSchema) ChunksProjection() (*TSDBProjection, error) {
	g := make(parquet.Group)
	skipPageBoundsOpts := make([]parquet.WriterOption, 0, len(s.DataColsIndexes))

	for _, c := range s.Schema.Columns() {
		if ok := IsDataColumn(c[0]); !ok {
			continue
		}
		lc, ok := s.Schema.Lookup(c...)
		if !ok {
			return nil, fmt.Errorf("column %v not found", c)
		}
		g[c[0]] = lc.Node
		skipPageBoundsOpts = append(skipPageBoundsOpts, parquet.SkipPageBounds(c...))
	}

	return &TSDBProjection{
		Schema:       WithCompression(parquet.NewSchema("chunk-projection", g)),
		ExtraOptions: skipPageBoundsOpts,
	}, nil
}
