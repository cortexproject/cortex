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
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/parquet-go/parquet-go/format"
)

const (
	LabelColumnPrefix = "l_"
	DataColumnPrefix  = "s_data_"
	ColIndexes        = "s_col_indexes"

	DataColSizeMd = "data_col_duration_ms"
	MinTMd        = "minT"
	MaxTMd        = "maxT"
)

func LabelToColumn(lbl string) string {
	return fmt.Sprintf("%s%s", LabelColumnPrefix, lbl)
}

func ExtractLabelFromColumn(col string) (string, bool) {
	if !strings.HasPrefix(col, LabelColumnPrefix) {
		return "", false
	}

	return strings.TrimPrefix(col, LabelColumnPrefix), true
}

func IsDataColumn(col string) bool {
	return strings.HasPrefix(col, DataColumnPrefix)
}

func DataColumn(i int) string {
	return fmt.Sprintf("%s%v", DataColumnPrefix, i)
}

func LabelsPfileNameForShard(name string, shard int) string {
	return fmt.Sprintf("%s/%d.%s", name, shard, "labels.parquet")
}

func ChunksPfileNameForShard(name string, shard int) string {
	return fmt.Sprintf("%s/%d.%s", name, shard, "chunks.parquet")
}

func WithCompression(s *parquet.Schema) *parquet.Schema {
	g := make(parquet.Group)

	for _, c := range s.Columns() {
		lc, _ := s.Lookup(c...)
		g[lc.Path[0]] = parquet.Compressed(lc.Node, &zstd.Codec{Level: zstd.SpeedBetterCompression})
	}

	return parquet.NewSchema("compressed", g)
}

func MetadataToMap(md []format.KeyValue) map[string]string {
	r := make(map[string]string, len(md))
	for _, kv := range md {
		r[kv.Key] = kv.Value
	}
	return r
}
