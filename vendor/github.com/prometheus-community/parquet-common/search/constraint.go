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

package search

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus-community/parquet-common/util"
)

type Constraint interface {
	fmt.Stringer

	// filter returns a set of non-overlapping increasing row indexes that may satisfy the constraint.
	filter(ctx context.Context, rgIdx int, primary bool, rr []RowRange) ([]RowRange, error)
	// init initializes the constraint with respect to the file schema and projections.
	init(f *storage.ParquetFile) error
	// path is the path for the column that is constrained
	path() string
}

func MatchersToConstraint(matchers ...*labels.Matcher) ([]Constraint, error) {
	r := make([]Constraint, 0, len(matchers))
	for _, matcher := range matchers {
		switch matcher.Type {
		case labels.MatchEqual:
			r = append(r, Equal(schema.LabelToColumn(matcher.Name), parquet.ValueOf(matcher.Value)))
		case labels.MatchNotEqual:
			r = append(r, Not(Equal(schema.LabelToColumn(matcher.Name), parquet.ValueOf(matcher.Value))))
		case labels.MatchRegexp:
			res, err := labels.NewFastRegexMatcher(matcher.Value)
			if err != nil {
				return nil, err
			}
			r = append(r, Regex(schema.LabelToColumn(matcher.Name), res))
		case labels.MatchNotRegexp:
			res, err := labels.NewFastRegexMatcher(matcher.Value)
			if err != nil {
				return nil, err
			}
			r = append(r, Not(Regex(schema.LabelToColumn(matcher.Name), res)))
		default:
			return nil, fmt.Errorf("unsupported matcher type %s", matcher.Type)
		}
	}
	return r, nil
}

func Initialize(f *storage.ParquetFile, cs ...Constraint) error {
	for i := range cs {
		if err := cs[i].init(f); err != nil {
			return fmt.Errorf("unable to initialize constraint %d: %w", i, err)
		}
	}
	return nil
}

func Filter(ctx context.Context, s storage.ParquetShard, rgIdx int, cs ...Constraint) ([]RowRange, error) {
	rg := s.LabelsFile().RowGroups()[rgIdx]
	// Constraints for sorting columns are cheaper to evaluate, so we sort them first.
	sc := rg.SortingColumns()

	var n int
	for i := range sc {
		if n == len(cs) {
			break
		}
		for j := range cs {
			if cs[j].path() == sc[i].Path()[0] {
				cs[n], cs[j] = cs[j], cs[n]
				n++
			}
		}
	}
	var err error
	rr := []RowRange{{From: int64(0), Count: rg.NumRows()}}
	for i := range cs {
		isPrimary := len(sc) > 0 && cs[i].path() == sc[0].Path()[0]
		rr, err = cs[i].filter(ctx, rgIdx, isPrimary, rr)
		if err != nil {
			return nil, fmt.Errorf("unable to filter with constraint %d: %w", i, err)
		}
	}
	return rr, nil
}

type pageToRead struct {
	// for data pages
	pfrom int64
	pto   int64

	idx int

	// for data and dictionary pages
	off int
	csz int
}

// symbolTable is a helper that can decode the i-th value of a page.
// Using it we only need to allocate an int32 slice and not a slice of
// string values.
// It only works for optional dictionary encoded columns. All of our label
// columns are that though.
type symbolTable struct {
	dict parquet.Dictionary
	syms []int32
}

func (s *symbolTable) Get(i int) parquet.Value {
	switch s.syms[i] {
	case -1:
		return parquet.NullValue()
	default:
		return s.dict.Index(s.syms[i])
	}
}

func (s *symbolTable) GetIndex(i int) int32 {
	return s.syms[i]
}

func (s *symbolTable) Reset(pg parquet.Page) {
	dict := pg.Dictionary()
	data := pg.Data()
	syms := data.Int32()
	defs := pg.DefinitionLevels()

	if s.syms == nil {
		s.syms = make([]int32, len(defs))
	} else {
		s.syms = slices.Grow(s.syms, len(defs))[:len(defs)]
	}

	sidx := 0
	for i := range defs {
		if defs[i] == 1 {
			s.syms[i] = syms[sidx]
			sidx++
		} else {
			s.syms[i] = -1
		}
	}
	s.dict = dict
}

type equalConstraint struct {
	pth string

	val parquet.Value
	f   *storage.ParquetFile

	comp func(l, r parquet.Value) int
}

func (ec *equalConstraint) String() string {
	return fmt.Sprintf("equal(%q,%q)", ec.pth, ec.val)
}

func Equal(path string, value parquet.Value) Constraint {
	return &equalConstraint{pth: path, val: value}
}

func (ec *equalConstraint) filter(ctx context.Context, rgIdx int, primary bool, rr []RowRange) ([]RowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	from, to := rr[0].From, rr[len(rr)-1].From+rr[len(rr)-1].Count

	rg := ec.f.RowGroups()[rgIdx]

	col, ok := rg.Schema().Lookup(ec.path())
	if !ok {
		// If match empty, return rr (filter nothing)
		// otherwise return empty
		if ec.matches(parquet.ValueOf("")) {
			return rr, nil
		}
		return []RowRange{}, nil
	}
	cc := rg.ColumnChunks()[col.ColumnIndex]

	if skip, err := ec.skipByBloomfilter(cc); err != nil {
		return nil, fmt.Errorf("unable to skip by bloomfilter: %w", err)
	} else if skip {
		return nil, nil
	}

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}
	res := make([]RowRange, 0)

	readPgs := make([]pageToRead, 0, 10)

	for i := 0; i < cidx.NumPages(); i++ {
		poff, pcsz := uint64(oidx.Offset(i)), oidx.CompressedPageSize(i)

		// If page does not intersect from, to; we can immediately discard it
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < oidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		// Page intersects [from, to] but we might be able to discard it with statistics
		if cidx.NullPage(i) {
			if ec.matches(parquet.ValueOf("")) {
				res = append(res, RowRange{pfrom, pcount})
			}
			continue
		}
		// If we are not matching the empty string ( which would be satisfied by Null too ), we can
		// use page statistics to skip rows
		minv, maxv := cidx.MinValue(i), cidx.MaxValue(i)
		if !ec.matches(parquet.ValueOf("")) && !maxv.IsNull() && ec.comp(ec.val, maxv) > 0 {
			if cidx.IsDescending() {
				break
			}
			continue
		}
		if !ec.matches(parquet.ValueOf("")) && !minv.IsNull() && ec.comp(ec.val, minv) < 0 {
			if cidx.IsAscending() {
				break
			}
			continue
		}
		// We cannot discard the page through statistics but we might need to read it to see if it has the value
		readPgs = append(readPgs, pageToRead{pfrom: pfrom, pto: pto, idx: i, off: int(poff), csz: int(pcsz)})
	}

	// Did not find any pages
	if len(readPgs) == 0 {
		return intersectRowRanges(simplify(res), rr), nil
	}

	dictOff, dictSz := ec.f.DictionaryPageBounds(rgIdx, col.ColumnIndex)

	minOffset := uint64(readPgs[0].off)
	maxOffset := readPgs[len(readPgs)-1].off + readPgs[len(readPgs)-1].csz

	// If the gap between the first page and the dic page is less than PagePartitioningMaxGapSize,
	// we include the dic to be read in the single read
	if int(minOffset-(dictOff+dictSz)) < ec.f.Cfg.PagePartitioningMaxGapSize {
		minOffset = dictOff
	}

	pgs, err := ec.f.GetPages(ctx, cc, int64(minOffset), int64(maxOffset))
	if err != nil {
		return nil, err
	}

	defer func() { _ = pgs.Close() }()

	symbols := new(symbolTable)
	for _, p := range readPgs {
		pfrom := p.pfrom
		pto := p.pto

		if err := pgs.SeekToRow(pfrom); err != nil {
			return nil, fmt.Errorf("unable to seek to row: %w", err)
		}
		pg, err := pgs.ReadPage()
		if err != nil {
			return nil, fmt.Errorf("unable to read page: %w", err)
		}

		symbols.Reset(pg)

		// The page has the value, we need to find the matching row ranges
		n := int(pg.NumRows())
		bl := int(max(pfrom, from) - pfrom)
		br := n - int(pto-min(pto, to))
		var l, r int
		switch {
		case cidx.IsAscending() && primary:
			l = sort.Search(n, func(i int) bool { return ec.comp(ec.val, symbols.Get(i)) <= 0 })
			r = sort.Search(n, func(i int) bool { return ec.comp(ec.val, symbols.Get(i)) < 0 })

			if lv, rv := max(bl, l), min(br, r); rv > lv {
				res = append(res, RowRange{pfrom + int64(lv), int64(rv - lv)})
			}
		default:
			off, count := bl, 0
			for j := bl; j < br; j++ {
				if !ec.matches(symbols.Get(j)) {
					if count != 0 {
						res = append(res, RowRange{pfrom + int64(off), int64(count)})
					}
					off, count = j, 0
				} else {
					if count == 0 {
						off = j
					}
					count++
				}
			}
			if count != 0 {
				res = append(res, RowRange{pfrom + int64(off), int64(count)})
			}
		}
	}

	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}

func (ec *equalConstraint) init(f *storage.ParquetFile) error {
	c, ok := f.Schema().Lookup(ec.path())
	ec.f = f
	if !ok {
		return nil
	}
	stringKind := parquet.String().Type().Kind()
	if ec.val.Kind() != stringKind {
		return fmt.Errorf("schema: can only search string kind, got: %s", ec.val.Kind())
	}
	if c.Node.Type().Kind() != stringKind {
		return fmt.Errorf("schema: cannot search value of kind %s in column of kind %s", stringKind, c.Node.Type().Kind())
	}
	ec.comp = c.Node.Type().Compare
	return nil
}

func (ec *equalConstraint) path() string {
	return ec.pth
}

func (ec *equalConstraint) matches(v parquet.Value) bool {
	return bytes.Equal(v.ByteArray(), ec.val.ByteArray())
}

func (ec *equalConstraint) skipByBloomfilter(cc parquet.ColumnChunk) (bool, error) {
	if ec.f.Cfg.SkipBloomFilters {
		return false, nil
	}

	bf := cc.BloomFilter()
	if bf == nil {
		return false, nil
	}
	ok, err := bf.Check(ec.val)
	if err != nil {
		return false, fmt.Errorf("unable to check bloomfilter: %w", err)
	}
	return !ok, nil
}

func Regex(path string, r *labels.FastRegexMatcher) Constraint {
	return &regexConstraint{pth: path, cache: make(map[parquet.Value]bool), r: r}
}

type regexConstraint struct {
	pth   string
	cache map[parquet.Value]bool
	f     *storage.ParquetFile
	r     *labels.FastRegexMatcher
}

func (rc *regexConstraint) String() string {
	return fmt.Sprintf("regex(%v,%v)", rc.pth, rc.r.GetRegexString())
}

func (rc *regexConstraint) filter(ctx context.Context, rgIdx int, primary bool, rr []RowRange) ([]RowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	from, to := rr[0].From, rr[len(rr)-1].From+rr[len(rr)-1].Count

	rg := rc.f.RowGroups()[rgIdx]

	col, ok := rg.Schema().Lookup(rc.path())
	if !ok {
		// If match empty, return rr (filter nothing)
		// otherwise return empty
		if rc.matches(parquet.ValueOf("")) {
			return rr, nil
		}
		return []RowRange{}, nil
	}
	cc := rg.ColumnChunks()[col.ColumnIndex]

	pgs, err := rc.f.GetPages(ctx, cc, 0, 0)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pages")
	}

	defer func() { _ = pgs.Close() }()

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}
	var (
		symbols = new(symbolTable)
		res     = make([]RowRange, 0)
	)
	for i := 0; i < cidx.NumPages(); i++ {
		// If page does not intersect from, to; we can immediately discard it
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < oidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		// Page intersects [from, to] but we might be able to discard it with statistics
		if cidx.NullPage(i) {
			if rc.matches(parquet.ValueOf("")) {
				res = append(res, RowRange{pfrom, pcount})
			}
			continue
		}
		// TODO: use setmatches / prefix for statistics

		// We cannot discard the page through statistics but we might need to read it to see if it has the value
		if err := pgs.SeekToRow(pfrom); err != nil {
			return nil, fmt.Errorf("unable to seek to row: %w", err)
		}
		pg, err := pgs.ReadPage()
		if err != nil {
			return nil, fmt.Errorf("unable to read page: %w", err)
		}

		symbols.Reset(pg)

		// The page has the value, we need to find the matching row ranges
		n := int(pg.NumRows())
		bl := int(max(pfrom, from) - pfrom)
		br := n - int(pto-min(pto, to))
		off, count := bl, 0
		for j := bl; j < br; j++ {
			if !rc.matches(symbols.Get(j)) {
				if count != 0 {
					res = append(res, RowRange{pfrom + int64(off), int64(count)})
				}
				off, count = j, 0
			} else {
				if count == 0 {
					off = j
				}
				count++
			}
		}
		if count != 0 {
			res = append(res, RowRange{pfrom + int64(off), int64(count)})
		}
	}
	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}

func (rc *regexConstraint) init(f *storage.ParquetFile) error {
	c, ok := f.Schema().Lookup(rc.path())
	rc.f = f
	if !ok {
		return nil
	}
	if stringKind := parquet.String().Type().Kind(); c.Node.Type().Kind() != stringKind {
		return fmt.Errorf("schema: cannot search value of kind %s in column of kind %s", stringKind, c.Node.Type().Kind())
	}
	rc.cache = make(map[parquet.Value]bool)
	return nil
}

func (rc *regexConstraint) path() string {
	return rc.pth
}

func (rc *regexConstraint) matches(v parquet.Value) bool {
	accept, seen := rc.cache[v]
	if !seen {
		accept = rc.r.MatchString(util.YoloString(v.ByteArray()))
		rc.cache[v] = accept
	}
	return accept
}

func Not(c Constraint) Constraint {
	return &notConstraint{c: c}
}

type notConstraint struct {
	c Constraint
}

func (nc *notConstraint) String() string {
	return fmt.Sprintf("not(%v)", nc.c.String())
}

func (nc *notConstraint) filter(ctx context.Context, rgIdx int, primary bool, rr []RowRange) ([]RowRange, error) {
	base, err := nc.c.filter(ctx, rgIdx, primary, rr)
	if err != nil {
		return nil, fmt.Errorf("unable to compute child constraint: %w", err)
	}
	// no need to intersect since its already subset of rr
	return complementRowRanges(base, rr), nil
}

func (nc *notConstraint) init(f *storage.ParquetFile) error {
	return nc.c.init(f)
}

func (nc *notConstraint) path() string {
	return nc.c.path()
}
