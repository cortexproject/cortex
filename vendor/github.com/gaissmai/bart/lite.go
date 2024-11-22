// Copyright (c) 2024 Karl Gaissmaier
// SPDX-License-Identifier: MIT

package bart

import "net/netip"

// Lite is just a convenience wrapper for Table, instantiated with an
// empty struct as payload. Lite is ideal for simple IP ACLs
// (access-control-lists) with plain true/false results without a payload.
//
// Lite delegates almost all methods unmodified to the underlying Table.
//
// Some of the Table methods make no sense without a payload.
// Their signature has been changed and they do not accept any arguments
// and if they are used anyway, they will generate a panic.
type Lite struct {
	Table[struct{}]
}

// Exists returns true if the prefix exists in the table.
// It's an adapter to [Table.Get].
func (l *Lite) Exists(pfx netip.Prefix) bool {
	_, ok := l.Get(pfx)
	return ok
}

// Insert a pfx into the tree.
func (l *Lite) Insert(pfx netip.Prefix) {
	l.Table.Insert(pfx, struct{}{})
}

// InsertPersist is similar to Insert but the receiver isn't modified.
func (l *Lite) InsertPersist(pfx netip.Prefix) *Lite {
	tbl := l.Table.InsertPersist(pfx, struct{}{})
	//nolint:govet // copy of *tbl is here by intention
	return &Lite{*tbl}
}

// DeletePersist is similar to Delete but the receiver isn't modified.
func (l *Lite) DeletePersist(pfx netip.Prefix) *Lite {
	tbl := l.Table.DeletePersist(pfx)
	//nolint:govet // copy of *tbl is here by intention
	return &Lite{*tbl}
}

// Clone returns a copy of the routing table.
func (l *Lite) Clone() *Lite {
	tbl := l.Table.Clone()
	//nolint:govet // copy of *tbl is here by intention
	return &Lite{*tbl}
}

// Union combines two tables, changing the receiver table.
func (l *Lite) Union(o *Lite) {
	l.Table.Union(&o.Table)
}

// Overlaps4 reports whether any IPv4 in the table matches a route in the
// other table or vice versa.
func (l *Lite) Overlaps4(o *Lite) bool {
	return l.Table.Overlaps4(&o.Table)
}

// Overlaps6 reports whether any IPv6 in the table matches a route in the
// other table or vice versa.
func (l *Lite) Overlaps6(o *Lite) bool {
	return l.Table.Overlaps6(&o.Table)
}

// Overlaps reports whether any IP in the table matches a route in the
// other table or vice versa.
func (l *Lite) Overlaps(o *Lite) bool {
	return l.Table.Overlaps(&o.Table)
}

// Deprecated: Update is pointless without payload and panics.
func (l *Lite) Update() {
	panic("Update is pointless without payload")
}

// Deprecated: UpdatePersist is pointless without payload and panics.
func (l *Lite) UpdatePersist() {
	panic("UpdatePersist is pointless without payload")
}

// Deprecated: GetAndDelete is pointless without payload and panics.
func (l *Lite) GetAndDelete() {
	panic("GetAndDelete is pointless without payload")
}

// Deprecated: GetAndDeletePersist is pointless without payload and panics.
func (l *Lite) GetAndDeletePersist() {
	panic("GetAndDeletePersist is pointless without payload")
}
