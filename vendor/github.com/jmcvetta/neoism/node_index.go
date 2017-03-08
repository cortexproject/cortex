// Copyright (c) 2012-2013 Jason McVetta.  This is Free Software, released under
// the terms of the GPL v3.  See http://www.gnu.org/copyleft/gpl.html for details.
// Resist intellectual serfdom - the ownership of ideas is akin to slavery.

package neoism

import (
	"net/url"
	"strconv"
)

// A LegacyNodeIndex is a searchable index for nodes.
type LegacyNodeIndex struct {
	index
}

// CreateLegacyNodeIndex creates a new node index with optional type and provider.
func (db *Database) CreateLegacyNodeIndex(name, idxType, provider string) (*LegacyNodeIndex, error) {
	idx, err := db.createIndex(db.HrefNodeIndex, name, idxType, provider)
	if err != nil {
		return nil, err
	}
	return &LegacyNodeIndex{*idx}, nil
}

// LegacyNodeIndexes returns all node indexes.
func (db *Database) LegacyNodeIndexes() ([]*LegacyNodeIndex, error) {
	indexes, err := db.indexes(db.HrefNodeIndex)
	if err != nil {
		return nil, err
	}
	nis := make([]*LegacyNodeIndex, len(indexes))
	for i, idx := range indexes {
		nis[i] = &LegacyNodeIndex{*idx}
	}
	return nis, nil
}

// LegacyNodeIndex returns the named relationship index.
func (db *Database) LegacyNodeIndex(name string) (*LegacyNodeIndex, error) {
	idx, err := db.index(db.HrefNodeIndex, name)
	if err != nil {
		return nil, err
	}
	ni := LegacyNodeIndex{*idx}
	return &ni, nil

}

// Add indexes a node with a key/value pair.
func (nix *LegacyNodeIndex) Add(n *Node, key string, value interface{}) error {
	return nix.add(n.entity, key, value)
}

// Remove deletes all entries with a given node, key and value from the index.
// If value or both key and value are the blank string, they are ignored.
func (nix *LegacyNodeIndex) Remove(n *Node, key, value string) error {
	id := strconv.Itoa(n.Id())
	return nix.remove(n.entity, id, key, value)
}

// Find locates Nodes in the index by exact key/value match.
func (idx *LegacyNodeIndex) Find(key, value string) (map[int]*Node, error) {
	nm := make(map[int]*Node)
	rawurl, err := idx.uri()
	if err != nil {
		return nm, err
	}
	rawurl = join(rawurl, key, value)
	u, err := url.ParseRequestURI(rawurl)
	if err != nil {
		return nm, err
	}
	result := []Node{}
	ne := NeoError{}
	resp, err := idx.db.Session.Get(u.String(), nil, &result, &ne)
	if err != nil {
		return nm, err
	}
	if resp.Status() != 200 {
		return nm, ne
	}
	for _, n := range result {
		n.Db = idx.db
		nm[n.Id()] = &n
	}
	return nm, nil
}

// Query finds nodes with a query.
func (idx *index) Query(query string) (map[int]*Node, error) {
	nm := make(map[int]*Node)
	rawurl, err := idx.uri()
	if err != nil {
		return nm, err
	}
	v := make(url.Values)
	v.Add("query", query)
	rawurl += "?" + v.Encode()
	u, err := url.ParseRequestURI(rawurl)
	if err != nil {
		return nm, err
	}
	result := []Node{}
	ne := NeoError{}
	resp, err := idx.db.Session.Get(u.String(), nil, &result, &ne)
	if err != nil {
		return nm, err
	}
	if resp.Status() != 200 {
		return nm, ne
	}
	for _, n := range result {
		n.Db = idx.db
		nm[n.Id()] = &n
	}
	return nm, nil
}
