// Copyright (c) 2012-2013 Jason McVetta.  This is Free Software, released under
// the terms of the GPL v3.  See http://www.gnu.org/copyleft/gpl.html for details.
// Resist intellectual serfdom - the ownership of ideas is akin to slavery.

package neoism

import "strconv"

// A LegacyRelationshipIndex is an index for searching Relationships.
type LegacyRelationshipIndex struct {
	index
}

// CreateLegacyRelIndex creates a new relationship index with optional type and
// provider.
func (db *Database) CreateLegacyRelIndex(name, idxType, provider string) (*LegacyRelationshipIndex, error) {
	idx, err := db.createIndex(db.HrefRelIndex, name, idxType, provider)
	if err != nil {
		return nil, err
	}
	return &LegacyRelationshipIndex{*idx}, nil
}

// LegacyRelIndexes returns all relationship indexes.
func (db *Database) LegacyRelIndexes() ([]*LegacyRelationshipIndex, error) {
	indexes, err := db.indexes(db.HrefRelIndex)
	if err != nil {
		return nil, err
	}
	ris := make([]*LegacyRelationshipIndex, len(indexes))
	for i, idx := range indexes {
		ris[i] = &LegacyRelationshipIndex{*idx}
	}
	return ris, nil
}

// LegacyRelIndex returns the named relationship index.
func (db *Database) LegacyRelIndex(name string) (*LegacyRelationshipIndex, error) {
	idx, err := db.index(db.HrefRelIndex, name)
	if err != nil {
		return nil, err
	}
	ri := LegacyRelationshipIndex{*idx}
	return &ri, nil
}

// Remove deletes all entries with a given node, key and value from the index.
// If value or both key and value are the blank string, they are ignored.
func (rix *LegacyRelationshipIndex) Remove(r *Relationship, key, value string) error {
	id := strconv.Itoa(r.Id())
	return rix.remove(r.entity, id, key, value)
}
