// Copyright (c) 2012-2013 Jason McVetta.  This is Free Software, released under
// the terms of the GPL v3.  See http://www.gnu.org/copyleft/gpl.html for details.
// Resist intellectual serfdom - the ownership of ideas is akin to slavery.

package neoism

import (
	"errors"
)

type indexRequest struct {
	PropertyKeys []string `json:"property_keys"`
}

// An Index improves the speed of looking up nodes in the database.
type Index struct {
	db           *Database
	Label        string
	PropertyKeys []string `json:"property_keys"`
}

// Drop removes the index.
func (idx *Index) Drop() error {
	uri := join(idx.db.Url, "schema/index", idx.Label, idx.PropertyKeys[0])
	ne := NeoError{}
	resp, err := idx.db.Session.Delete(uri, nil, nil, &ne)
	if err != nil {
		return err
	}
	if resp.Status() == 404 {
		return NotFound
	}
	if resp.Status() != 204 {
		return ne
	}
	return nil
}

// CreateIndex starts a background job in the database that will create and
// populate the new index of a specified property on nodes of a given label.
func (db *Database) CreateIndex(label, property string) (*Index, error) {
	uri := join(db.Url, "schema/index", label)
	payload := indexRequest{[]string{property}}
	result := Index{db: db}
	ne := NeoError{}
	resp, err := db.Session.Post(uri, payload, &result, &ne)
	if err != nil {
		return nil, err
	}
	switch resp.Status() {
	case 200:
		return &result, nil // Success
	case 405:
		return nil, NotAllowed
	}
	return nil, ne
}

// Indexes lists indexes for a label.  If a blank string is given as the label,
// returns all indexes.
func (db *Database) Indexes(label string) ([]*Index, error) {
	uri := join(db.Url, "schema/index", label)
	result := []*Index{}
	ne := NeoError{}
	resp, err := db.Session.Get(uri, nil, &result, &ne)
	if err != nil {
		return result, err
	}
	if resp.Status() == 404 {
		return result, NotFound
	}
	if resp.Status() != 200 {
		return result, ne
	}
	for _, idx := range result {
		idx.db = db
	}
	return result, nil
}

type uniqueConstraintRequest struct {
	PropertyKeys []string `json:"property_keys"`
}

// A UniqueConstraint makes sure that your database will never contain more
// than one node with a specific label and one property value.
type UniqueConstraint struct {
	db           *Database
	Label        string   `json:"label"`
	Type         string   `json:"type"`
	PropertyKeys []string `json:"property_keys"`
}

// CreateUniqueConstraint create a unique constraint on a property on nodes
// with a specific label.
func (db *Database) CreateUniqueConstraint(label, property string) (*UniqueConstraint, error) {
	uri := join(db.Url, "schema/constraint", label, "uniqueness")
	payload := uniqueConstraintRequest{[]string{property}}
	result := UniqueConstraint{db: db}
	ne := NeoError{}
	resp, err := db.Session.Post(uri, payload, &result, &ne)
	if err != nil {
		return nil, err
	}
	switch resp.Status() {
	case 200:
		return &result, nil // Success
	case 405:
		return nil, NotAllowed
	case 409:
		return nil, NotAllowed // Constraint is viloated by existing data
	}
	return nil, ne
}

// UniqueConstraints get a specific unique constraint for a label and a property.
// If a blank string is given as the property, return all unique constraint for
// the label.
func (db *Database) UniqueConstraints(label, property string) ([]*UniqueConstraint, error) {
	if label == "" {
		return nil, errors.New("Label cannot be empty")
	}

	uri := join(db.Url, "schema/constraint", label, "uniqueness", property)
	result := []*UniqueConstraint{}
	ne := NeoError{}
	resp, err := db.Session.Get(uri, nil, &result, &ne)
	if err != nil {
		return result, err
	}
	switch resp.Status() {
	case 200:
		for _, cstr := range result {
			cstr.db = db
		}
		return result, nil // Success
	case 404:
		return nil, NotFound
	}
	return nil, ne
}

// Drop removes the unique constraint.
func (cstr *UniqueConstraint) Drop() error {
	uri := join(cstr.db.Url, "schema/constraint", cstr.Label, "uniqueness", cstr.PropertyKeys[0])
	ne := NeoError{}
	resp, err := cstr.db.Session.Delete(uri, nil, nil, &ne)
	if err != nil {
		return err
	}
	switch resp.Status() {
	case 204:
		return nil
	case 404:
		return NotFound
	}
	return ne
}
