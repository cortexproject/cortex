// Copyright (c) 2012-2013 Jason McVetta.  This is Free Software, released under
// the terms of the GPL v3.  See http://www.gnu.org/copyleft/gpl.html for details.
// Resist intellectual serfdom - the ownership of ideas is akin to slavery.

package neoism

import (
	"strings"
)

// An entity is an object - either a Node or a Relationship - in a Neo4j graph
// database.  An entity may optinally be assigned an arbitrary set of key:value
// properties.
type entity struct {
	Db             *Database
	HrefSelf       string `json:"self"`
	HrefProperty   string `json:"property"`
	HrefProperties string `json:"properties"`
}

// SetProperty sets the single property key to value.
func (e *entity) SetProperty(key string, value string) error {
	parts := []string{e.HrefProperties, key}
	url := strings.Join(parts, "/")
	ne := NeoError{}
	resp, err := e.Db.Session.Put(url, &value, nil, &ne)
	if err != nil {
		return err
	}
	if resp.Status() != 204 {
		return ne
	}
	return nil // Success!
}

// GetProperty fetches the value of property key.
func (e *entity) Property(key string) (string, error) {
	var val string
	parts := []string{e.HrefProperties, key}
	url := strings.Join(parts, "/")
	ne := NeoError{}
	resp, err := e.Db.Session.Get(url, nil, &val, &ne)
	if err != nil {
		return val, err
	}
	switch resp.Status() {
	case 200:
	case 404:
		return val, NotFound
	default:
		return val, ne
	}
	return val, nil // Success!
}

// DeleteProperty deletes property key
func (e *entity) DeleteProperty(key string) error {
	parts := []string{e.HrefProperties, key}
	url := strings.Join(parts, "/")
	ne := NeoError{}
	resp, err := e.Db.Session.Delete(url, nil, nil, &ne)
	if err != nil {
		return err
	}
	switch resp.Status() {
	case 204:
		return nil // Success!
	case 404:
		return NotFound
	}
	return ne
}

// Delete removes the object from the DB.
func (e *entity) Delete() error {
	ne := NeoError{}
	resp, err := e.Db.Session.Delete(e.HrefSelf, nil, nil, &ne)
	if err != nil {
		return err
	}
	switch resp.Status() {
	case 204:
	case 404:
		return NotFound
	case 409:
		return CannotDelete
	default:
		return ne
	}
	return nil
}

// Properties fetches all properties
func (e *entity) Properties() (Props, error) {
	props := Props{}
	resp, err := e.Db.Session.Get(e.HrefProperties, nil, &props, nil)
	if err != nil {
		return props, err
	}
	// Status code 204 indicates no properties on this node
	if resp.Status() == 204 {
		props = Props{}
	}
	return props, nil
}

// SetProperties updates all properties, overwriting any existing properties.
func (e *entity) SetProperties(p Props) error {
	ne := NeoError{}
	resp, err := e.Db.Session.Put(e.HrefProperties, &p, nil, &ne)
	if err != nil {
		return err
	}
	if resp.Status() == 204 {
		return nil // Success!
	}
	return ne
}

// DeleteProperties deletes all properties.
func (e *entity) DeleteProperties() error {
	ne := NeoError{}
	resp, err := e.Db.Session.Delete(e.HrefProperties, nil, nil, &ne)
	if err != nil {
		return err
	}
	switch resp.Status() {
	case 204:
		return nil // Success!
	case 404:
		return NotFound
	}
	return ne
}
